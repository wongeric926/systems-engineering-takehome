"""The ledger: the system's source of truth for "how much has this customer been charged".

This is where effectively-once actually lives. Delivery is at-least-once, so the same
order_id can reach us more than once (duplicate publish, redelivery after a crash). The
job here is to make the ledger increment happen *exactly once per order_id*, atomically,
so no race between duplicate deliveries or concurrent workers can double-count.
"""
from models import Order

# Check-and-increment in a single server-side step. Because Redis runs the script
# atomically, the EXISTS gate and the INCRBY can't interleave with another delivery of
# the same order_id -- the classic read-then-write race that would double-charge.
#
#   KEYS[1] = processed:{order_id}   (idempotency marker)
#   KEYS[2] = ledger:{customer_id}
#   KEYS[3] = processed_count
#   ARGV[1] = amount_cents
# returns 1 if this call applied the charge, 0 if it was already applied.
_APPLY_CHARGE_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('INCRBY', KEYS[2], ARGV[1])
redis.call('INCR', KEYS[3])
return 1
"""


class LedgerRepository:
    def __init__(self, redis_client):
        self._r = redis_client
        self._apply_charge = self._r.register_script(_APPLY_CHARGE_LUA)

    def already_applied(self, order: Order) -> bool:
        """Cheap pre-check so a duplicate we've already handled skips the payment call."""
        return bool(self._r.exists(self._processed_key(order.order_id)))

    def record_charge(self, order: Order) -> bool:
        """Idempotently apply the charge to the ledger. Returns True if it was applied
        now, False if a prior delivery already applied it."""
        applied = self._apply_charge(
            keys=[
                self._processed_key(order.order_id),
                f"ledger:{order.customer_id}",
                "processed_count",
            ],
            args=[order.amount_cents],
        )
        return applied == 1

    @staticmethod
    def _processed_key(order_id: str) -> str:
        return f"processed:{order_id}"
