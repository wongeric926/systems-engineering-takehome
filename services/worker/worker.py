"""Order worker: consume orders, charge the customer, record the ledger -- correctly
and resiliently in spite of duplicate deliveries, restarts, and a flaky payments provider.

Composition root only. The real work lives in single-purpose collaborators:
  - StreamConsumer   : at-least-once delivery via a Redis consumer group (read/ack/reclaim/DLQ)
  - PaymentsClient   : bounded, backed-off retries around the flaky provider
  - LedgerRepository : effectively-once ledger writes (atomic idempotent increment)

Delivery is at-least-once; correctness ("effectively-once") comes from idempotent
processing, not from pretending the network is reliable. See docs/ADR.md.
"""
import redis

from config import Config
from consumer import StreamConsumer
from ledger import LedgerRepository
from models import Order, PoisonMessage
from payments import PaymentPermanentError, PaymentsClient


class OrderProcessor:
    """Wires payment + ledger for a single order. This is the effectively-once seam."""

    def __init__(self, payments: PaymentsClient, ledger: LedgerRepository):
        self._payments = payments
        self._ledger = ledger

    def handle(self, order: Order) -> None:
        # Fast path for duplicates we've already settled: skip the charge entirely.
        if self._ledger.already_applied(order):
            return

        try:
            self._payments.charge(order)
        except PaymentPermanentError as exc:
            # The provider will never accept this charge. Surface it as poison so the
            # consumer dead-letters it instead of retrying forever.
            raise PoisonMessage(f"permanent payment failure: {exc}") from exc

        # Charge succeeded. Apply to the ledger idempotently: if a concurrent delivery
        # or a redelivery-after-crash already recorded it, this is a no-op.
        if self._ledger.record_charge(order):
            print(f"charged {order.order_id} -> {order.customer_id} "
                  f"({order.amount_cents}c)", flush=True)


def main() -> None:
    config = Config.from_env()
    redis_client = redis.from_url(config.redis_url, decode_responses=True)

    payments = PaymentsClient(
        config.payments_url,
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        max_attempts=config.max_attempts,
        backoff_base=config.backoff_base,
        backoff_cap=config.backoff_cap,
    )
    ledger = LedgerRepository(redis_client)
    processor = OrderProcessor(payments, ledger)
    consumer = StreamConsumer(redis_client, config)

    print(f"worker started (consumer={config.consumer}, group={config.group})", flush=True)
    consumer.run(processor.handle)


if __name__ == "__main__":
    main()
