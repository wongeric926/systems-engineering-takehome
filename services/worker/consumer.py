"""Redis Streams consumer-group plumbing.

Owns the delivery semantics: create the group, read (new + own backlog), acknowledge
only after successful processing, reclaim messages stranded by a dead worker, and
dead-letter poison messages. It knows nothing about payments or ledgers -- it hands
each decoded Order to a callback and acks based on the outcome.
"""
import redis

from config import Config
from models import Order, PoisonMessage


class StreamConsumer:
    def __init__(self, redis_client: redis.Redis, config: Config):
        self._r = redis_client
        self._cfg = config
        self._ensure_group()

    def _ensure_group(self) -> None:
        # MKSTREAM + id "0" so the group covers the whole stream, including anything
        # published before the worker first came up. BUSYGROUP just means it exists.
        try:
            self._r.xgroup_create(
                self._cfg.orders_stream, self._cfg.group, id="0", mkstream=True
            )
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def run(self, handle) -> None:
        """Consume forever. `handle(Order) -> None` processes one order; if it raises,
        the message is left un-acked (stays pending) for a later retry/reclaim."""
        # On startup, first replay this consumer's own pending entries (id "0"): anything
        # it read but never acked before a restart. Then switch to new messages (">").
        self._drain(handle, cursor="0")
        while True:
            self._reclaim_stranded(handle)
            self._drain(handle, cursor=">", block_ms=self._cfg.block_ms)

    def _drain(self, handle, cursor: str, block_ms: int | None = None) -> None:
        resp = self._r.xreadgroup(
            self._cfg.group,
            self._cfg.consumer,
            {self._cfg.orders_stream: cursor},
            count=self._cfg.batch_size,
            block=block_ms,
        )
        if not resp:
            return
        for _stream, messages in resp:
            for msg_id, fields in messages:
                self._dispatch(handle, msg_id, fields)

    def _reclaim_stranded(self, handle) -> None:
        """Steal messages that another consumer read but never acked (e.g. it was killed
        mid-charge and its container name changed). XAUTOCLAIM only hands over entries
        idle longer than the threshold, so it won't fight a worker that's merely slow."""
        cursor = "0-0"
        while True:
            # XAUTOCLAIM returns [next_cursor, claimed, deleted] on Redis 7 (the deleted
            # list is absent on older servers/clients) -- index rather than unpack a fixed
            # arity so a version difference can't raise.
            result = self._r.xautoclaim(
                self._cfg.orders_stream,
                self._cfg.group,
                self._cfg.consumer,
                min_idle_time=self._cfg.reclaim_idle_ms,
                start_id=cursor,
                count=self._cfg.reclaim_batch,
            )
            cursor, claimed = result[0], result[1]
            for msg_id, fields in claimed:
                self._dispatch(handle, msg_id, fields)
            if cursor == "0-0":
                return

    def _dispatch(self, handle, msg_id: str, fields: dict) -> None:
        # A tombstone from a trimmed/deleted entry comes back with no fields; ack it.
        if not fields:
            self._ack(msg_id)
            return
        try:
            order = Order.parse(fields)
        except PoisonMessage as exc:
            self._dead_letter(msg_id, fields, reason=str(exc))
            return
        try:
            handle(order)
        except PoisonMessage as exc:  # permanent: will never succeed -> DLQ
            self._dead_letter(msg_id, fields, reason=str(exc))
            return
        except Exception as exc:  # transient: leave pending for redelivery
            print(f"WARN handling {msg_id} ({order.order_id}) failed, will retry: {exc}",
                  flush=True)
            return
        self._ack(msg_id)

    def _dead_letter(self, msg_id: str, fields: dict, reason: str) -> None:
        # Poison messages can never succeed; park them on a DLQ stream (with the reason)
        # and ack the original so one bad message can't wedge the pipeline.
        payload = dict(fields)
        payload["_dlq_reason"] = reason
        payload["_dlq_source_id"] = msg_id
        self._r.xadd(self._cfg.dead_letter_stream, payload)
        self._ack(msg_id)
        print(f"DLQ {msg_id}: {reason}", flush=True)

    def _ack(self, msg_id: str) -> None:
        self._r.xack(self._cfg.orders_stream, self._cfg.group, msg_id)
