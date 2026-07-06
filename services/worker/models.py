"""Domain types shared across the worker.

Parsing lives here so the consumer loop can distinguish a *permanent* decode error
(poison message -> dead-letter) from a *transient* processing failure (retry).
"""
from dataclasses import dataclass


class PoisonMessage(Exception):
    """A message that can never be processed (bad shape). Route to the DLQ, don't retry."""


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_id: str
    amount_cents: int

    @staticmethod
    def parse(fields: dict) -> "Order":
        import json

        try:
            data = json.loads(fields["data"])
            order_id = str(data["order_id"])
            customer_id = str(data["customer_id"])
            amount_cents = int(data["amount_cents"])
        except (KeyError, ValueError, TypeError) as exc:
            raise PoisonMessage(f"cannot parse order: {exc!r}") from exc
        if not order_id or not customer_id or amount_cents < 0:
            raise PoisonMessage(f"invalid order values: {data!r}")
        return Order(order_id=order_id, customer_id=customer_id, amount_cents=amount_cents)
