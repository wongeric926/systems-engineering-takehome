"""Client for the flaky payments provider.

Owns exactly one concern: turning an unreliable HTTP dependency into a call that
either succeeds or raises after a bounded, backed-off retry budget. It classifies
failures as transient (retry) vs. permanent (give up) so the caller doesn't have to.
"""
import random
import time

import requests

from models import Order


class PaymentPermanentError(Exception):
    """Provider rejected the charge in a way that will never succeed (4xx)."""


class PaymentTransientError(Exception):
    """Provider failed in a way that may succeed on retry (5xx / timeout / network)."""


class PaymentsClient:
    def __init__(
        self,
        base_url: str,
        *,
        connect_timeout: float,
        read_timeout: float,
        max_attempts: int,
        backoff_base: float,
        backoff_cap: float,
        sleep=time.sleep,
        session=None,
    ):
        self._url = base_url.rstrip("/") + "/charge"
        self._timeout = (connect_timeout, read_timeout)
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._sleep = sleep
        # Injected for tests; defaults to a real pooled session in production.
        self._session = session or requests.Session()

    def charge(self, order: Order) -> None:
        """Charge the customer, retrying transient failures with exponential backoff.

        Sends an Idempotency-Key so a *correct* provider would de-duplicate our retries.
        (Our stand-in ignores it; the ledger write is where we enforce idempotency for
        real -- see LedgerRepository.) Raises after exhausting the retry budget so the
        message stays un-acked and gets redelivered later rather than being dropped.
        """
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._attempt_charge(order)
                return
            except PaymentPermanentError:
                raise
            except PaymentTransientError as exc:
                last_exc = exc
                if attempt < self._max_attempts:
                    self._sleep(self._backoff(attempt))
        raise PaymentTransientError(
            f"charge failed after {self._max_attempts} attempts: {last_exc}"
        )

    def _attempt_charge(self, order: Order) -> None:
        try:
            resp = self._session.post(
                self._url,
                json={"order_id": order.order_id, "amount_cents": order.amount_cents},
                headers={"Idempotency-Key": order.order_id},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise PaymentTransientError(f"network error: {exc}") from exc

        if resp.status_code >= 500:
            raise PaymentTransientError(f"provider {resp.status_code}")
        if resp.status_code >= 400:
            raise PaymentPermanentError(f"provider {resp.status_code}: {resp.text[:200]}")

    def _backoff(self, attempt: int) -> float:
        # Exponential backoff with full jitter, capped. Jitter spreads retries so a
        # fleet of workers doesn't synchronously hammer a recovering provider.
        ceiling = min(self._backoff_cap, self._backoff_base * (2 ** (attempt - 1)))
        return random.uniform(0, ceiling)
