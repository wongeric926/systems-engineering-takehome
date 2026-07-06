"""Runtime configuration for the worker.

Single source of truth for environment-driven knobs. Kept separate so the rest of
the code depends on a typed config object rather than reaching into os.environ.
"""
import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    redis_url: str
    payments_url: str

    # Stream / consumer-group identity.
    orders_stream: str
    dead_letter_stream: str
    group: str
    consumer: str

    # How many messages to pull per read, and how long to block waiting for new ones.
    batch_size: int
    block_ms: int

    # Payment call resilience.
    connect_timeout: float
    read_timeout: float
    max_attempts: int
    backoff_base: float
    backoff_cap: float

    # Reclaim: steal messages that another (dead) consumer left un-acked.
    reclaim_idle_ms: int
    reclaim_batch: int

    @staticmethod
    def from_env() -> "Config":
        return Config(
            redis_url=os.environ["REDIS_URL"],
            payments_url=os.environ["PAYMENTS_URL"],
            orders_stream=os.environ.get("ORDERS_STREAM", "orders"),
            dead_letter_stream=os.environ.get("DLQ_STREAM", "orders:dead"),
            group=os.environ.get("CONSUMER_GROUP", "order-workers"),
            # Stable per-container identity so a restarted worker can recover its own
            # pending entries; overridable to run several workers side by side.
            consumer=os.environ.get("CONSUMER_NAME", socket.gethostname()),
            batch_size=int(os.environ.get("BATCH_SIZE", "10")),
            block_ms=int(os.environ.get("BLOCK_MS", "5000")),
            # Read timeout sits comfortably above the payments SLOW_SECONDS (5s) so a
            # slow-but-successful call is waited out rather than needlessly retried.
            connect_timeout=float(os.environ.get("CONNECT_TIMEOUT", "3.05")),
            read_timeout=float(os.environ.get("READ_TIMEOUT", "10")),
            max_attempts=int(os.environ.get("MAX_ATTEMPTS", "8")),
            backoff_base=float(os.environ.get("BACKOFF_BASE", "0.2")),
            backoff_cap=float(os.environ.get("BACKOFF_CAP", "3.0")),
            reclaim_idle_ms=int(os.environ.get("RECLAIM_IDLE_MS", "30000")),
            reclaim_batch=int(os.environ.get("RECLAIM_BATCH", "10")),
        )
