# ADR-001: Effectively-once order → payment processing on Redis Streams

## Status
Accepted

## Context
An order→payment pipeline: a FastAPI **producer** publishes orders to a Redis Stream; a
**worker** consumes them and charges a **payments** provider; a Redis **ledger** records
the total charged per customer and is the observable definition of correctness (every
customer charged *exactly* what they ordered).

The system must survive three real conditions, none of which the prototype handled:

1. **Duplicate deliveries.** At-least-once upstreams (client retries, LB redrives) send
   the same `order_id` more than once. `scripts/load.py` does this on purpose.
2. **Worker restarts mid-flight.** The container can be killed between charging and
   recording — or before it ever records.
3. **A flaky downstream.** `payments` returns 500s ~30% of the time and hangs ~10% of the
   time. Treated as a third-party we don't control.

**What was wrong with the prototype** (`worker.py` as shipped):
- Read with `XREAD` from `$` and **no consumer group** — no acknowledgements, no
  pending-entries list, so a crash lost the in-flight message and any order published
  before the worker subscribed was never seen.
- Advanced its cursor *before* processing, so any failure **silently skipped** the order.
- **No idempotency** — a duplicate `order_id` was charged twice → overcharge, which
  `check.py` treats as irrecoverable.
- **No timeout** on the HTTP call — a hang blocked the single consumer indefinitely.
- **Unhandled 500** — `raise_for_status()` propagated out of the loop and killed the
  process on the first transient error, with no restart policy.

## Decision

### Delivery & consistency semantics: **at-least-once delivery + effectively-once processing**
You cannot get true exactly-once across a network boundary (the two-generals problem: a
charge can succeed while its acknowledgement is lost). So we choose the only honest,
achievable target: **at-least-once delivery, made effectively-once by idempotent
processing.** Deliver as many times as necessary to never lose an order; make reprocessing
the same order a no-op so we never double-count.

This implies the consumer must guarantee: (a) it **acks only after** the ledger write
succeeds, so a crash re-delivers rather than drops; and (b) the ledger write is
**idempotent per `order_id`**, so redelivery/duplicates don't accumulate.

Mechanism — Redis **consumer groups** (`XREADGROUP`/`XACK`):
- Group created at id `0` with `MKSTREAM`, so it covers the entire stream including
  anything published before the worker came up.
- On startup the worker replays its **own pending entries** (`XREADGROUP … 0`) before
  reading new messages (`>`) — recovering work it read but never acked before a restart.
- `XAUTOCLAIM` reclaims entries a *dead* consumer left pending beyond an idle threshold
  (covers a permanently gone worker or a changed container identity, and enables scaling
  to multiple workers).

### Idempotency
The key is `order_id`; state lives in Redis as `processed:{order_id}`. The check-and-apply
is a **single Lua script** (`ledger.py`) that runs atomically on the Redis server:

```
if EXISTS processed:{order_id}: return 0        # already applied
SET processed:{order_id}; INCRBY ledger:{cust}; INCR processed_count; return 1
```

**The race this avoids:** a plain "read marker → if absent, increment" is a classic
check-then-act TOCTOU. Two concurrent deliveries of the same `order_id` (redelivery +
duplicate, or two workers) could both read "absent" and both increment → double charge.
Folding the gate and the increment into one atomic script makes the ledger increment
happen *exactly once* regardless of concurrency. A cheap `EXISTS` pre-check short-circuits
the common duplicate case before we even call payments.

**Ordering — charge before ledger:** we charge, *then* record. If we crash in between, the
order stays un-acked and is redelivered; the `EXISTS` pre-check has not been set, so we
charge again but the ledger still increments once. The alternative (mark-then-charge)
would drop the charge if we crashed after marking. We accept a rare *duplicate call to the
provider* (bounded to the crash window) in exchange for never losing or double-counting in
the ledger — and we send an `Idempotency-Key: order_id` header so a real provider would
de-duplicate that window too. Our stand-in ignores it; the ledger is where we *enforce*
it.

### Failure handling
- **Timeouts:** connect 3.05s, read 10s — above the provider's 5s slow path, so a
  slow-but-successful call is waited out, but a true hang is bounded and retried.
- **Retries + backoff:** transient failures (5xx, timeout, connection error) retry up to 8
  times with **exponential backoff + full jitter** (cap 3s). With a 30% failure rate,
  0.3⁸ ≈ 6·10⁻⁵ per order — negligible across 50 orders — while jitter avoids a
  thundering herd against a recovering provider.
- **Transient vs permanent:** 5xx/timeout/network → transient (retry). 4xx → permanent
  (`PaymentPermanentError`) — the provider will never accept it, so retrying is pointless.
- **Poison messages / DLQ:** un-decodable messages, and permanent payment rejections, are
  written to an `orders:dead` stream (with a reason) and acked, so **one bad message can't
  wedge the pipeline**. A charge that merely *exhausts its retry budget* is treated as
  still-transient: it is **left un-acked** so it stays pending and is retried/reclaimed
  later — dropping it would be a lost order and a `check.py` failure.
- **One bad message can't halt everything:** each message is dispatched independently;
  failures are isolated to that message, not the batch.
- `restart: unless-stopped` on the worker is defence in depth — correctness does not
  depend on it (the PEL does), but there's no reason to stay down.

## Tradeoffs & alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker
**Keep Redis Streams here.** At this scale it gives us exactly what we need — consumer
groups, per-consumer PEL, `XAUTOCLAIM`, atomic Lua — with near-zero operational weight,
and Redis is already in the stack. I'd switch when one of these crosses a line:
- **Durability/retention:** Redis is memory-first; AOF `everysec` can lose ~1s on a hard
  crash, and the stream competes with other keys for RAM. If orders are money and must
  survive broker loss, or must be **replayable for days**, move to **Kafka** (disk log,
  long retention) or **SQS** (managed durability).
- **Throughput/ordering at scale:** past ~10⁴–10⁵ msg/s or when we need many partitions
  with per-key ordering and consumer-group rebalancing, **Kafka** is the right tool.
- **Team/ops:** if we don't want to run a broker at all, **SQS** (+ its native DLQ and
  redrive) removes the operational burden — at the cost of no ordering and per-request
  cost.
Rule of thumb: Redis Streams until durability, retention, or partitioned scale forces the
move; then Kafka for high-throughput ordered logs, SQS for hands-off managed queuing.

### From CI to CD
CI here proves *correctness*. For continuous delivery I'd add:
- **Image promotion:** build once, tag by immutable digest, push to a registry (GHCR).
  The *same* digest promotes dev → stage → prod — never rebuild per environment.
- **Environments:** dev (auto-deploy on merge), stage (integration/soak), prod (gated).
  Config via env/secrets, not baked into images.
- **Rollout:** **canary** for the worker — shift a slice of traffic/partitions, watch
  error-rate and ledger-drift SLOs, auto-roll-back on breach. Blue-green for the stateless
  producer where instant cutover/rollback is cheap.
- **GitOps:** yes — **Argo CD** reconciling a declarative repo. Git becomes the source of
  truth; rollback is `git revert`; drift is detected and corrected. Worth it once there's
  more than one environment/service to keep in sync.

### Scaling to 100×
First bottleneck is the **single worker's payment latency**, not Redis. At 100× I'd:
- Run **N worker replicas** in the same consumer group — the design already supports it
  (per-consumer PEL + `XAUTOCLAIM` for the dead), so this is horizontal out of the box.
- The next limit is the **ledger hot key**: `INCRBY ledger:{customer}` serialises per
  customer. Fan-out via sharded counters (`ledger:{cust}:{shard}`, summed on read) or move
  the ledger to a store built for high-write counters.
- Then **Redis single-threaded throughput / memory** — partition the stream (or move to
  Kafka) and cap retention with `MAXLEN`. The idempotency set (`processed:*`) grows
  unbounded; add a TTL sized to the max redelivery/duplicate window.

## Consequences
**Better now:** no lost orders (ack-after-write + PEL + reclaim), no double charges
(atomic idempotent ledger), survives the flaky provider (bounded retries with jitter and
timeouts), survives restarts, and one poison message can't halt the line. Code is split by
concern (config / models / payments / ledger / consumer / composition) so each piece is
unit-testable in isolation.

**Still weak / next with more time:** no TTL on `processed:*` (unbounded growth); no
metrics/alerting on DLQ depth, retry rate, or ledger drift; retry budget is fixed rather
than adaptive (circuit breaker) to a provider that's fully down; DLQ has no automated
redrive. None blocks correctness at this scale; all matter before production traffic.
