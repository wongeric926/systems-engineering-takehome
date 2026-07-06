# AI Notes

## A prompt I used
> "I'm consuming an order stream with a Redis consumer group and charging a flaky payments
> service, then incrementing a Redis ledger per customer. Orders can be delivered more than
> once and the worker can crash mid-flight. Show me the consume → charge → ledger loop with
> idempotency so I never double-charge or lose an order."

## Something the AI got wrong — and how I caught it
The first draft acked the message right after `XREADGROUP` and guarded idempotency with a
separate `GET processed:{id}` / `if not set: charge + INCRBY` block. Two real bugs for
*this* system:

1. **Ack-before-process loses orders.** Acking on receipt removes the entry from the
   pending list, so a crash between the ack and the ledger write drops the charge — exactly
   the "lost order" `check.py` fails on. I moved the `XACK` to *after* a successful ledger
   write, so an in-flight crash leaves the message pending and it gets redelivered.

2. **The idempotency check was a check-then-act race.** `GET` then later `INCRBY` is a
   TOCTOU window: two concurrent deliveries of the same `order_id` (a duplicate racing a
   redelivery, or two workers) can both read "not processed" and both increment → double
   charge, which `check.py` treats as irrecoverable. The AI presented this as "idempotent"
   but it only holds under a single serial consumer. I collapsed the gate and the increment
   into one **atomic Lua script** (`EXISTS → SET + INCRBY + INCR`) that Redis runs
   server-side as a unit, so the increment happens exactly once regardless of concurrency.

I also had to correct the AI's claim that this achieves "exactly-once." It doesn't — the
provider can still be called twice if we crash between charging and recording. The honest
framing is **at-least-once delivery + effectively-once processing**, with an
`Idempotency-Key` on the charge so a real provider de-duplicates the crash-window retry.
