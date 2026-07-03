# Trade-offs and Alternatives Considered

Every rejected alternative below was rejected for stated, quantified reasons — not ignorance.
Each entry: what it is, what it buys, why the primary design wins here.

## 1. SQS-buffered entry (API → SQS → writer Lambda)

- **What it buys:** a shock absorber — the queue absorbs arbitrary bursts; writers drain at a
  smooth, cheap rate; warm throughput could be far smaller.
- **Cost:** position-visibility latency. Fans' arrival evidence isn't durable in the *table*
  until the writer drains their message; "securing your place" lasts minutes instead of
  seconds, and the sequencer's watermark must stretch to cover queue lag (fairness definition
  gets murkier: durable-in-SQS vs durable-in-table).
- **Verdict:** unnecessary given a uniform partition key + warm throughput — DynamoDB *is*
  the shock absorber. Keep SQS as the fallback lever if warm-throughput quotas can't be
  raised in time. (FIFO SQS as an *ordering* device was never a candidate: 300–3,000 msg/s
  per queue caps ~0.3% of the required rate.)

## 2. K-way merge for strict global tiebreak order

- **What it buys:** exact within-bucket sort by tiebreak across all 2,048 shards.
- **Cost:** a 2,048-way streaming merge (one open cursor per GSI partition), lockstep paging,
  complex checkpointing on crash — for a result **statistically indistinguishable** from
  shard-major order, since `(shard, tiebreak)` is i.i.d. uniform random exactly like a wider
  tiebreak ([doc 02 §5](02-fair-ordering.md#5-within-bucket-order-shard-major-is-statistically-fair)).
- **Verdict:** rejected. Complexity that buys zero measurable fairness.

## 3. Edge caching vs DAX for the serving state

- **Alternative:** serve `GET /serving-state` through CloudFront/API Gateway response caching
  with a 1 s max-age; the hot read never reaches the region.
- **What it buys:** removes the DAX cluster entirely for the singleton; edge latency;
  potentially large savings on the HTTP tier (the true cost center,
  [doc 06 §6](06-scaling-and-cost.md#6-event-window-total-order-of-magnitude)).
- **Cost:** correctness posture unchanged (still 1 s-stale, still safe by monotonicity) but
  the *fan-item* read remains, so DAX-or-not is decided by the singleton alone; split
  endpoints and cache-key discipline required.
- **Verdict:** kept as the natural production evolution; the submission's primary design
  stays DAX + polling because it is simpler to specify precisely, keeps the solution
  DynamoDB-centric (this is a DynamoDB challenge), and the swap remains straightforward
  later.

## 4. Push (WebSocket / AppSync) vs polling

- **What it buys:** near-zero notification latency; no polling load at all.
- **Cost:** 1M+ concurrent stateful connections (API Gateway WebSocket / AppSync connection
  quotas, connection-table state, reconnect storms after any blip — a reconnect stampede
  rivals the entry stampede), plus a broadcast fan-out problem when the threshold moves.
- **Verdict:** polling + 1 s cached singleton delivers a ≤ ~6 s eligibility-notification
  bound ([doc 04 §4](04-status-queries.md#4-staleness-bound-and-why-it-is-safe)) with zero
  connection state. For a waiting room, seconds-late is imperceptible; reconnect-storm
  fragility is not. Poll wins.

## 5. Provisioned + scheduled scaling vs on-demand + warm throughput

- **What it buys:** provisioned capacity for the event window is cheaper per unit
  (~$0.00065/WCU-hour vs $0.625/M requests) if utilization is high and the window is known.
- **Cost:** you must predict the peak *exactly* — under-provisioning throttles the stampede
  (the one unforgivable failure), over-provisioning pays for air; auto-scaling reacts in
  minutes, far too slow for a 15 s burst; table-level max-throughput quotas apply either way.
- **Verdict:** on-demand + warm throughput. Warm throughput gives the guaranteed floor
  (like provisioned) while billing only for actual requests (like on-demand) — the correct
  hybrid for a 15-second spike followed by a long polling tail. Provisioned remains
  reasonable for the *steady* post-burst hours if cost-tuning an actual event.

## 6. Fixed-size vs deficit-driven promotion batches

- **What fixed buys:** trivially predictable admission waves; no dependency on the
  `ActiveCounter` read.
- **Cost:** open-loop control. Too big → standing 429 herd at `/token`; too small → idle
  purchasing slots. Correct size *is* the deficit — a fixed number is just a stale guess at
  it.
- **Verdict:** deficit-driven (closed-loop) with a fixed-size policy only for the `PRE→OPEN`
  first tick, when no admission history exists
  ([doc 03 §2](03-batch-promotion.md#2-what-triggers-promotion-and-how-batch-size-is-determined)).

## 7. Other rejected shapes (one line each)

- **Global counter at entry** (assign positions synchronously): one-item ceiling ~1k/s —
  0.1% of required scale; also couples the hot path to coordination (violates Decision A).
- **`PK = eventId, SK = position`**: the entire stampede into one partition; fails
  immediately.
- **Per-fan eligibility writes at promotion**: O(batch) writes per tick and a race window
  mid-batch; the monotonic threshold does it in O(1)
  ([doc 03 §1](03-batch-promotion.md#1-the-centerpiece-o1-promotion-via-a-monotonic-serving-threshold)).
- **ElastiCache/Redis or Kinesis as the queue backbone**: excluded by challenge framing —
  the queue of record must be DynamoDB; DAX is the only cache, and only on the read path.
- **KMS `Sign` per token**: adds a quota-bound external call on the token path for zero
  security gain over an in-memory event-scoped key wrapped by KMS
  ([doc 02 §7](02-fair-ordering.md#7-verifiable-positions-signed-tokens)).

## 8. Limitations

Stated plainly, one line each:

- **Payment flow** — out of scope; the design hands purchasing a verifiable admission JWT
  and a capacity guarantee, nothing more.
- **Bot-detection depth** — out of scope; WAF/identity layering is assumed, and the queue's
  guarantee is only that speed buys no position advantage (no amplification).
- **Multi-region** — out of scope; the design is single-region. Global tables would break
  the strongly-consistent conditional writes the admission plane relies on; a multi-region
  variant would partition fans by home region with per-region capacity shares.
- **Identity / Sybil resistance** — assumed upstream ("one entry per authenticated fan");
  the strength of "one fan, one place in line" is exactly the strength of that identity.
