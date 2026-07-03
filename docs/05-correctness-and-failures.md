# Correctness, Failure Modes, and Edge Cases

Each entry follows **threat → mechanism → residual risk**. Component-local failure tables
live with their components (docs [01](01-table-design.md)–[04](04-status-queries.md)); this
doc is the system-level view a reviewer can audit top to bottom.

## 1. Clock skew across the fleet

- **Threat:** servers stamping `arrivalBucket` disagree about the time; fans are ordered by
  which server they hit, not when they arrived.
- **Mechanism:** coarse 1,000 ms buckets vs. Amazon Time Sync-disciplined clocks (error
  typically ≪ 1 ms, single-digit ms degraded). Displacement probability
  `≤ maxSkewMs / bucketMs ≈ 0.1%`, worst effect = one bucket (1 s of queue). Within-bucket
  order is random regardless, so sub-bucket skew has zero effect on expected position.
  ([doc 02 §2](02-fair-ordering.md#2-clock-skew))
- **Residual risk:** a server with a *broken* (not merely skewed) clock could stamp wild
  buckets. Mitigation: reject buckets outside `[eventOpen, now + 2 s]` at write time —
  a one-line condition. Effectively nil.

## 2. Near-simultaneous arrivals / thundering herd

- **Threat:** 1M fans in one second overwhelm the table or force arbitrary ordering.
- **Mechanism:** absorb plane writes by uniform fan UUID (no hot partition possible), GSI
  writes spread over 2,048 random shards (~488 writes/s/shard at peak), warm throughput
  pre-set at 1.2× peak on table + GSI; ordering within the second is an explicit uniform
  lottery. ([doc 01 §2.2, §3.1](01-table-design.md), [doc 02 §3](02-fair-ordering.md#3-near-simultaneous-arrivals-at-scale))
- **Residual risk:** peak beyond design point (> 1.2M/s). Warm throughput is a config value —
  size it from marketing's reach estimates with margin; on-demand still adapts above it,
  just not instantly.

## 3. Queue-position gaming

- **Threat:** actors improve their position via timing, duplication, forgery, or bots.
- **Mechanism:** four defenses, one per vector —
  timing → within-bucket lottery (speed buys nothing);
  multi-entry → conditional put keyed by authenticated `fanId`;
  forgery → ES256-signed position/admission JWTs verified against JWKS;
  bots → WAF/edge rate-limiting + upstream identity.
  ([doc 02 §6–7](02-fair-ordering.md#6-gaming-prevention))
- **Residual risk:** Sybil attacks — many *real* verified identities — are an
  identity-layer problem no queue can solve; the queue guarantees only that it adds no
  amplification (speed ↛ position). Bot-detection depth is explicitly out of scope
  ([doc 07 §8](07-tradeoffs-and-alternatives.md#8-limitations)).

## 4. Retries and at-least-once delivery

**This is the direct answer to "without dropping or misordering fans."**

- **Threat:** clients, Lambda, Streams, Step Functions are all at-least-once; naive designs
  drop fans on lost responses or reorder them on retries.
- **Mechanism:** two system-wide rules.
  1. **Ordering is defined by recorded evidence, not processing order.** A fan's place is a
     pure function of `(arrivalBucket, shard, tiebreak)`, durable before the `202`. Whether
     the sequencer processes them first or last, crashes and re-reads, or runs twice — the
     computed position order is the same, because inputs, not schedules, define it.
  2. **Every write is idempotent or conditionally guarded** (each is called out inline where
     it appears): entry put — `attribute_not_exists(PK)`; position write-back —
     `attribute_not_exists(queuePosition)`; counter reservation — atomic `ADD` (duplicates
     burn gaps, never reuse positions); promotion — read-value condition + `≤
     positionsAssigned`; admission — status-flip condition inside a transaction; session
     close — `ADMITTED → CLOSED/EXPIRED` flip. A retry of any step is either a no-op or a
     clean resume.
  Dropped is impossible after the `202` (evidence durable, sweep passes pick up index
  stragglers, and un-sequenced items remain in the sparse GSI until processed — the backlog
  is self-enumerating). Misordered is impossible because order is data.
- **Residual risk:** a fan who never gets the `202` and never retries was never durably in
  the queue — client SDKs retry by default; this is the correct semantics.

## 5. GSI eventual consistency and late writes

- **Threat:** the sequencer reads a bucket's GSI partition before all of that bucket's index
  writes have propagated; stragglers would be skipped.
- **Mechanism:** watermark (`process B only after (B+1)·bucketMs + graceMs`, grace 2 s ≫
  typical GSI lag) + a second sweep pass per bucket before completion + the sparse-index
  property that any item still carrying `gsi1pk` *will* appear eventually and can never be
  lost, only sequenced late. Fairness definition covers the semantics: effective arrival =
  durable landing. ([doc 02 §4, §8.2](02-fair-ordering.md))
- **Residual risk:** GSI lag beyond grace + sweep window under extreme replication pressure —
  affected fans are appended a few buckets late but never dropped. Monitoring hook:
  `positionsAssigned` vs. entry count.

## 6. Sequencer crash / restart

- **Threat:** crash mid-bucket corrupts ordering or loses fans.
- **Mechanism:** lease for single-running efficiency (correctness never depends on it);
  atomic counter reservation + conditional write-back mean a restart (or accidental double
  run) can only burn gap positions; processed items self-remove from the sparse index so
  restart work shrinks monotonically. ([doc 02 §8.4](02-fair-ordering.md#84-failure-modes))
- **Residual risk:** gaps make `servingPosition − admittedCount` an overestimate of truly
  eligible fans; the deficit-driven promoter self-corrects within one tick.

## 7. Promoter overshoot vs. the hard cap

- **Threat:** promoter bug/misconfig makes far more fans eligible than purchasing can hold.
- **Mechanism:** two-layer control — the threshold is soft; the admission transaction's
  `activeCount < capacity` condition is the invariant. Overshoot ⇒ brief 429s, never > 1,000
  active. ([doc 03 §4](03-batch-promotion.md#4-why-over-promotion-cannot-overwhelm-purchasing-the-two-layer-control))
- **Residual risk:** sustained extreme overshoot creates a 429-retry herd on `/token` —
  rate-limited at the edge; promoter condition `≤ positionsAssigned` bounds how far it can
  run ahead.

## 8. DAX staleness

- **Threat:** cached reads mislead fans or, worse, drive incorrect decisions.
- **Mechanism:** staleness is bounded (`≤ TTL 1 s` + poll interval) and **conservative-only**
  because `servingPosition` is monotonic — a stale read under-reports eligibility, never
  over-reports it. Every correctness-critical read (admission, promoter control loop)
  bypasses DAX with strong consistency. ([doc 04 §4](04-status-queries.md#4-staleness-bound-and-why-it-is-safe))
- **Residual risk:** none on correctness; UX shows eligibility up to ~6 s late.

## 9. TTL deletion lag

- **Threat:** DynamoDB TTL deletes minutes late; abandoned sessions squat on capacity.
- **Mechanism:** the wall-clock `deadline` attribute is authoritative; a 60 s sweeper over
  the bounded session partition closes overdue sessions via the idempotent flip; TTL is
  demoted to byte cleanup. ([doc 03 §6.2](03-batch-promotion.md#62-the-three-close-paths))
- **Residual risk:** capacity reclaim latency ≤ deadline + 60 s sweep interval — sized into
  `capacityTarget` headroom.

## 10. Hot-partition audit

Design rule of thumb: ~1,000 WCU/s and ~3,000 RCU/s per physical partition; a single item
lives on exactly one partition (adaptive capacity cannot split one item).

| Item / key family | Cardinality | Peak write rate | Peak read rate (to DynamoDB) | Verdict |
|---|---|---|---|---|
| `FanEntry` (`FAN#<uuid>`) | 10M | 1M/s aggregate, uniform → ~per-partition trivial | ~333k/s aggregate, uniform | **Safe** — uniform by construction; warm throughput sized 1.2× |
| GSI1 partitions (`B#evt#bucket#shard`) | 2,048/bucket | 1M/s ÷ 2,048 ≈ 488/s per shard | sequencer Query, ~1 page/s/shard | **Safe** — sharded below 800/s target with headroom |
| `PositionCounter` | 1 | ≤ 400 `ADD`/s (batched, page 500) | rare | **Safe** — batching keeps one-item writes ≤ 40% of ceiling |
| `ServingState` | 1 | ~0.1/s | **~N (DAX nodes)/s** — was 333k/s before DAX | **Safe only because of DAX** — the one deliberately-tamed hot spot |
| `ActiveCounter` | 1 | ≤ ~100/s (admissions + closes, ramp worst case) | promoter + admission, strongly consistent, ≤ ~100/s | **Safe** — an order of magnitude under ceiling |
| `Session` partition (`EVT#` + `SESSION#`) | ≤ ~1k live | tens/s | sweeper Query 1/60 s | **Safe** — bounded population, bounded rates |
| `EventConfig`, `Batch`, `Lease` | 1 each / few | ≪ 1/s | ≪ 10/s | **Safe** — noise |
| `EVT#<eventId>` partition aggregate | — | ≤ ~600 WCU/s total ([doc 01 §2.4](01-table-design.md#24-why-the-evteventid-partition-is-safe)) | ≤ ~200 reads/s direct | **Safe** — every coordination item summed stays under one partition's ceiling |

## 11. Multi-event isolation

- **Threat:** two overlapping on-sales interfere.
- **Mechanism:** `eventId` is in every SK, every GSI key, every counter identity — state,
  counters, thresholds, and sessions are disjoint per event; the same fan may hold one
  `FanEntry` per event under one `FAN#` partition key.
- **Residual risk:** shared table-level warm throughput and account quotas — capacity
  planning must sum concurrent events.

## 12. Sellout / event close

- **Threat:** undefined behavior when tickets run out with millions still queued.
- **Mechanism:** ops sets `eventState = CLOSED` on `ServingState` (one write). Promoter
  no-ops; admission rejects (`eventState` checked in the same strongly consistent read);
  status API — the one item everyone already polls — carries the closed state to every fan
  within ~1 poll interval, with clear messaging. `PAUSED` works identically for incident
  freeze. Post-event, `ttl` on `FanEntry` garbage-collects the 10M items.
- **Residual risk:** fans mid-admission at close complete their sessions (drain, not
  guillotine) — intended.

## 13. A note on scale

At 1,000 concurrent purchasers with ~3-minute sessions, throughput is ~5.5 admissions/s ≈
20,000 fans/hour. Draining 10M fans would take ~21 days — but no real inventory survives
remotely that long. The waiting room's actual job is to keep the first minutes *fair and
stable*: absorb everyone, order everyone verifiably, admit at exactly the rate purchasing
can bear, and tell the other 9.99M fans the truth (position, a realistic ETA, sellout state)
quickly and cheaply. This design optimizes for that job, and says so.
