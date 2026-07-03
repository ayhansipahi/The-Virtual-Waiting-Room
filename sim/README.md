# Discrete-Event Simulator

Zero-dependency TypeScript, no AWS calls. Models the full fan lifecycle —
stampede arrivals (clock skew, lost-then-retried first attempts, duplicate
retries), background sequencing (injected mid-page crashes), deficit-driven
promotion, hard-capped admission, session churn with abandoners and a 60 s
sweeper — and **asserts the design invariants** rather than just reporting:

- **(a)** `activeCount ≤ capacity` — checked at every admission and every tick
- **(b)** assigned position order equals canonical `(bucket, shard, tiebreak)`
  order — verified over every fan post-run (this is the "no misordering" claim)
- **(c)** refill keeps active within `[target − ε, target]` after ramp (ε = 100,
  from the churn-per-tick math in [docs/03 §6.3](../docs/03-batch-promotion.md#63-refill-dynamics-what-pinned-at-1000-means-quantitatively))
- **(d)** no fan lost or duplicated across all injected retries and crashes

Any violation exits non-zero, so CI treats the simulation as a test.

```sh
node waiting-room-sim.ts                       # 1M fans, 30 min sim time
npm run sim:quick                              # 100k fans (CI)
npm run sim:full                               # 10M fans - the design point
node waiting-room-sim.ts --fans 5e6 --seed 7   # custom
```

Requires Node ≥ 22.18 (runs TypeScript directly). Deterministic per seed.
The 10M design-point run finishes in single-digit seconds of wall time;
results are embedded in the [root README](../README.md#simulation-results).

Modeling notes (deliberate deviations from production):

- Fan reaction to eligibility is scheduled at promotion time (poll delay +
  human delay) instead of simulating 333k literal polls/s — identical admission
  dynamics at a fraction of the compute.
- A "lost first attempt" re-enters at retry time and its bucket is computed
  from the retry — exercising the fairness definition (effective arrival =
  durable write, [docs/02 §4](../docs/02-fair-ordering.md#4-late-writes-and-the-fairness-definition)).
- Sequencer crashes burn reserved-but-unwritten position ranges; the sim
  asserts order is preserved and gaps stay unassigned forever — the
  [gaps-are-harmless](../docs/01-table-design.md#25-position-gaps-are-harmless-state-this-once-rely-on-it-everywhere) claim under test.
