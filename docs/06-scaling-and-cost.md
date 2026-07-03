# Scaling and Cost Math

Order-of-magnitude engineering: every number below is `formula → value`, using public
us-east-1 reference prices (verify current pricing before an actual event; the *formulas* are
the deliverable, not the cents).

## 1. Assumptions

| Parameter | Value | Basis |
|---|---|---|
| Event size | 10,000,000 fans | challenge |
| Peak arrival | 1,000,000 entries/s for ~15 s | challenge ("within seconds") |
| Purchasing capacity (`capacityTarget`) | 1,000 concurrent | S1 |
| Mean session | 180 s | assumption |
| Steady admissions | `1000 / 180 ≈ 5.5/s` | derived |
| Poll interval | 3 s (2–5 s + jitter) | design |
| Event window analyzed | 1 h at full polling load | conservative |
| `FanEntry` item size | < 1 KB | schema |
| On-demand prices (us-east-1 reference) | $0.625 / M write units; $0.125 / M read units; transactional 2× | AWS pricing page |

## 2. Entry burst (writes)

```
burst writes      = 10M puts × (1 WCU base + 1 WCU GSI1 KEYS_ONLY)         = 20M write units
burst cost        = 20M / 1e6 × $0.625                                     ≈ $12.50
peak rate         = 1M/s base + 1M/s GSI                                   = 2M write units/s aggregate
warm throughput   = 1.2 × 1M/s on table AND 1.2 × 1M/s on GSI1             = 1.2M + 1.2M units/s
```

**Why on-demand alone is insufficient:** on-demand scales to ~2× a table's *previous* peak.
A fresh table has no previous peak; the first seconds of the on-sale would throttle exactly
when fairness matters most. Warm throughput pre-provisions the floor; on-demand billing still
applies per request. **Quota reality:** default per-table on-demand throughput quotas
(~40k write units/s) are ~30× below this design point — an event of this size is a planned
AWS engagement (quota increases + capacity review), not a default-account deployment. The
provisioned-capacity alternative is compared in
[doc 07 §5](07-tradeoffs-and-alternatives.md#5-provisioned--scheduled-scaling-vs-on-demand--warm-throughput).

## 3. Sequencer budget

```
allocation ceiling   = 1,000 counter ADDs/s × 500 page size      = 500k positions/s (not the binding limit)
write-back budget    = 100k–200k UpdateItem/s (Lambda concurrency knob; uniform PKs, table already warm)
write-back cost      = 10M × 1 WCU base                          = 10M write units ≈ $6.25
GSI cleanup (REMOVE) = 10M × 1 GSI write unit                    = 10M write units ≈ $6.25
counter ADDs         = 10M / 500 = 20k writes                    ≈ negligible
full sequencing time = 10M / 200k/s                              ≈ 50 s   (100 s at conservative 100k/s)
p50 time-to-position = 3 s watermark + 5M/200k                   ≈ 28 s   (< 30 s target)
p100                 = 3 + 10M/200k ≈ 53 s (conservative ~103 s) (< 3 min target)
```

Governing invariant: sequencing (100k+/s) ≫ admission (5.5/s). Slack factor ≈ 20,000×.

## 4. Read path cost: DAX vs no DAX

1M concurrent pollers × (1/3 s) = 333k req/s; each request = 2 GetItems.

| Path | Reads/s to DynamoDB | 1 h cost formula | 1 h cost |
|---|---|---|---|
| Fan items, no DAX | 333k | `333,333 × 0.5 RRU × 3600 / 1e6 × $0.125` | ≈ $75 |
| ServingState, no DAX | 333k against **one item** | — | **outage, not a cost** (~55× a partition's read ceiling — [doc 04 §1](04-status-queries.md#1-the-read-problem-stated-precisely)) |
| Fan items, via DAX (≈ all miss at 3 s poll vs 1 s TTL) | ~333k | same | ≈ $75 |
| ServingState, via DAX | ~N nodes ≈ 5–10 | `~10 × 0.5 × 3600 / 1e6 × $0.125` | ≈ $0.002 |

### DAX cluster sizing sketch

The cluster must *carry* the full request stream (667k GetItem ops/s including the ~all-miss
fan reads), even though it only *shields* DynamoDB from the singleton read.

```
per-node throughput (r-family, small items, mixed hit/miss)  ≈ 75k–100k ops/s   (validate by load test)
nodes ≥ 667k / 75k ≈ 9  → 10 × dax.r5.2xlarge (headroom + AZ spread)
cost ≈ 10 × ~$0.99/h ≈ $10/h for the event window
```

Cluster-wide TTL trade-off: DAX item-cache TTL is a **cluster-wide parameter** (parameter
group, `record-ttl-millis = 1000`). The 1 s value is dictated by `ServingState` freshness;
fan items would tolerate minutes (immutable post-sequencing except the admission flip), but
get 1 s because the knob is global. That is fine here: fan-item misses land on uniform keys
DynamoDB absorbs anyway (§4 table); the miss traffic is a cost line (~$75/h), not a risk. If
that line matters, the levers are a second cluster or direct fan-item reads
([doc 07 §3](07-tradeoffs-and-alternatives.md#3-edge-caching-vs-dax-for-the-serving-state)).

## 5. Admission / session plane (transactional writes)

```
admissions   = 3-item txn × 2× transactional WCU = 6 write units each
steady rate  = 5.5/s → 20k admissions/h → 120k write units/h ≈ $0.08/h
closes       = flip + decrement (+ session update) ≈ 3 write units each ≈ same order
streams      = TTL-expiry records only (filtered) — Lambda invocations, cents
```

The expensive-per-unit machinery (transactions) sits at the lowest-volume point of the
system — by design ([doc 03 §5](03-batch-promotion.md#5-component-spec-admission--post-eventseventidtoken)).

## 6. Event-window total (order of magnitude)

```
writes:  entry $12.5 + sequencing $12.5 + admission/session ≈ $0.2      ≈ $25
reads:   1 h full-load polling                                          ≈ $75/h
DAX:     10 nodes                                                       ≈ $10/h
glue:    Lambda (~1.2M entry GB-s + pollers), API GW/CloudFront at 333k req/s — the HTTP tier
         (~$1/M requests) actually dominates: ~$1.2k/h at full poll load; CDN caching of the
         status response is the lever ([doc 07 §3])
─────────────────────────────────────────────────────────────
DynamoDB+DAX for the whole event                                        ≈ low hundreds of dollars
```

The database is cheap; the HTTP fan-in is where money goes at mass-polling scale — which is
why the design keeps the response CDN-cacheable from day one
([doc 04 §3](04-status-queries.md#3-what-the-client-computes)).
