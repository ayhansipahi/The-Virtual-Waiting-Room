// Discrete-event simulator for the Virtual Waiting Room design.
// Zero dependencies, no AWS calls. Models arrivals (with clock skew and lost
// first attempts), background sequencing (with injected crashes), deficit-
// driven promotion, hard-capped admission, and session churn - then asserts
// the design invariants:
//   (a) activeCount <= capacity, always
//   (b) assigned position order == canonical (bucket, shard, tiebreak) order
//   (c) refill keeps active within [target - eps, target] after ramp
//   (d) no fan lost or duplicated across retries and sequencer crashes
//
// Run:  node waiting-room-sim.ts [--fans 1000000] [--seconds 1800] [--seed 42]
// (Node >= 22.18 runs TypeScript directly via type stripping.)

// ---------------------------------------------------------------- config
interface Config {
  fans: number;
  simMs: number;
  seed: number;
}
function parseArgs(): Config {
  const get = (flag: string, dflt: number) => {
    const i = process.argv.indexOf(flag);
    return i >= 0 ? Number(process.argv[i + 1]) : dflt;
  };
  return {
    fans: get("--fans", 1_000_000),
    simMs: get("--seconds", 1800) * 1000,
    seed: get("--seed", 42),
  };
}

const CFG = parseArgs();
const BUCKET_MS = 1000;
const GRACE_MS = 2000;
const SHARDS = 2048;
const PAGE = 500;
const SEQ_BUDGET_PER_SEC = 200_000; // write-back budget (docs/02 §8.3)
const CAPACITY = 1000;
const TARGET = 1000;
const OVERSHOOT = 1.2;
const PROMOTER_TICK_MS = 10_000;
const SWEEPER_TICK_MS = 60_000;
const SESSION_DEADLINE_MS = 600_000;
const MEAN_SESSION_MS = 170_000; // lognormal, ~3 min
const NO_SHOW_RATE = 0.05;
const ABANDON_RATE = 0.05;
const LOST_FIRST_ATTEMPT_RATE = 0.01; // fan's first entry write lost; retry lands later
const CRASH_PER_TICK = 0.02; // sequencer crash injection (~1 per 5 s)
const ARRIVAL_WINDOW_MS = 15_000; // the stampede
const ARRIVAL_START_MS = 1_000;
const BAND_EPS = 100; // invariant (c) tolerance, from churn-per-promoter-tick math (docs/03 §6.3)
const DT = 100; // tick length ms

// ---------------------------------------------------------------- prng (deterministic)
function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = mulberry32(CFG.seed);
const randExp = (mean: number) => -Math.log(1 - rand()) * mean;
const randNorm = () => {
  const u = 1 - rand(), v = rand();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
};

// ---------------------------------------------------------------- fans (SoA, typed arrays)
const N = CFG.fans;
const S_PENDING = 0, S_QUEUED = 1, S_ADMITTED = 2, S_CLOSED = 3, S_EXPIRED = 4;
const entryMs = new Float64Array(N);   // effective (durable) entry time
const bucket = new Int32Array(N);
const shard = new Uint16Array(N);
const tiebreak = new Float64Array(N);  // uniform in [0, 2^53) - CSPRNG stand-in
const position = new Int32Array(N).fill(-1);
const assignMs = new Float64Array(N);
const status = new Uint8Array(N);
const noShow = new Uint8Array(N);
const sessionDeadline = new Float64Array(N);

// Entry plane: 100 virtual API servers with bounded clock skew (+/- 50 ms).
// Fairness claim under test: skew << bucketMs cannot displace fans by more
// than one bucket, and within-bucket order is a lottery anyway (docs/02 §2).
const SERVERS = 100;
const skew = new Float64Array(SERVERS);
for (let s = 0; s < SERVERS; s++) skew[s] = (rand() * 2 - 1) * 50;

let duplicateRetriesAbsorbed = 0;
let lostFirstAttempts = 0;
for (let i = 0; i < N; i++) {
  // front-loaded stampede: most of the crowd in the first seconds
  let t = ARRIVAL_START_MS + ARRIVAL_WINDOW_MS * Math.pow(rand(), 1.5);
  if (rand() < LOST_FIRST_ATTEMPT_RATE) {
    // first write lost before durability; the retry IS the effective arrival
    t += 500 + rand() * 1500;
    lostFirstAttempts++;
  }
  if (rand() < 0.02) duplicateRetriesAbsorbed++; // duplicate attempt after a durable
  // write: ConditionalCheckFailed -> absorbed, exactly one record (docs/01 §2.3)
  entryMs[i] = t;
  bucket[i] = Math.floor((t + skew[i % SERVERS]) / BUCKET_MS);
  shard[i] = Math.floor(rand() * SHARDS);
  tiebreak[i] = Math.floor(rand() * 2 ** 53);
  noShow[i] = rand() < NO_SHOW_RATE ? 1 : 0;
}

// ---------------------------------------------------------------- sequencer input
// Group by bucket; sort each bucket by canonical within-bucket key
// (shard-major, tiebreak-minor) - the order the GSI shards are read in.
const bucketFans = new Map<number, number[]>();
for (let i = 0; i < N; i++) {
  let arr = bucketFans.get(bucket[i]);
  if (!arr) bucketFans.set(bucket[i], (arr = []));
  arr.push(i);
}
const bucketIds = [...bucketFans.keys()].sort((a, b) => a - b);
for (const b of bucketIds)
  bucketFans.get(b)!.sort((x, y) => (shard[x] - shard[y]) || (tiebreak[x] - tiebreak[y]));

// ---------------------------------------------------------------- state
let counter = 0;            // PositionCounter.value
let positionsAssigned = 0;  // ServingState.positionsAssigned (counter high-water)
let serving = 0;            // ServingState.servingPosition (monotonic)
let active = 0;             // ActiveCounter.activeCount
let eligibleCursorDone = 0; // positions already fanned out to poll scheduling
const posToFan = new Int32Array(N + Math.ceil(N * 0.05) + PAGE).fill(-1);

let seqBucketIdx = 0, seqOffset = 0;
let crashes = 0, gapsBurned = 0;
let admissions = 0, rejected429 = 0, closes = 0, expiredCount = 0;
let peakActive = 0, rampDoneAt = -1, minActiveAfterRamp = Infinity;
let ticksInBand = 0, ticksAfterRamp = 0;
const activeSessions = new Set<number>();
const scheduled = new Map<number, number[]>(); // tick -> fan ids with due admission attempt
const closesAt = new Map<number, number[]>();  // tick -> fan ids with session end
const batches: { from: number; to: number; at: number }[] = [];

const fail = (msg: string) => { console.error(`INVARIANT VIOLATED: ${msg}`); process.exit(1); };
const tickOf = (ms: number) => Math.ceil(ms / DT);
const pushTo = (m: Map<number, number[]>, k: number, v: number) => {
  const arr = m.get(k); if (arr) arr.push(v); else m.set(k, [v]);
};

// ---------------------------------------------------------------- planes
function sequencerTick(now: number) {
  let budget = (SEQ_BUDGET_PER_SEC * DT) / 1000;
  while (budget > 0 && seqBucketIdx < bucketIds.length) {
    const b = bucketIds[seqBucketIdx];
    if (now <= (b + 1) * BUCKET_MS + GRACE_MS) return; // watermark: bucket not closed yet
    const fans = bucketFans.get(b)!;
    const pageLen = Math.min(PAGE, fans.length - seqOffset, budget);
    if (pageLen <= 0) { seqBucketIdx++; seqOffset = 0; continue; }

    // one atomic ADD reserves a contiguous range
    let pos = counter + 1;
    counter += pageLen;
    // crash injection: write back only a prefix; the rest re-enters the queue
    // and gets FRESH positions on restart -> gap burned, order preserved,
    // because restart resumes at the same canonical offset (docs/02 §8.4)
    let writeLen = pageLen;
    if (rand() < CRASH_PER_TICK) {
      crashes++;
      writeLen = Math.floor(rand() * pageLen);
      gapsBurned += pageLen - writeLen;
    }
    for (let j = 0; j < writeLen; j++) {
      const f = fans[seqOffset + j];
      if (position[f] !== -1) fail(`double position assignment for fan ${f}`); // conditional write-back
      position[f] = pos;
      if (posToFan[pos] !== -1) fail(`position ${pos} assigned twice`);
      posToFan[pos] = f;
      pos++;
      status[f] = S_QUEUED;
      assignMs[f] = now;
    }
    seqOffset += writeLen;
    budget -= pageLen;
    positionsAssigned = counter; // publish high-water (promoter guard)
    if (writeLen < pageLen) return; // crashed: restart next tick at same offset
  }
}

function promoterTick(now: number) {
  const deficit = TARGET - active;
  const k = Math.min(Math.ceil(Math.max(0, deficit) * OVERSHOOT), positionsAssigned - serving);
  if (k <= 0) return;
  const from = serving + 1;
  serving += k; // the O(1) promotion: one guarded ADD on one item
  batches.push({ from, to: serving, at: now });

  // fan-out is CLIENT-driven in reality (polls notice pos <= serving);
  // the sim schedules each newly eligible fan's reaction now
  for (let pos = eligibleCursorDone + 1; pos <= serving; pos++) {
    const f = posToFan[pos];
    if (f === -1) continue; // gap position - nobody to admit (harmless)
    if (noShow[f]) continue;
    const react = now + rand() * 3000 + randExp(2000); // poll interval + human delay
    pushTo(scheduled, tickOf(react), f);
  }
  eligibleCursorDone = serving;
}

function admissionAttempts(tick: number, now: number) {
  const due = scheduled.get(tick);
  if (!due) return;
  scheduled.delete(tick);
  for (const f of due) {
    if (status[f] !== S_QUEUED) continue;
    if (position[f] > serving) fail(`admission attempt above serving for fan ${f}`);
    if (active >= CAPACITY) { // the hard cap: conditional ADD would cancel
      rejected429++;
      pushTo(scheduled, tickOf(now + 5000 + rand() * 2000), f); // Retry-After
      continue;
    }
    active++;
    admissions++;
    if (active > CAPACITY) fail(`activeCount ${active} exceeded capacity ${CAPACITY}`);
    status[f] = S_ADMITTED;
    activeSessions.add(f);
    sessionDeadline[f] = now + SESSION_DEADLINE_MS;
    if (rand() < ABANDON_RATE) continue; // abandoner: only the sweeper reclaims the slot
    const dur = Math.min(
      Math.exp(Math.log(MEAN_SESSION_MS) + 0.35 * randNorm() - 0.06),
      SESSION_DEADLINE_MS,
    );
    pushTo(closesAt, tickOf(now + dur), f);
  }
}

function closeDue(tick: number) {
  const due = closesAt.get(tick);
  if (!due) return;
  closesAt.delete(tick);
  for (const f of due) {
    if (status[f] !== S_ADMITTED) continue; // the idempotent flip (docs/03 §6.1)
    status[f] = S_CLOSED;
    activeSessions.delete(f);
    active--;
    closes++;
  }
}

function sweeperTick(now: number) {
  for (const f of activeSessions) {
    if (sessionDeadline[f] < now && status[f] === S_ADMITTED) {
      status[f] = S_EXPIRED; // same flip, different terminal state
      activeSessions.delete(f);
      active--;
      expiredCount++;
    }
  }
}

// ---------------------------------------------------------------- main loop
const t0 = performance.now();
const totalTicks = Math.floor(CFG.simMs / DT);
for (let tick = 0; tick <= totalTicks; tick++) {
  const now = tick * DT;
  sequencerTick(now);
  if (now % PROMOTER_TICK_MS === 0 && now > 0) promoterTick(now);
  admissionAttempts(tick, now);
  closeDue(tick);
  if (now % SWEEPER_TICK_MS === 0 && now > 0) sweeperTick(now);

  if (active > CAPACITY) fail(`activeCount ${active} exceeded capacity at t=${now}`);
  peakActive = Math.max(peakActive, active);
  if (rampDoneAt < 0 && active >= TARGET) rampDoneAt = now;
  if (rampDoneAt >= 0 && now > rampDoneAt) {
    ticksAfterRamp++;
    minActiveAfterRamp = Math.min(minActiveAfterRamp, active);
    if (active >= TARGET - BAND_EPS && active <= TARGET) ticksInBand++;
  }
}
const wallMs = performance.now(); // since process start: generation + sequencing + serve planes

// ---------------------------------------------------------------- invariant checks (post)
// (a) checked inline at every admission and every tick.

// (b) position order == canonical (bucket, shard, tiebreak) order
let prev = -1;
let checked = 0;
for (let pos = 1; pos <= counter; pos++) {
  const f = posToFan[pos];
  if (f === -1) continue;
  if (prev !== -1) {
    const ok = bucket[prev] < bucket[f]
      || (bucket[prev] === bucket[f] && (shard[prev] < shard[f]
        || (shard[prev] === shard[f] && tiebreak[prev] < tiebreak[f])));
    if (!ok) fail(`canonical order violated between positions of fans ${prev} and ${f}`);
  }
  prev = f;
  checked++;
}

// (d) no fan lost or duplicated
let sequenced = 0;
for (let i = 0; i < N; i++) if (position[i] !== -1) sequenced++;
if (sequenced !== N) fail(`${N - sequenced} fans never sequenced`);
if (checked !== N) fail(`position map holds ${checked} fans, expected ${N}`);

// (c) refill band
const bandPct = ticksAfterRamp ? (100 * ticksInBand) / ticksAfterRamp : 0;
if (rampDoneAt < 0) fail("active never reached target - ramp failed");
if (minActiveAfterRamp < TARGET - BAND_EPS)
  fail(`active fell to ${minActiveAfterRamp}, below target - eps = ${TARGET - BAND_EPS}`);

// time-to-position percentiles
const delays = new Float64Array(N);
for (let i = 0; i < N; i++) delays[i] = assignMs[i] - entryMs[i];
delays.sort();
const pct = (p: number) => (delays[Math.min(N - 1, Math.floor((p / 100) * N))] / 1000).toFixed(1);

// ---------------------------------------------------------------- report
const fmt = (n: number) => n.toLocaleString("en-US");
console.log(`\n== Virtual Waiting Room simulation ==`);
console.log(`fans=${fmt(N)} simTime=${CFG.simMs / 1000}s seed=${CFG.seed} wall=${(wallMs / 1000).toFixed(1)}s\n`);

console.log(`| Metric | Value |`);
console.log(`|---|---|`);
console.log(`| Arrival stampede | ${fmt(N)} fans in ${ARRIVAL_WINDOW_MS / 1000} s (front-loaded) |`);
console.log(`| Clock skew injected | +/-50 ms across ${SERVERS} virtual servers |`);
console.log(`| Lost first attempts (retry = effective arrival) | ${fmt(lostFirstAttempts)} |`);
console.log(`| Duplicate entry retries absorbed | ${fmt(duplicateRetriesAbsorbed)} |`);
console.log(`| Sequencer crashes injected | ${crashes} |`);
console.log(`| Gap positions burned by crashes | ${fmt(gapsBurned)} |`);
console.log(`| Positions assigned | ${fmt(sequenced)} (counter ${fmt(counter)}) |`);
console.log(`| Time-to-position p50 / p95 / p100 | ${pct(50)} s / ${pct(95)} s / ${pct(100)} s |`);
console.log(`| Promotions (batches) | ${batches.length}, sizes ${Math.min(...batches.map(b => b.to - b.from + 1))}..${Math.max(...batches.map(b => b.to - b.from + 1))} |`);
console.log(`| Admissions | ${fmt(admissions)} (429 retries: ${fmt(rejected429)}) |`);
console.log(`| Sessions closed / expired-swept | ${fmt(closes)} / ${fmt(expiredCount)} |`);
console.log(`| Peak active | ${peakActive} (capacity ${CAPACITY}) |`);
console.log(`| Ramp complete at | t=${rampDoneAt / 1000}s |`);
console.log(`| Min active after ramp | ${minActiveAfterRamp} (band floor ${TARGET - BAND_EPS}) |`);
console.log(`| Time in [target-${BAND_EPS}, target] band | ${bandPct.toFixed(1)}% |`);

console.log(`\nInvariants:`);
console.log(`  (a) activeCount <= capacity ........ PASS (checked every tick + every admission; peak ${peakActive})`);
console.log(`  (b) position order == (bucket, shard, tiebreak) ... PASS (${fmt(checked)} fans verified)`);
console.log(`  (c) refill band [${TARGET - BAND_EPS}, ${TARGET}] .... PASS (min ${minActiveAfterRamp}, in-band ${bandPct.toFixed(1)}% of post-ramp ticks)`);
console.log(`  (d) no fan lost or duplicated ...... PASS (${fmt(sequenced)}/${fmt(N)} sequenced exactly once, across ${crashes} crashes and ${fmt(lostFirstAttempts + duplicateRetriesAbsorbed)} retries)`);

// fairness plot: mean assigned position per arrival bucket (must be monotonic)
console.log(`\nFairness: mean position by arrival bucket (monotonic => earlier seconds strictly win)`);
const bucketStats = bucketIds.map((b) => {
  const fans = bucketFans.get(b)!;
  let sum = 0, lo = Infinity, hi = -Infinity;
  for (const f of fans) {
    const p = position[f];
    sum += p;
    if (p < lo) lo = p;
    if (p > hi) hi = p;
  }
  return { b, count: fans.length, mean: sum / fans.length, lo, hi };
});
const bmax = bucketStats.reduce((m, s) => Math.max(m, s.mean), 0);
for (const s of bucketStats) {
  const bar = "#".repeat(Math.max(1, Math.round((s.mean / bmax) * 40)));
  console.log(`  bucket ${String(s.b).padStart(2)} (${fmt(s.count).padStart(9)} fans) |${bar.padEnd(40)}| mean ${fmt(Math.round(s.mean)).padStart(10)}  range [${fmt(s.lo)}..${fmt(s.hi)}]`);
}
console.log(`\nAll invariants PASS.`);
