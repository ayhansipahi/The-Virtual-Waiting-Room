#!/usr/bin/env node
// Validates workbench/waiting-room-model.json against the NoSQL Workbench
// data-model shape and this repo's sample-data requirements (every item type,
// every fan status, same-bucket fans with distinct tiebreaks).
// Exit 0 = valid; non-zero with a message otherwise. Run in CI.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import assert from "node:assert/strict";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const model = JSON.parse(
  readFileSync(join(root, "workbench", "waiting-room-model.json"), "utf8"),
);

// --- schema shape ---
assert.equal(typeof model.ModelName, "string", "ModelName missing");
assert.ok(model.ModelMetadata?.Author, "ModelMetadata.Author missing");
assert.ok(Array.isArray(model.DataModel) && model.DataModel.length === 1, "DataModel[] missing");

const t = model.DataModel[0];
assert.equal(t.TableName, "WaitingRoom");
assert.equal(t.KeyAttributes.PartitionKey.AttributeName, "PK");
assert.equal(t.KeyAttributes.SortKey.AttributeName, "SK");
assert.ok(Array.isArray(t.NonKeyAttributes) && t.NonKeyAttributes.length > 0, "NonKeyAttributes missing");

const gsi = t.GlobalSecondaryIndexes?.[0];
assert.equal(gsi?.IndexName, "SequencingIndex", "GSI SequencingIndex missing");
assert.equal(gsi.KeyAttributes.PartitionKey.AttributeName, "gsi1pk");
assert.equal(gsi.KeyAttributes.SortKey.AttributeName, "gsi1sk");
assert.equal(gsi.Projection.ProjectionType, "KEYS_ONLY");

// --- sample-data requirements ---
const rows = t.TableData;
assert.ok(rows.length >= 20, `need >= 20 rows, got ${rows.length}`);

for (const [i, row] of rows.entries()) {
  assert.ok(row.PK?.S && row.SK?.S && row.type?.S, `row ${i} missing PK/SK/type`);
  for (const [attr, val] of Object.entries(row)) {
    const [dtype, raw] = Object.entries(val)[0];
    assert.ok(["S", "N"].includes(dtype), `row ${i} ${attr}: unexpected type ${dtype}`);
    if (dtype === "N") assert.ok(!Number.isNaN(Number(raw)), `row ${i} ${attr}: bad number`);
  }
}

const byType = (ty) => rows.filter((r) => r.type.S === ty);
const REQUIRED_TYPES = ["FanEntry", "PositionCounter", "ServingState", "ActiveCounter",
  "EventConfig", "Batch", "Session", "Lease"];
for (const ty of REQUIRED_TYPES) assert.ok(byType(ty).length > 0, `no ${ty} row`);

const fanStatuses = new Set(byType("FanEntry").map((r) => r.status.S));
for (const s of ["PENDING", "QUEUED", "ADMITTED", "CLOSED", "EXPIRED"])
  assert.ok(fanStatuses.has(s), `no FanEntry with status ${s}`);

// PENDING rows carry sparse GSI keys; sequenced rows must not
for (const r of byType("FanEntry")) {
  if (r.status.S === "PENDING") {
    assert.ok(r.gsi1pk?.S && r.gsi1sk?.S, "PENDING FanEntry missing sparse GSI keys");
    assert.ok(!r.queuePosition, "PENDING FanEntry must not have queuePosition");
  } else {
    assert.ok(!r.gsi1pk && !r.gsi1sk, `${r.status.S} FanEntry still carries GSI keys`);
    assert.ok(r.queuePosition?.N, `${r.status.S} FanEntry missing queuePosition`);
  }
}

// fairness visibility: >= 3 fans in one bucket, all tiebreaks distinct,
// positions consistent with (shard, tiebreak) order inside that bucket
const buckets = {};
for (const r of byType("FanEntry"))
  (buckets[r.arrivalBucket.N] ??= []).push(r);
const demo = Object.values(buckets).find((g) => g.length >= 3);
assert.ok(demo, "need >= 3 fans in one arrival bucket");
assert.equal(new Set(demo.map((r) => r.tiebreak.S)).size, demo.length, "tiebreaks not distinct");
const seq = demo.filter((r) => r.queuePosition)
  .sort((a, b) => Number(a.queuePosition.N) - Number(b.queuePosition.N));
for (let i = 1; i < seq.length; i++) {
  const [a, b] = [seq[i - 1], seq[i]];
  const ka = [Number(a.shard.N), a.tiebreak.S];
  const kb = [Number(b.shard.N), b.tiebreak.S];
  assert.ok(ka[0] < kb[0] || (ka[0] === kb[0] && ka[1] < kb[1]),
    "within-bucket positions violate (shard, tiebreak) order");
}

console.log(`workbench model OK: ${rows.length} rows, ` +
  `${REQUIRED_TYPES.length} item types, all 5 fan statuses, ` +
  `${demo.length} same-bucket fans with distinct tiebreaks`);
