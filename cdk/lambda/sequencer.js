// Sequencer - background position assignment (docs/02 §8)
// Converts durable arrival evidence into integer positions. Runs on a 1 s
// Step Functions tick; must outpace admission (~5.5/s), never arrivals.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient, QueryCommand, UpdateCommand,
} = require("@aws-sdk/lib-dynamodb");

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;
const EVENT_ID = process.env.EVENT_ID;
const SHARD_COUNT = 2048;
const BUCKET_MS = 1000;
const GRACE_MS = 2000;
const PAGE_SIZE = 500;
const LEASE_MS = 15000;
const OWNER = process.env.AWS_LAMBDA_LOG_STREAM_NAME ?? "sequencer";

const evtKey = (sk) => ({ PK: `EVT#${EVENT_ID}`, SK: sk });

async function acquireLease() {
  try {
    await ddb.send(new UpdateCommand({
      TableName: TABLE,
      Key: evtKey("LEASE#SEQUENCER"),
      UpdateExpression: "SET #o = :me, expiresAt = :exp, #t = :ty",
      ConditionExpression:
        "attribute_not_exists(expiresAt) OR expiresAt < :now OR #o = :me",
      ExpressionAttributeNames: { "#o": "owner", "#t": "type" },
      ExpressionAttributeValues: {
        ":me": OWNER, ":exp": Date.now() + LEASE_MS, ":now": Date.now(), ":ty": "Lease",
      },
    }));
    return true;
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") return false; // someone else runs
    throw err;
  }
}

// Reserve a contiguous position range with one atomic ADD. Crash after this
// but before write-back burns a gap - harmless (docs/01 §2.5).
async function reserveRange(n) {
  const res = await ddb.send(new UpdateCommand({
    TableName: TABLE,
    Key: evtKey("CTR#POSITION"),
    UpdateExpression: "ADD #v :n SET #t = if_not_exists(#t, :ty)",
    ExpressionAttributeNames: { "#v": "value", "#t": "type" },
    ExpressionAttributeValues: { ":n": n, ":ty": "PositionCounter" },
    ReturnValues: "UPDATED_NEW",
  }));
  return res.Attributes.value - n + 1; // first position of the reserved range
}

async function sequenceBucket(bucket) {
  let assigned = 0;
  for (let shard = 0; shard < SHARD_COUNT; shard++) {
    let cursor;
    do {
      const page = await ddb.send(new QueryCommand({
        TableName: TABLE,
        IndexName: "SequencingIndex",
        KeyConditionExpression: "gsi1pk = :pk",
        ExpressionAttributeValues: { ":pk": `B#${EVENT_ID}#${bucket}#${shard}` },
        Limit: PAGE_SIZE,
        ExclusiveStartKey: cursor,
        // gsi1sk ascending = tiebreak order (canonical within-bucket lottery)
      }));
      cursor = page.LastEvaluatedKey;
      if (!page.Items?.length) break;

      let pos = await reserveRange(page.Items.length);
      // Parallel write-back: uniform PKs, conditional so restarts/double-runs
      // can never reassign or reorder (docs/02 §8.2).
      await Promise.all(page.Items.map((it) => {
        const fanId = it.gsi1sk.split("#")[1];
        const myPos = pos++;
        return ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `FAN#${fanId}`, SK: `EVT#${EVENT_ID}` },
          UpdateExpression:
            "SET queuePosition = :p, #s = :q, batchId = :b REMOVE gsi1pk, gsi1sk",
          ConditionExpression: "attribute_not_exists(queuePosition)",
          ExpressionAttributeNames: { "#s": "status" },
          ExpressionAttributeValues: {
            ":p": myPos, ":q": "QUEUED",
            ":b": `B-${String(Math.ceil(myPos / PAGE_SIZE)).padStart(6, "0")}`,
          },
        })).catch((err) => {
          if (err.name !== "ConditionalCheckFailedException") throw err; // already sequenced
        });
      }));
      assigned += page.Items.length;
    } while (cursor);
  }
  return assigned;
}

exports.handler = async () => {
  if (!(await acquireLease())) return { continue: true };

  // Watermark: only process buckets whose second is fully past + grace,
  // covering straggler writes and GSI eventual-consistency lag (docs/02 §8.2).
  // Cursor state (lowest unfinished bucket, sweep pass bookkeeping) lives on
  // the Lease item; abbreviated here to a single-bucket demo pass.
  const bucket = Math.floor((Date.now() - BUCKET_MS - GRACE_MS) / BUCKET_MS);
  const assigned = await sequenceBucket(bucket);

  if (assigned > 0) {
    // Publish the high-water mark the promoter's guard reads (docs/03 §3).
    await ddb.send(new UpdateCommand({
      TableName: TABLE,
      Key: evtKey("STATE#SERVING"),
      UpdateExpression: "ADD positionsAssigned :n SET #t = if_not_exists(#t, :ty)",
      ExpressionAttributeNames: { "#t": "type" },
      ExpressionAttributeValues: { ":n": assigned, ":ty": "ServingState" },
    }));
  }
  return { continue: true, bucket, assigned };
};
