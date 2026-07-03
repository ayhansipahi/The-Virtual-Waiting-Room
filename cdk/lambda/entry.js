// Entry API - POST /events/{eventId}/enter (docs/01 §5)
// Absorb plane: compute evidence server-side, one idempotent conditional put.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, PutCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const crypto = require("node:crypto");

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;
const SHARD_COUNT = 2048;
const BUCKET_MS = 1000;

exports.handler = async (event) => {
  // Identity is upstream (authorizer); NEVER a client-supplied timestamp.
  const eventId = event.pathParameters?.eventId ?? process.env.EVENT_ID;
  const fanId = event.requestContext?.authorizer?.fanId ?? JSON.parse(event.body ?? "{}").fanId;
  if (!fanId) return { statusCode: 401, body: "unauthenticated" };

  const serverEpochMs = Date.now();
  const arrivalBucket = Math.floor(serverEpochMs / BUCKET_MS);
  const tiebreak = crypto.randomBytes(8).toString("hex"); // 64-bit CSPRNG, hex16
  const shard = crypto.randomInt(SHARD_COUNT);

  const item = {
    PK: `FAN#${fanId}`,
    SK: `EVT#${eventId}`,
    type: "FanEntry",
    status: "PENDING",
    arrivalBucket,
    tiebreak,
    shard,
    enteredAt: serverEpochMs,
    gsi1pk: `B#${eventId}#${arrivalBucket}#${shard}`, // sparse SequencingIndex key
    gsi1sk: `${tiebreak}#${fanId}`,
    ttl: Math.floor(serverEpochMs / 1000) + 86400,
  };

  let entry = item;
  try {
    // Idempotent: first write wins, retries are exact no-ops.
    await ddb.send(new PutCommand({
      TableName: TABLE,
      Item: item,
      ConditionExpression: "attribute_not_exists(PK)",
    }));
  } catch (err) {
    if (err.name !== "ConditionalCheckFailedException") throw err;
    const existing = await ddb.send(new GetCommand({
      TableName: TABLE,
      Key: { PK: item.PK, SK: item.SK },
    }));
    entry = existing.Item; // return the original evidence, not the retry's
  }

  // Entry receipt: ES256 JWT (sub, evt, iat) signed with the event-scoped key
  // (KMS GenerateDataKeyPair at cold start - docs/02 §7). Stubbed here.
  const receipt = { sub: fanId, evt: eventId, iat: Math.floor(entry.enteredAt / 1000) };

  return {
    statusCode: 202,
    body: JSON.stringify({
      receipt,
      poll: { url: `/events/${eventId}/status`, intervalMs: 3000, jitterMs: 1500 },
    }),
  };
};
