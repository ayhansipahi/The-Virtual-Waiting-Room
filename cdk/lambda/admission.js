// Admission API - POST /events/{eventId}/token (docs/03 §4-5)
// The hard capacity gate: one TransactWriteItems that flips the fan, bumps
// the active counter under its < capacity condition, and creates the session.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient, GetCommand, TransactWriteCommand,
} = require("@aws-sdk/lib-dynamodb");

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;
const SESSION_TTL_SEC = 600;

exports.handler = async (event) => {
  const eventId = event.pathParameters?.eventId ?? process.env.EVENT_ID;
  const fanId = event.requestContext?.authorizer?.fanId
    ?? JSON.parse(event.body ?? "{}").fanId;
  if (!fanId) return { statusCode: 401, body: "unauthenticated" };

  // Strongly consistent, direct DynamoDB - correctness path bypasses DAX.
  // TOCTOU is safe: servingPosition is monotonic, so a stale read is only
  // ever conservative (docs/03 §5).
  const [serving, fan] = await Promise.all([
    ddb.send(new GetCommand({
      TableName: TABLE, Key: { PK: `EVT#${eventId}`, SK: "STATE#SERVING" },
      ConsistentRead: true,
    })),
    ddb.send(new GetCommand({
      TableName: TABLE, Key: { PK: `FAN#${fanId}`, SK: `EVT#${eventId}` },
      ConsistentRead: true,
    })),
  ]);
  if (serving.Item?.eventState !== "OPEN")
    return { statusCode: 409, body: JSON.stringify({ eventState: serving.Item?.eventState }) };
  if (!fan.Item) return { statusCode: 404, body: "not in queue" };
  if (fan.Item.status === "ADMITTED") return existingToken(eventId, fan.Item); // idempotent retry

  const now = Date.now();
  const sessionId = `${fanId}@${now}`;
  try {
    await ddb.send(new TransactWriteCommand({
      TransactItems: [
        {
          Update: {
            TableName: TABLE,
            Key: { PK: `FAN#${fanId}`, SK: `EVT#${eventId}` },
            UpdateExpression: "SET #s = :adm, admittedAt = :now, sessionId = :sid",
            ConditionExpression: "#s = :queued AND queuePosition <= :serving",
            ExpressionAttributeNames: { "#s": "status" },
            ExpressionAttributeValues: {
              ":adm": "ADMITTED", ":queued": "QUEUED",
              ":serving": serving.Item?.servingPosition ?? 0, ":now": now, ":sid": sessionId,
            },
          },
        },
        {
          Update: {
            TableName: TABLE,
            Key: { PK: `EVT#${eventId}`, SK: "CTR#ACTIVE" },
            UpdateExpression: "ADD activeCount :one",
            // THE hard cap. "capacity" is a DynamoDB reserved word - alias it.
            ConditionExpression: "activeCount < #cap",
            ExpressionAttributeNames: { "#cap": "capacity" },
            ExpressionAttributeValues: { ":one": 1 },
          },
        },
        {
          Put: {
            TableName: TABLE,
            Item: {
              PK: `EVT#${eventId}`, SK: `SESSION#${fanId}`, type: "Session",
              status: "ACTIVE", sessionId, admittedAt: now,
              deadline: now + SESSION_TTL_SEC * 1000,       // sweeper authority
              ttl: Math.floor(now / 1000) + SESSION_TTL_SEC, // TTL cleanup (lags)
            },
            ConditionExpression: "attribute_not_exists(PK)",
          },
        },
      ],
    }));
  } catch (err) {
    if (err.name === "TransactionCanceledException") {
      // Lost a race with our own retry? Idempotent success if already admitted.
      const again = await ddb.send(new GetCommand({
        TableName: TABLE, Key: { PK: `FAN#${fanId}`, SK: `EVT#${eventId}` },
      }));
      if (again.Item?.status === "ADMITTED") return existingToken(eventId, again.Item);
      // Capacity full or not yet eligible: fan stays QUEUED and retries.
      return { statusCode: 429, headers: { "Retry-After": "5" }, body: "capacity full, retry" };
    }
    throw err;
  }

  return {
    statusCode: 200,
    body: JSON.stringify({
      token: sign(fanId, eventId, fan.Item.queuePosition, sessionId, now),
    }),
  };
};

function existingToken(eventId, fanItem) {
  return {
    statusCode: 200,
    body: JSON.stringify({
      token: sign(fanItem.PK.slice(4), eventId, fanItem.queuePosition,
        fanItem.sessionId, fanItem.admittedAt),
    }),
  };
}

// ES256 JWT with the event-scoped in-memory key (KMS-wrapped at rest; never a
// KMS call per request - docs/02 §7). Stubbed claims-only here.
function sign(fanId, eventId, queuePosition, sessionId, admittedAt) {
  return {
    sub: fanId, evt: eventId, pos: queuePosition,
    iat: Math.floor(admittedAt / 1000),
    exp: Math.floor(admittedAt / 1000) + SESSION_TTL_SEC,
    jti: sessionId,
  };
}
