// Stream handler - TTL-expiry close path (docs/03 §6.2, path 2)
// Event source is filtered to userIdentity = dynamodb.amazonaws.com, i.e.
// only TTL deletions arrive here. At-least-once delivery is safe because the
// close funnels through the conditional FanEntry flip (docs/03 §6.1).
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, UpdateCommand } = require("@aws-sdk/lib-dynamodb");

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;

exports.handler = async (event) => {
  for (const record of event.Records ?? []) {
    const old = record.dynamodb?.OldImage;
    if (record.eventName !== "REMOVE" || old?.type?.S !== "Session") continue;

    const [, eventId] = old.PK.S.split("#");
    const [, fanId] = old.SK.S.split("#");
    await closeSession(eventId, fanId, "EXPIRED");
  }
};

async function closeSession(eventId, fanId, terminal) {
  try {
    // The flip: exactly-once per fan regardless of redeliveries.
    await ddb.send(new UpdateCommand({
      TableName: TABLE,
      Key: { PK: `FAN#${fanId}`, SK: `EVT#${eventId}` },
      UpdateExpression: "SET #s = :t",
      ConditionExpression: "#s = :admitted",
      ExpressionAttributeNames: { "#s": "status" },
      ExpressionAttributeValues: { ":t": terminal, ":admitted": "ADMITTED" },
    }));
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") return; // already closed elsewhere
    throw err;
  }
  // Decrement only after winning the flip - can never double-count.
  await ddb.send(new UpdateCommand({
    TableName: TABLE,
    Key: { PK: `EVT#${eventId}`, SK: "CTR#ACTIVE" },
    UpdateExpression: "ADD activeCount :minus",
    ExpressionAttributeValues: { ":minus": -1 },
  }));
}

exports.closeSession = closeSession; // shared by sweeper/purchase-complete paths
