// Sweeper - TTL-lag insurance (docs/03 §6.2, path 3)
// DynamoDB TTL deletes minutes late; slots must reclaim in seconds. Every
// 60 s, scan the BOUNDED active-session partition (<= ~capacity items) and
// close anything past its wall-clock deadline via the same idempotent flip.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient, QueryCommand, UpdateCommand,
} = require("@aws-sdk/lib-dynamodb");

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;
const EVENT_ID = process.env.EVENT_ID;

exports.handler = async () => {
  const now = Date.now();
  let closed = 0;
  let cursor;
  do {
    const page = await ddb.send(new QueryCommand({
      TableName: TABLE,
      KeyConditionExpression: "PK = :pk AND begins_with(SK, :s)",
      FilterExpression: "#st = :active AND deadline < :now",
      ExpressionAttributeNames: { "#st": "status" },
      ExpressionAttributeValues: {
        ":pk": `EVT#${EVENT_ID}`, ":s": "SESSION#", ":active": "ACTIVE", ":now": now,
      },
      ExclusiveStartKey: cursor,
    }));
    cursor = page.LastEvaluatedKey;

    for (const session of page.Items ?? []) {
      const fanId = session.SK.split("#")[1];
      try {
        // Same flip as the stream handler - the two paths racing is safe.
        await ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `FAN#${fanId}`, SK: `EVT#${EVENT_ID}` },
          UpdateExpression: "SET #s = :expired",
          ConditionExpression: "#s = :admitted",
          ExpressionAttributeNames: { "#s": "status" },
          ExpressionAttributeValues: { ":expired": "EXPIRED", ":admitted": "ADMITTED" },
        }));
      } catch (err) {
        if (err.name === "ConditionalCheckFailedException") continue; // lost the race - fine
        throw err;
      }
      await Promise.all([
        ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `EVT#${EVENT_ID}`, SK: "CTR#ACTIVE" },
          UpdateExpression: "ADD activeCount :minus",
          ExpressionAttributeValues: { ":minus": -1 },
        })),
        ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `EVT#${EVENT_ID}`, SK: session.SK },
          UpdateExpression: "SET #s = :closed",
          ExpressionAttributeNames: { "#s": "status" },
          ExpressionAttributeValues: { ":closed": "CLOSED" },
        })),
      ]);
      closed++;
    }
  } while (cursor);
  return { closed };
};
