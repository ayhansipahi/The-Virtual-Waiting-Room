// Promoter / Refiller - O(1) batch promotion (docs/03)
// One guarded ADD on one item promotes an entire batch. Deficit-driven:
// batch size = measured free capacity x overshoot.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient, GetCommand, UpdateCommand, PutCommand,
} = require("@aws-sdk/lib-dynamodb");

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;
const EVENT_ID = process.env.EVENT_ID;
const OVERSHOOT = 1.2;

const evtKey = (sk) => ({ PK: `EVT#${EVENT_ID}`, SK: sk });

exports.handler = async () => {
  // Control-loop reads are strongly consistent and NEVER via DAX (docs/03 §2).
  const [serving, active] = await Promise.all([
    ddb.send(new GetCommand({ TableName: TABLE, Key: evtKey("STATE#SERVING"), ConsistentRead: true })),
    ddb.send(new GetCommand({ TableName: TABLE, Key: evtKey("CTR#ACTIVE"), ConsistentRead: true })),
  ]);
  const s = serving.Item ?? {};
  if (s.eventState !== "OPEN") return { continue: s.eventState !== "CLOSED" }; // kill switch

  const capacityTarget = active.Item?.capacity ?? 1000;
  const deficit = capacityTarget - (active.Item?.activeCount ?? 0);
  const current = s.servingPosition ?? 0;
  const k = Math.min(Math.ceil(Math.max(0, deficit) * OVERSHOOT),
    (s.positionsAssigned ?? 0) - current); // never promote positions that don't exist
  if (k <= 0) return { continue: true, promoted: 0 };

  try {
    // Guarded on the value just read: a duplicated tick moves the threshold
    // at most once per read (docs/03 §3).
    await ddb.send(new UpdateCommand({
      TableName: TABLE,
      Key: evtKey("STATE#SERVING"),
      UpdateExpression: "SET servingPosition = :new, rateEmaPerSec = :ema, updatedAt = :now",
      ConditionExpression: "servingPosition = :read AND :new <= positionsAssigned",
      ExpressionAttributeValues: {
        ":new": current + k, ":read": current,
        ":ema": s.rateEmaPerSec ?? 0, // EMA update from ActiveCounter deltas; abbreviated
        ":now": Date.now(),
      },
    }));
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") return { continue: true, promoted: 0 };
    throw err;
  }

  await ddb.send(new PutCommand({ // audit trail
    TableName: TABLE,
    Item: {
      ...evtKey(`BATCH#P-${String(Date.now())}`),
      type: "Batch", fromPosition: current + 1, toPosition: current + k,
      size: k, promotedAt: Date.now(),
    },
  }));
  return { continue: true, promoted: k };
};
