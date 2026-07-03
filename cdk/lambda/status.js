// Status API - GET /events/{eventId}/status (docs/04)
// Two reads, both via DAX (eventually consistent, item TTL 1 s). The
// ServingState singleton is the read DAX exists for; fan items are uniform
// and pass-through-tolerant.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand } = require("@aws-sdk/lib-dynamodb");

// Production: const dax = new AmazonDaxClient({ endpoints: [process.env.DAX_ENDPOINT] })
// (amazon-dax-client-sdkv3, bundled with the function). Skeleton uses the
// plain client so the stub stays dependency-free; the call shape is identical.
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE = process.env.TABLE_NAME;

exports.handler = async (event) => {
  const eventId = event.pathParameters?.eventId ?? process.env.EVENT_ID;
  const fanId = event.requestContext?.authorizer?.fanId
    ?? event.queryStringParameters?.fanId;
  if (!fanId) return { statusCode: 401, body: "unauthenticated" };

  const [fan, serving] = await Promise.all([
    ddb.send(new GetCommand({
      TableName: TABLE, Key: { PK: `FAN#${fanId}`, SK: `EVT#${eventId}` },
    })),
    ddb.send(new GetCommand({
      TableName: TABLE, Key: { PK: `EVT#${eventId}`, SK: "STATE#SERVING" },
    })),
  ]);
  if (!fan.Item) return { statusCode: 404, body: "not in queue" };

  const s = serving.Item ?? {};
  const pos = fan.Item.queuePosition;
  const body = pos === undefined
    ? { state: "SECURING", message: "securing your place in line", eventState: s.eventState }
    : {
        position: pos,
        peopleAhead: Math.max(0, pos - (s.servingPosition ?? 0)),
        // Client-side: etaSeconds = peopleAhead / rateEmaPerSec (clamped/smoothed)
        rateEmaPerSec: s.rateEmaPerSec ?? 0,
        eligible: pos <= (s.servingPosition ?? 0), // derived, never stored (docs/03 §1)
        status: fan.Item.status,
        eventState: s.eventState,
        asOf: s.updatedAt,
      };

  return {
    statusCode: 200,
    headers: { "Cache-Control": "max-age=1" }, // CDN-ready without redesign
    body: JSON.stringify(body),
  };
};
