import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as dax from "aws-cdk-lib/aws-dax";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kms from "aws-cdk-lib/aws-kms";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as eventsources from "aws-cdk-lib/aws-lambda-event-sources";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as apigw from "aws-cdk-lib/aws-apigateway";
import * as path from "path";

// Design-point parameters (docs/01, docs/06). The warm-throughput values used
// here are deploy-friendly demo numbers; the real event needs the design-point
// values below plus a service-quota increase arranged with AWS beforehand.
const DESIGN_POINT = {
  tableWarmWritesPerSec: 1_200_000, // 1.2 x 1M/s entry burst
  gsiWarmWritesPerSec: 1_200_000,
  tableWarmReadsPerSec: 500_000,
};
const DEMO = {
  tableWarmWritesPerSec: 12_000,
  gsiWarmWritesPerSec: 12_000,
  tableWarmReadsPerSec: 12_000,
};

export class WaitingRoomStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const eventId = "CUPFINAL2026";

    // ------------------------------------------------------------------
    // Table: single-table model, on-demand + warm throughput, TTL, Streams
    // ------------------------------------------------------------------
    const table = new dynamodb.Table(this, "WaitingRoom", {
      tableName: "WaitingRoom",
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "ttl",
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // challenge sandbox; RETAIN in production
    });

    // Sparse sequencing index: only PENDING FanEntry items carry gsi1pk/gsi1sk;
    // the sequencer REMOVEs them at write-back, so the index self-cleans.
    table.addGlobalSecondaryIndex({
      indexName: "SequencingIndex",
      partitionKey: { name: "gsi1pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "gsi1sk", type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.KEYS_ONLY,
    });

    // Warm throughput (docs/01 §4): on-demand alone adapts to ~2x previous
    // peak; a scheduled stampede must be pre-warmed on table AND GSI.
    const cfnTable = table.node.defaultChild as dynamodb.CfnTable;
    cfnTable.addPropertyOverride("WarmThroughput", {
      ReadUnitsPerSecond: DEMO.tableWarmReadsPerSec,
      WriteUnitsPerSecond: DEMO.tableWarmWritesPerSec,
    });
    cfnTable.addPropertyOverride("GlobalSecondaryIndexes.0.WarmThroughput", {
      ReadUnitsPerSecond: DEMO.tableWarmReadsPerSec,
      WriteUnitsPerSecond: DEMO.gsiWarmWritesPerSec,
    });

    // ------------------------------------------------------------------
    // DAX: read path only (docs/04). Item-cache TTL 1000 ms via parameter
    // group; TTL is cluster-wide (docs/06 §4).
    // ------------------------------------------------------------------
    const vpc = new ec2.Vpc(this, "Vpc", {
      maxAzs: 3,
      natGateways: 0,
      subnetConfiguration: [
        { name: "isolated", subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      ],
    });
    vpc.addGatewayEndpoint("DynamoEndpoint", {
      service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
    });

    const daxSg = new ec2.SecurityGroup(this, "DaxSg", { vpc, allowAllOutbound: true });
    daxSg.addIngressRule(ec2.Peer.ipv4(vpc.vpcCidrBlock), ec2.Port.tcp(8111), "DAX from VPC");

    const daxRole = new iam.Role(this, "DaxRole", {
      assumedBy: new iam.ServicePrincipal("dax.amazonaws.com"),
    });
    table.grantReadData(daxRole);

    const daxParams = new dax.CfnParameterGroup(this, "DaxParams", {
      parameterGroupName: "waiting-room-ttl-1s",
      description: "Item/query cache TTL 1000 ms - bounds ServingState staleness to 1 s",
      parameterNameValues: {
        "record-ttl-millis": "1000",
        "query-ttl-millis": "1000",
      },
    });

    const daxSubnets = new dax.CfnSubnetGroup(this, "DaxSubnets", {
      subnetGroupName: "waiting-room-dax",
      subnetIds: vpc.isolatedSubnets.map((s) => s.subnetId),
    });

    const daxCluster = new dax.CfnCluster(this, "DaxCluster", {
      clusterName: "waiting-room",
      iamRoleArn: daxRole.roleArn,
      nodeType: "dax.r5.large", // demo size; docs/06 sizes ~10x r5.2xlarge for 667k ops/s
      replicationFactor: 3,
      parameterGroupName: daxParams.parameterGroupName,
      subnetGroupName: daxSubnets.subnetGroupName,
      securityGroupIds: [daxSg.securityGroupId],
    });
    daxCluster.addDependency(daxParams);
    daxCluster.addDependency(daxSubnets);

    // ------------------------------------------------------------------
    // KMS: one event-scoped signing keypair, generated once (docs/02 §7).
    // The signing Lambdas call GenerateDataKeyPair at cold start, never per
    // request.
    // ------------------------------------------------------------------
    const signingKey = new kms.Key(this, "TokenSigningKey", {
      description: "Wraps the event-scoped ES256 keypair for position/admission JWTs",
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ------------------------------------------------------------------
    // Lambdas (skeletons with correct wiring + IAM; logic in ../lambda)
    // ------------------------------------------------------------------
    const code = lambda.Code.fromAsset(path.join(__dirname, "..", "lambda"));
    const commonEnv = {
      TABLE_NAME: table.tableName,
      EVENT_ID: eventId,
      DAX_ENDPOINT: daxCluster.attrClusterDiscoveryEndpointUrl,
    };
    const fn = (name: string, handler: string, extra?: Partial<lambda.FunctionProps>) =>
      new lambda.Function(this, name, {
        runtime: lambda.Runtime.NODEJS_22_X,
        code,
        handler,
        memorySize: 512,
        timeout: cdk.Duration.seconds(30),
        environment: commonEnv,
        ...extra,
      });

    const entryFn = fn("EntryFn", "entry.handler");
    const sequencerFn = fn("SequencerFn", "sequencer.handler", {
      timeout: cdk.Duration.minutes(1),
      memorySize: 1024,
    });
    const promoterFn = fn("PromoterFn", "promoter.handler");
    // Status is the only DAX consumer -> VPC-attached, reaches DynamoDB via
    // the gateway endpoint on cache misses.
    const statusFn = fn("StatusFn", "status.handler", {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });
    const admissionFn = fn("AdmissionFn", "admission.handler");
    const streamFn = fn("StreamHandlerFn", "stream-handler.handler");
    const sweeperFn = fn("SweeperFn", "sweeper.handler");

    // IAM: least privilege per component
    table.grantWriteData(entryFn);
    table.grantReadData(entryFn); // read-back on ConditionalCheckFailed
    table.grantReadWriteData(sequencerFn); // GSI query + counter ADD + write-back + lease
    table.grantReadWriteData(promoterFn);
    table.grantReadData(statusFn);
    statusFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dax:GetItem", "dax:BatchGetItem", "dax:Query"],
        resources: [
          `arn:aws:dax:${this.region}:${this.account}:cache/${daxCluster.clusterName}`,
        ],
      }),
    );
    table.grantReadWriteData(admissionFn); // TransactWriteItems (flip + counter + session)
    table.grantReadWriteData(streamFn); // conditional flip + decrement
    table.grantReadWriteData(sweeperFn);
    signingKey.grant(entryFn, "kms:GenerateDataKeyPair", "kms:Decrypt");
    signingKey.grant(admissionFn, "kms:GenerateDataKeyPair", "kms:Decrypt");

    // Streams: TTL expirations only (userIdentity is the DynamoDB service)
    streamFn.addEventSource(
      new eventsources.DynamoEventSource(table, {
        startingPosition: lambda.StartingPosition.LATEST,
        batchSize: 100,
        retryAttempts: 5,
        filters: [
          lambda.FilterCriteria.filter({
            userIdentity: {
              type: lambda.FilterRule.isEqual("Service"),
              principalId: lambda.FilterRule.isEqual("dynamodb.amazonaws.com"),
            },
          }),
        ],
      }),
    );

    // ------------------------------------------------------------------
    // Tick loops. EventBridge Scheduler cannot tick sub-minute, so the
    // 1 s sequencer and 10 s promoter run as Step Functions Wait-loops;
    // handlers return { continue: bool } and the loop exits when the event
    // closes. Standard-workflow history (25k events) bounds one execution
    // to a few hours; ops (or a rate(1 hour) rule) restarts it.
    // ------------------------------------------------------------------
    const loop = (name: string, fnToTick: lambda.Function, waitSec: number) => {
      const wait = new sfn.Wait(this, `${name}Wait`, {
        time: sfn.WaitTime.duration(cdk.Duration.seconds(waitSec)),
      });
      const invoke = new tasks.LambdaInvoke(this, `${name}Invoke`, {
        lambdaFunction: fnToTick,
        payloadResponseOnly: true,
      });
      const done = new sfn.Succeed(this, `${name}Done`);
      const decide = new sfn.Choice(this, `${name}Continue?`)
        .when(sfn.Condition.booleanEquals("$.continue", true), wait)
        .otherwise(done);
      return new sfn.StateMachine(this, `${name}Loop`, {
        definitionBody: sfn.DefinitionBody.fromChainable(wait.next(invoke).next(decide)),
      });
    };
    const sequencerLoop = loop("Sequencer", sequencerFn, 1);
    const promoterLoop = loop("Promoter", promoterFn, 10);

    // Sweeper: 60 s granularity, EventBridge is fine at >= 1 minute
    new events.Rule(this, "SweeperRule", {
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      targets: [new targets.LambdaFunction(sweeperFn)],
    });

    // ------------------------------------------------------------------
    // API (edge assumptions - CloudFront/WAF - documented in docs/01 §5.2)
    // ------------------------------------------------------------------
    const api = new apigw.RestApi(this, "WaitingRoomApi", {
      restApiName: "waiting-room",
      deployOptions: { stageName: "v1", throttlingRateLimit: 10000, throttlingBurstLimit: 5000 },
    });
    const eventRes = api.root.addResource("events").addResource("{eventId}");
    eventRes.addResource("enter").addMethod("POST", new apigw.LambdaIntegration(entryFn));
    eventRes.addResource("status").addMethod("GET", new apigw.LambdaIntegration(statusFn));
    eventRes.addResource("token").addMethod("POST", new apigw.LambdaIntegration(admissionFn));

    new cdk.CfnOutput(this, "TableName", { value: table.tableName });
    new cdk.CfnOutput(this, "DaxEndpoint", { value: daxCluster.attrClusterDiscoveryEndpointUrl });
    new cdk.CfnOutput(this, "SequencerLoopArn", { value: sequencerLoop.stateMachineArn });
    new cdk.CfnOutput(this, "PromoterLoopArn", { value: promoterLoop.stateMachineArn });
    new cdk.CfnOutput(this, "DesignPointNote", {
      value: `Demo warm throughput deployed; design point requires ${DESIGN_POINT.tableWarmWritesPerSec}/s table + ${DESIGN_POINT.gsiWarmWritesPerSec}/s GSI (quota increase)`,
    });
  }
}
