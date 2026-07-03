#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { WaitingRoomStack } from "../lib/waiting-room-stack";

const app = new cdk.App();
new WaitingRoomStack(app, "WaitingRoomStack", {
  description:
    "Virtual Waiting Room - DynamoDB Football Data Modeling Challenge #1 (table, sparse GSI, DAX, control-plane Lambdas)",
});
