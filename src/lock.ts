import * as core from '@actions/core';
import { DeleteItemCommand, DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const tableName = 'LocksTable';
const client = new DynamoDBClient({ region: 'us-east-1' });
const lockID = 'pulumi-global-lock';
const defaultTTL = 1800;

export async function acquireGlobalLock(owner: string): Promise<void> {
    for (; ;) {
        try {
            const now = Math.floor(Date.now() / 1000);
            const ttl = now + defaultTTL;
            await client.send(new PutItemCommand({
                TableName: tableName,
                Item: {
                    "LockID": { S: lockID },
                    "LockOwner": { S: owner },
                    "ExpireTime": { N: ttl.toString() }
                },
                ConditionExpression: "attribute_not_exists(LockID) OR ExpireTime < :now",
                ExpressionAttributeValues: { ":now": { N: now.toString() } }
            }));
            core.info(`pulumi global lock acquired: ${owner}`);
            return
        } catch (e) {
            core.info(`waiting for pulumi global lock: ${owner}: ${e}`);
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}

export async function releaseGlobalLock(owner: string): Promise<void> {
    try {
        await client.send(new DeleteItemCommand({
            TableName: tableName,
            Key: {
                "LockID": { S: lockID },
            },
            ConditionExpression: "LockOwner = :owner",
            ExpressionAttributeValues: { ":owner": { S: owner } }
        }))
        core.info('pulumi global lock released');
    } catch (e) {
        if (e.name === 'ConditionalCheckFailedException') {
            core.info(`pulumi global lock has already been released, no action needed`);
            return;
        }
        core.info(`failed to release pulumi global lock: ${e}`);
    }
}
