import * as core from '@actions/core';
import { DeleteItemCommand, DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const client = new DynamoDBClient({ region: 'us-east-1' });
const defaultTableName = 'LocksTable';
const defaultTTL = 1800;
const lockID = 'pulumi-global-lock';

interface LockOptions {
    ttl?: number;
    tableName?: string;
}

export async function acquireGlobalLock(owner: string, options: LockOptions): Promise<void> {
    const tableName = options.tableName ? options.tableName : defaultTableName;
    const ttl = options.ttl ? options.ttl : defaultTTL;
    for (; ;) {
        try {
            const now = Math.floor(Date.now() / 1000);
            const expireTime = now + ttl;
            await client.send(new PutItemCommand({
                TableName: tableName,
                Item: {
                    "LockID": { S: lockID },
                    "LockOwner": { S: owner },
                    "ExpireTime": { N: expireTime.toString() }
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

export async function releaseGlobalLock(owner: string, options: LockOptions): Promise<void> {
    const tableName = options.tableName ? options.tableName : defaultTableName;
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
