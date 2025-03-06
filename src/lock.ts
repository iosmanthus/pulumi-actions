import * as core from '@actions/core';
import {
  DeleteItemCommand,
  DynamoDBClient,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import { Config } from './config';

const tableName = 'LocksTable';
const client = new DynamoDBClient({ region: 'us-east-1' });
const lockID = 'pulumi-global-lock';
const defaultTTL = 1800;

export async function acquireGlobalLock(owner: string): Promise<void> {
  for (;;) {
    try {
      const now = Math.floor(Date.now() / 1000);
      const ttl = now + defaultTTL;
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            LockID: { S: lockID },
            Owner: { S: owner },
            ExpireTime: { N: ttl.toString() },
          },
          ConditionExpression:
            'attribute_not_exists(LockID) OR ExpireTime < :now',
          ExpressionAttributeValues: { ':now': { N: now.toString() } },
        }),
      );
      core.debug(`pulumi global lock acquired: ${owner}`);
      return;
    } catch (e) {
      core.debug(`waiting for pulumi global lock: ${owner}: ${e}`);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

export async function releaseGlobalLock(): Promise<void> {
  try {
    await client.send(
      new DeleteItemCommand({
        TableName: tableName,
        Key: {
          LockID: { S: lockID },
        },
      }),
    );
    core.debug('pulumi global lock released');
  } catch (e) {
    core.debug(`failed to release pulumi global lock: ${e}`);
  }
}

export class LockError extends Error {
  code: string;
}

export async function tryAcquireStackLock(config: Config) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const ttl = now + defaultTTL;
    const lockID = `${config.workDir}-${config.stackName}`;
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          LockID: { S: lockID },
          ExpireTime: { N: ttl.toString() },
        },
        ConditionExpression:
          'attribute_not_exists(LockID) OR ExpireTime < :now',
        ExpressionAttributeValues: { ':now': { N: now.toString() } },
      }),
    );
  } catch {
    const error = new Error('Failed to acquire stack lock') as LockError;
    error.code = 'LockAcquisitionError';
    throw error;
  }
}

export async function releaseStackLock(config: Config) {
  const lockID = `${config.workDir}-${config.stackName}`;
  await client.send(
    new DeleteItemCommand({
      TableName: tableName,
      Key: { LockID: { S: lockID } },
    }),
  );
}
