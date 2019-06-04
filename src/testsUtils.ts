import {DynamoDB} from 'aws-sdk';

export function tests_StartDynamoDB(): DynamoDB {
    return new DynamoDB({endpoint: 'http://localhost:8000', apiVersion: '2012-08-10', region: 'us-east-1'});
}

export function tests_RandomName(prefix: string): string {
    return `${prefix}_${Date.now()}.${Math.floor(Math.random() * 1000)}`;
}
