import {StreamSpecification, SSESpecification} from 'aws-sdk/clients/dynamodb';
import {BillingModes} from './billingModes';

export interface IMakeTableOptions {
    readCapacity?: number;
    writeCapacity?: number;
    streamSpecification?: StreamSpecification;
    billingMode?: BillingModes;
    sseSpecification?: SSESpecification;
    tags?: {Key: string, Value: string}[];
}
