import {DynamoDBIndexTypes} from '../../database/types/dynamodbIndexTypes';
import {Projection} from 'aws-sdk/clients/dynamodb';

export interface IIndex {
    hashKeyName:    string;
    rangeKeyName?:  string;
    name:           string;
    type:           DynamoDBIndexTypes;
    projection?:    Projection;
    readCapacity?:  number;
    writeCapacity?: number;
}
