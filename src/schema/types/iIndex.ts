import {DynamoDBIndexTypes} from '../../database/types/dynamodbIndexTypes';

export interface IIndex {
    hashKeyName:    string;
    rangeKeyName?:  string;
    name:           string;
    type:           DynamoDBIndexTypes;
    projection?:    Object;
    readCapacity?:  number;
    writeCapacity?: number;
}
