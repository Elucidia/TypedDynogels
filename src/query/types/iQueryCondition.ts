import {ExpressionAttributeNameMap, ExpressionAttributeValueMap} from 'aws-sdk/clients/dynamodb';

export interface IQueryCondition {
    attributeNames: ExpressionAttributeNameMap;
    attributeValues: ExpressionAttributeValueMap;
    statement: any;
}
