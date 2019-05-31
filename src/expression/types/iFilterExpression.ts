import {ExpressionAttributeNameMap, ExpressionAttributeValueMap} from 'aws-sdk/clients/dynamodb';

export interface IFilterExpression {
    attributeNames: ExpressionAttributeNameMap;
    statement: string;
    attributeValues: ExpressionAttributeValueMap;
}
