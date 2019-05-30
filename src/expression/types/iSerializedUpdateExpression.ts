import {ExpressionAttributeValueMap, ExpressionAttributeNameMap} from 'aws-sdk/clients/dynamodb';

export interface ISerializedUpdateExpression {
    expressions: Object;
    attributeNames: ExpressionAttributeNameMap;
    values: ExpressionAttributeValueMap;
}
