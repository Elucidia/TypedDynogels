import {UpdateItemInput} from 'aws-sdk/clients/dynamodb';

export interface IUpdateItemOptions {
    expected?: any;
    updateItemRequest?: UpdateItemInput;
}
