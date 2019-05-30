import {DeleteItemInput} from 'aws-sdk/clients/dynamodb';

export interface IDeleteItemOptions {
    expected?: any;
    deleteItemRequest?: DeleteItemInput;
}
