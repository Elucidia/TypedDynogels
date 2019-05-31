import {PutItemInput} from 'aws-sdk/clients/iot';

export interface IPutItemOptions {
    expected?: any;
    overwrite?: boolean;
    putItemRequest?: PutItemInput;
}
