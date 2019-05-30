import {IIndex} from './iIndex';
import {SchemaMap} from '@hapi/joi';
import {IValidationOptions} from './iValidationOptions';

export interface ISchemaConfiguration {
    hashKeyName:            string;
    rangeKeyName?:          string;
    tableName?:             string;
    schema?:                SchemaMap;
    indexes?:               IIndex[];
    timestamps?:            boolean;
    createdAt?:             string | boolean;
    updatedAt?:             string | boolean;
    validation?:            IValidationOptions;
}
