import * as _ from 'lodash';
import {validate, ValidationError, object, date, ValidationResult, boolean, SchemaMap, string, ValidationOptions} from '@hapi/joi';
import {Internals} from '../internals/internals';
import {Log} from '../util/log';
import {DynamoDBIndexTypes} from '../database/types/dynamodbIndexTypes';
import {ISchemaConfiguration} from './types/iSchemaConfiguration';
import {IValidationOptions} from './types/iValidationOptions';
import {IIndex} from './types/iIndex';

export class Schema {
    private hashKeyName: string;
    private rangeKeyName: string;
    private tableName: string | Function;
    private timestamps: boolean;
    private createdAt: string | boolean;
    private updatedAt: string | boolean;
    // OLD
    //private secondaryIndexes: _.Dictionary<IIndex> = {};
    //private globalIndexes: _.Dictionary<IIndex> = {};
    // NEW
    private localIndexes: Map<string, IIndex> = new Map<string, IIndex>();
    private globalIndexes: Map<string, IIndex> = new Map<string, IIndex>();
    private validationOptions: IValidationOptions;

    private modelSchema;
    private datatypes;

    public async initialize(schemaConfiguration: ISchemaConfiguration): Promise<any> {
        this.validationOptions = schemaConfiguration.validation;

        await validate(schemaConfiguration, Internals.CONFIGURATION_SCHEMA, {context: {hashKey: schemaConfiguration.hashKeyName}})
            .then((data: ISchemaConfiguration) => { //type is same as configuration parameter
                // TODO: optimiser cette shit
                this.hashKeyName = data.hashKeyName;
                this.rangeKeyName = data.rangeKeyName;
                this.tableName = data.tableName;
                this.timestamps = data.timestamps;
                this.createdAt = data.createdAt;
                this.updatedAt = data.updatedAt;

                if (data.indexes) {
                    // OLD: this.globalIndexes = _.chain(data.indexes).filter((o) => o.type === DynamoDBIndexTypes.GLOBAL).keyBy('name').value();
                    // OLD: this.secondaryIndexes = _.chain(data.indexes).filter((o) => o.type === DynamoDBIndexTypes.LOCAL).keyBy('name').value();
                    data.indexes.forEach((index: IIndex) => {
                        switch (index.type) {
                            case DynamoDBIndexTypes.GLOBAL: {
                                this.globalIndexes.set(index.name, index);
                                break;
                            }
                            case DynamoDBIndexTypes.LOCAL: {
                                this.localIndexes.set(index.name, index);
                                break;
                            }
                            default: {
                                Log.Error(Schema.name, 'initialize', 'Invalid index type', [{name: 'Given index type', value: index.type}]);
                                break;
                            }
                        }
                    });
                }

                if (data.schema) {
                    this.modelSchema = _.isPlainObject(data.schema) ? object().keys(data.schema) : data.schema;
                } else {
                    this.modelSchema = object();
                }

                if (this.timestamps) {
                    const valids = {};
                    let createdAtParamName = 'createdAt';
                    let updatedAtParamName = 'updatedAt';

                    if (this.createdAt && _.isString(this.createdAt)) {
                        createdAtParamName = this.createdAt;
                    }

                    if (this.updatedAt && _.isString(this.updatedAt)) {
                        updatedAtParamName = this.updatedAt;
                    }

                    if (this.createdAt !== false) {
                        valids[createdAtParamName] = date();
                    }

                    if (this.updatedAt !== false) {
                        valids[updatedAtParamName] = date();
                    }

                    const extended = this.modelSchema.keys(valids);
                    this.modelSchema = extended;
                }

                this.datatypes = Internals.ParseDynamoDBTypes(this.modelSchema.describe());
            })
            .catch((error: ValidationError) => {
                Log.Error(Schema.name, 'initialize', 'Invalid table schema, check your configuration', [{name: 'Error', value: error}]);
                return Promise.reject(error);
            });
    }

    public validate(value: any, options?: ValidationOptions): ValidationResult<any> {
        options = options || {};
        if (this.validationOptions) {
            _.extend(options, this.validationOptions);
        }

        return validate(value, this.modelSchema, options);
    }

    public applyDefaults(data: any) {
        return Internals.InvokeDefaultFunctions(this.validate(data, {abortEarly: false}).value);
    }

    public getHashKeyName(): string {
        return this.hashKeyName;
    }

    public getRangeKeyName(): string {
        return this.rangeKeyName;
    }

    public getTableName(): string | Function {
        return this.tableName;
    }

    public hasTimestamps(): boolean {
        return this.timestamps;
    }

    public getCreatedAt(): string | boolean {
        return this.createdAt;
    }

    public getUpdatedAt(): string | boolean {
        return this.updatedAt;
    }

    public getGlobalIndexes(): Map<string, IIndex> {
        return this.globalIndexes;
    }

    public getLocalIndexes(): Map<string, IIndex> {
        return this.localIndexes;
    }

    public getDatatypes(): any {
        return this.datatypes;
    }
}
