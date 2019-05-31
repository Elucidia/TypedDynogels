import * as _ from 'lodash';
import {DocumentClient, GetItemInput, GetItemOutput, PutItemInput, DeleteItemInput, ExpressionAttributeNameMap, ExpressionAttributeValueMap, QueryInput, UpdateItemInput, UpdateItemOutput, DeleteItemOutput} from 'aws-sdk/clients/dynamodb';
import {waterfall, map} from 'async';
import {Item} from './item';
import {ITableConfiguration} from './types/iTableConfiguration';
import {Schema} from '../schema/schema';
import {Internals} from '../internals/internals';
import {DynamoDB, AWSError} from 'aws-sdk';
import {Log} from '../util/log';
import {Serializer} from '../serializer/serializer';
import {EventEmitter} from 'events';
import {Utils} from '../util/utils';
import {TableEvents} from './types/tableEvents';
import {ValidationResult} from '@hapi/joi';
import {IPutItemOptions} from './types/iPutItemOptions';
import {ConditionExpressionOperators} from './types/conditionExpressionOperators';
import {Expressions} from '../expression/expressions';
import {IFilterExpression} from '../expression/types/iFilterExpression';
import {ISerializedUpdateExpression} from '../expression/types/iSerializedUpdateExpression';
import {UpdateReturnValuesModes} from './types/updateReturnValuesModes';
import {IUpdateItemOptions} from './types/iUpdateItemOptions';
import {DynamoDBRequestMethods} from './types/dynamodbRequestMethods';
import {IDeleteItemOptions} from './types/iDeleteItemOptions';
import {Query} from '../scanQuery/query';
import {Scan} from '../scanQuery/scan';

export class Table {
    private configuration: ITableConfiguration;
    private schema: Schema;
    private serializer: any;
    private documentClient: DocumentClient;
    private itemFactory: any = null;

    private beforeEvent: EventEmitter = new EventEmitter();
    private beforeBoundEvent;
    private afterEvent: EventEmitter = new EventEmitter();
    private afterBoundEvent;

    constructor(tableName: string, schema: Schema, serializer: any, documentClient: DocumentClient) {
        this.configuration = {name: tableName};
        this.schema = schema;
        this.serializer = serializer;
        this.documentClient = documentClient;

        this.beforeBoundEvent = this.beforeEvent.on.bind(this.beforeEvent);
        this.afterBoundEvent = this.afterEvent.on.bind(this.afterEvent);
    }

    // TODO rename
    public initializeItem(attributes): Item {
        if (this.itemFactory) {
            return new this.itemFactory(attributes);
        } else {
            return new Item(attributes, this);
        }
    }

    public getTableName(): string {
        const tableName: string | Function = this.schema.getTableName();
        if (tableName) {
            if (_.isFunction(tableName)) {
                return tableName.call(this);
            } else {
                return tableName as string;
            }
        } else {
            return this.configuration.name;
        }
    }

    public getSchema(): Schema {
        return this.schema;
    }

    public async sendRequest(method: DynamoDBRequestMethods, parameters: any): Promise<any | AWSError> {
        let driver: DynamoDB | DocumentClient;
        if (_.isFunction(Internals.GetDocumentClient()[method])) {
            driver = Internals.GetDocumentClient();
        } else if (_.isFunction(Internals.GetDynamoDBDriver()[method])) {
            driver = Internals.GetDynamoDBDriver();
        }

        const startTime: number = Date.now();

        Log.Info(Table.name, 'sendRequest', 'Sending request', [
            {name: 'Method', value: method.toUpperCase()},
            {name: 'Parameters', value: parameters}
        ]);

        await driver[method].call(driver, parameters)
            .then((data: any) => {
                Log.Success(Table.name, 'sendRequest', `Successfully called AWS method ${method.toUpperCase()}`, [{name: 'Duration', value: Date.now() - startTime}]);
                return Promise.resolve(data);
            })
            .catch((error: AWSError) => {
                Log.Error(Table.name, 'sendRequest', `An error occured while calling AWS method ${method.toUpperCase()}`, [{name: 'Error', value: error}]);
                return Promise.reject(error);
            });
    }

    public async getItem(hashKey: Object, rangeKey: Object = null, options: Object = {}): Promise<any | AWSError> {
        const dynamoDBKey: DynamoDB.Key = {hashKey, rangeKey};
        let parameters: GetItemInput = {
            TableName: this.getTableName(),
            Key: Serializer.BuildKey(this.schema, dynamoDBKey)
        };

        parameters = {...parameters, ...options};

        let item;
        await this.sendRequest(DynamoDBRequestMethods.GET, parameters)
            .then((success: GetItemOutput) => {
                let item = null;
                if (success.Item) {
                    item = this.initializeItem(Serializer.DeserializeItem(success.Item));
                }
            })
            .catch((error: AWSError) => {
                Log.Error(Table.name, 'getItem', 'Unable to get item');
                return Promise.reject(error);
            });

        return Promise.resolve(item);
    }
    // TODO: bien tester, pas sur du resultat
    public async putItem(item: Item, options?: IPutItemOptions): Promise<any> {
        options = options || {};
        let itemPut;
        if (Array.isArray(item)) {
            await map(item, async (data, callback) => {
                await Table.NotifyBeforeEventListeners(this, TableEvents.CREATE, Table.PrepareItem) // TODO: vider le .then et faire comme dans updateItem()
                    .then(async (result: Object) => {
                        const validatedSchema: ValidationResult<any> = (await this.schema.validate(result));
                        if (validatedSchema.error) {
                            validatedSchema.error.message = `${validatedSchema.error.message} on ${this.getTableName()}`;
                            return Promise.reject(validatedSchema.error);
                        }

                        const nullOmittedAttributes = Utils.OmitNulls(result);
                        let itemParameters = {
                            TableName: this.getTableName(),
                            Item: Serializer.SerializeItem(this.schema, nullOmittedAttributes)
                        };

                        if (options.expected) {
                            Table.AddConditionExpressionToRequest(itemParameters, options.expected);
                            delete options.expected;
                        }

                        if (!options.overwrite) {
                            const expected = _.chain([this.schema.getHashKeyName(), this.schema.getRangeKeyName()]).compact().reduce((accumulator: any, key: string) => {
                                _.set(accumulator, `${key}.<>`, _.get(itemParameters.Item, key));
                                return accumulator;
                            }, {}).value();

                            Table.AddConditionExpressionToRequest(itemParameters, expected);
                        }

                        delete options.overwrite;
                        itemParameters = {...itemParameters, ...options};

                        await this.sendRequest(DynamoDBRequestMethods.PUT, itemParameters)
                            .then(() => {
                                const item = this.initializeItem(nullOmittedAttributes);
                                this.afterEvent.emit(TableEvents.CREATE, item);
                                itemPut = item;
                            })
                            .catch((error) => {
                                Log.Error(Table.name, 'createItem', 'An error occurred while putting item in DynamoDB');
                            })
                    })
                    .catch((error: any) => {
                        Log.Error(Table.name, 'createItem', 'An error occurred while notifying listeners for "create" event', [{name: 'Error', value: error}]);
                        return Promise.reject(error);
                    });
            });
        }
        return Promise.resolve(item);
    }

    public async validateItemAttributes(item: any) {
        return this.schema.validate(item.attributes);
    }

    public async updateItem(item: any, options?: IUpdateItemOptions): Promise<UpdateItemOutput | any> {
        const schemaValidation = (await Table.ValidateItemFragment(this.schema, item));
        if (schemaValidation.error) {
            Log.Error(Table.name, 'updateItem', 'An error occurred on schema validation for item', [{name: 'Error', value: schemaValidation.error}, {name: 'Table name', value: this.getTableName()}]);
            return Promise.reject(schemaValidation.error);
        }

        let dataFromNotifyBeforeEventListeners: any;
        await Table.NotifyBeforeEventListeners(this, TableEvents.UPDATE, Table.PrepareItemUpdate)
            .then((success: any) => {
                dataFromNotifyBeforeEventListeners = success;
            })
            .catch((error: Error) => {
                Log.Error(Table.name, 'updateItem', 'An error occurred while notifying listeners for "update" event', [{name: 'Error', value: error}]);
                return Promise.reject(error);
            });

        const hashKey = dataFromNotifyBeforeEventListeners[this.schema.getHashKeyName()];
        let rangeKey = dataFromNotifyBeforeEventListeners[this.schema.getRangeKeyName()];

        if (_.isUndefined(rangeKey)) {
            rangeKey = null;
        }

        const dynamoDBKey = {hashKey, rangeKey};
        let updateParameters: UpdateItemInput = {
            TableName: this.getTableName(),
            Key: Serializer.BuildKey(this.schema, dynamoDBKey),
            ReturnValues: UpdateReturnValuesModes.ALL_ATTRIBUTES_AFTER_UPDATE
        };

        let updateExpression: {
            ExpressionAttributeValues: ExpressionAttributeValueMap;
            ExpressionAttributeNames: ExpressionAttributeNameMap;
            UpdateExpression: string;
        } = null;
        try {
            updateExpression = Table.UpdateExpressions(this.schema, dataFromNotifyBeforeEventListeners, options.updateItemRequest);
        } catch (error) {
            Log.Error(Table.name, 'updateItem', 'An error occurred while making update expression', [{name: 'Error', value: error}]);
            return Promise.reject(error);
        }

        // was: updateParameters = _.assign(updateParameters, updateExpression)
        updateParameters = {...updateParameters, ...updateExpression};

        if (options.expected) {
            Table.AddConditionExpressionToRequest(updateParameters, options.expected);
        }
        delete options.expected;

        const unprocessedOptions = _.omit(options.updateItemRequest, ['UpdateExpression', 'ExpressionAttributeValues', 'ExpressionAttributeNames']);
        // was: updateParameters = _.chain({}).merge(updateParameters, unprocessedOptions).omitBy(_.isEmpty).value
        updateParameters = Utils.RemoveEmptyAttributesFromObject({...updateParameters, ...unprocessedOptions});

        let resultToReturn: UpdateItemOutput;
        await this.sendRequest(DynamoDBRequestMethods.UPDATE, updateParameters)
            .then((success: UpdateItemOutput) => {
                let result = null;
                if (success.Attributes) {
                    result = this.initializeItem(Serializer.DeserializeItem(success.Attributes));
                }

                this.afterEvent.emit(TableEvents.UPDATE, result);
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'updateItem', 'An error occurred while updating item in DynamoDB');
                return Promise.reject(error);
            });

        return Promise.resolve(resultToReturn);
    }

    public async deleteItem(hashKey: Object, rangeKey: Object = null, options?: IDeleteItemOptions): Promise<DeleteItemOutput | any> {
        /* was:
            if (_.isPlainObject(hashKey)) {
                rangeKey = hashKey[self.schema.rangeKey];

                if (_.isUndefined(rangeKey)) {
                rangeKey = null;
                }

                hashKey = hashKey[self.schema.hashKey];
            }*/
        const dynamoDBKey: DynamoDB.Key = {hashKey, rangeKey};
        let deleteParameters: DeleteItemInput = {
            TableName: this.getTableName(),
            Key: Serializer.BuildKey(this.schema, dynamoDBKey)
        };

        if (options.expected) {
            Table.AddConditionExpressionToRequest(deleteParameters, options.expected);
            delete options.expected;
        }

        // was deleteParameters = _.merge({}, deleteParameters, options.deleteItemRequest)
        if (options.deleteItemRequest) {
            deleteParameters = {...deleteParameters, ...options.deleteItemRequest};
        }

        let resultToReturn;
        await this.sendRequest(DynamoDBRequestMethods.DELETE, deleteParameters)
            .then((success: DeleteItemOutput) => {
                let item = null;
                if (success.Attributes) {
                    item = this.initializeItem(Serializer.DeserializeItem(success.Attributes));
                }

                this.afterEvent.emit(TableEvents.DELETE, item);
                resultToReturn = item;
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'deleteItem', 'An error occurred while deleting item in DynamoDB');
                return Promise.reject(error);
            });

        return Promise.resolve(resultToReturn);
    }

    public getQueryRequestForTable(hashKey: Object): Query {
        return new Query(this, hashKey);
    }

    public getScanRequestForTable(): Scan {
        return new Scan(this);
    }

    // formally CallBeforeHooks()
    private static async NotifyBeforeEventListeners(table: Table, eventName: TableEvents, startFunc: Function): Promise<any> {
        const listeners: Function[] = table.beforeEvent.listeners(eventName);
        return new Promise((resolve, reject) => {
            waterfall([startFunc].concat(listeners), (error, result) => {
                if (error) {
                    reject(error);
                }
                resolve(result);
            });
        });
    }

    private static PrepareItem(table: Table, item: Item): Object {
        const defaultsAppliedItem: Object = table.schema.applyDefaults(item);
        const createdAt: string | boolean = table.schema.getCreatedAt();
        const parameterName: string = Utils.IsString(createdAt) ? createdAt as string : 'createdAt';
        //                                                          originally: _.has(data, parameterName)
        if (table.schema.hasTimestamps() && createdAt !== false && defaultsAppliedItem.hasOwnProperty(parameterName)) {
            defaultsAppliedItem[parameterName] = new Date().toISOString();
        }

        return defaultsAppliedItem;
    }

    private static AddConditionExpressionToRequest(request: PutItemInput | DeleteItemInput | UpdateItemInput, expectedConditions) {
        _.each(expectedConditions, (value, key) => {
            let operator: ConditionExpressionOperators;
            let expectedValue = null;

            const existingValueKeys: string[] = _.keys(request.ExpressionAttributeValues);

            if (Utils.IsObject(value) && Utils.IsBoolean(value.Exists)) {
                if (value.Exists) {
                    operator = ConditionExpressionOperators.ATTRIBUTE_EXISTS;
                } else if (!value.Exists) {
                    operator = ConditionExpressionOperators.ATTRIBUTE_NOT_EXISTS;
                }
            } else if (Utils.IsObject(value) && value.hasOwnProperty('<>')) {
                operator = ConditionExpressionOperators.NOT_EQUALS;
                expectedValue = value['<>'];
            } else {
                operator = ConditionExpressionOperators.EQUALS;
                expectedValue = value;
            }

            const condition: IFilterExpression = Expressions.BuildFilterExpression(key, operator, existingValueKeys, expectedValue);
            request.ExpressionAttributeNames = {...condition.attributeNames, ...request.ExpressionAttributeNames} as ExpressionAttributeNameMap;
            request.ExpressionAttributeValues = {...condition.attributeValues, ...request.ExpressionAttributeValues} as ExpressionAttributeValueMap;

            if (Utils.IsString(request.ConditionExpression)) {
                request.ConditionExpression = `${request.ConditionExpression} AND (${condition.statement})`;
            } else {
                request.ConditionExpression = `${condition.statement}`;
            }
        });
    }

    private static UpdateExpressions(schema: Schema, item: Item, updateItemRequest: UpdateItemInput): {ExpressionAttributeValues: ExpressionAttributeValueMap, ExpressionAttributeNames: ExpressionAttributeNameMap, UpdateExpression: string} {
        const serializedExpression: ISerializedUpdateExpression = Expressions.SerializeUpdateExpression(schema, item);

        if (updateItemRequest) {
            if (updateItemRequest.UpdateExpression) {
                const parsed = Expressions.Parse(updateItemRequest.UpdateExpression);

                serializedExpression.expressions = _.reduce(parsed, (accumulator: any, currentValue: any, key: string) => {
                    if (!_.isEmpty(currentValue)) {
                        accumulator[key] = accumulator[key].concat(currentValue);
                    }
                    return accumulator;
                }, serializedExpression.expressions);
            }

            if (_.isPlainObject(updateItemRequest.ExpressionAttributeValues)) {
                serializedExpression.values = {...serializedExpression.values, ...updateItemRequest.ExpressionAttributeValues};
            }

            if (_.isPlainObject(updateItemRequest.ExpressionAttributeNames)) {
                serializedExpression.attributeNames = {...serializedExpression.attributeNames, ...updateItemRequest.ExpressionAttributeNames};
            }
        }

        return _.merge({}, {
            ExpressionAttributeValues: serializedExpression.values,
            ExpressionAttributeNames: serializedExpression.attributeNames,
            UpdateExpression: Expressions.Stringify(serializedExpression.expressions)
        });
    }

    private static async ValidateItemFragment(schema: Schema, item: Item): Promise<{error: any}> {
        const result = {error: null};
        const error = {};

        const attributesToRemove = _.pickBy(item, _.isNull);
        const objectValueAttributes = _.pickBy(item, i => _.isPlainObject(i) && (i.$add || i.$del));
        const attributesToUpdate = _.omit(item, Object.keys(attributesToRemove).concat(Object.keys(objectValueAttributes)));

        const removalValidation: ValidationResult<any> = (await schema.validate({}, {abortEarly: false}));

        if (removalValidation.error) {
            const errors = _.pickBy(removalValidation.error.details, e => _.isEqual(e.type, 'any.required') && Object.prototype.hasOwnProperty.call(attributesToRemove, e.path));
            if (!_.isEmpty(errors)) {
                error['remove'] = errors;
                result.error = error;
            }
        }

        const updateValidation: ValidationResult<any> = (await schema.validate(attributesToUpdate, {abortEarly: false}));
        if (updateValidation.error) {
            const errors = _.pickBy(updateValidation.error.details, e => _.isEqual(e.type, 'any.required'));
            if (!_.isEmpty(errors)) {
                error['update'] = errors;
                result.error = error;
            }
        }

        return Promise.resolve(result);
    }

    private static PrepareItemUpdate(schema: Schema, item: any) {
        const updatedAt: string | boolean = schema.getUpdatedAt();
        const updatedAtParameterName: string = _.isString(updatedAt) ? updatedAt : 'updatedAt';

        if (schema.hasTimestamps() && updatedAt !== false && _.has(item, updatedAtParameterName)) {
            item[updatedAtParameterName] = new Date().toISOString();
        }

        return item;
    }
}
