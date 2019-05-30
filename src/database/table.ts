import * as _ from 'lodash';
import {DocumentClient, GetItemInput, GetItemOutput, PutItemInput} from 'aws-sdk/clients/dynamodb';
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
import {ICreateItemOptions} from './types/iCreateItemOptions';
import {ConditionExpressionOperators} from './types/conditionExpressionOperators';

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

    public async sendRequest(method: string, parameters: any): Promise<any | AWSError> {
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
        await this.sendRequest('get', parameters)
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
    public async createItem(item: Item, options?: ICreateItemOptions): Promise<any> {
        options = options || {};

        if (Array.isArray(item)) {
            await map(item, async (data, callback) => {
                await Table.NotifyBeforeEventListeners(this, TableEvents.CREATE, Table.PrepareItem)
                    .then(async (result: Object) => {
                        const validatedSchema: ValidationResult<any> = (await this.schema.validate(result));
                        if (validatedSchema.error) {
                            validatedSchema.error.message = `${validatedSchema.error.message} on ${this.getTableName()}`;
                            return Promise.reject(validatedSchema.error);
                        }

                        const nullOmittedAttributes = Utils.OmitNulls(result);
                        let itemParameters = {
                            TableName: this.getTableName(),
                            Item: Serializer.SerializeItem(this.schema, nullOmittedAttributes);
                        };

                        if (options.expected) {

                        }
                    })
                    .catch((error: any) => {
                        Log.Error(Table.name, 'createItem', 'An error occurred while notifying listeners for "create" event', [{name: 'Error', value: error}]);
                        return Promise.reject(error);
                    })
            })
        }
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

    private static AddConditionExpressionToPutItemRequest(putItemRequest: PutItemInput, expectedConditions) {
        _.each(expectedConditions, (value, key) => {
            let operator: ConditionExpressionOperators;
            let expectedValue = null;

            const existingValueKeys: string[] = _.keys(putItemRequest.ExpressionAttributeValues);

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

            const condition =
        });
    }
}
