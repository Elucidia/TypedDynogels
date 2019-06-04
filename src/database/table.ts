import * as _ from 'lodash';
import {DocumentClient, GetItemInput, GetItemOutput, PutItemInput, DeleteItemInput, ExpressionAttributeNameMap, ExpressionAttributeValueMap, QueryInput, UpdateItemInput, UpdateItemOutput, DeleteItemOutput, PutItemOutput, QueryOutput, ScanOutput, AttributeMap, ScanInput, BatchGetItemInput, BatchGetItemOutput, AttributeDefinition, KeySchemaElement, LocalSecondaryIndex, GlobalSecondaryIndex, CreateTableInput, CreateTableOutput, DescribeTableOutput, DescribeTableInput, DeleteTableOutput, DeleteTableInput, GlobalSecondaryIndexDescription, UpdateTableInput, ProvisionedThroughput, UpdateTableOutput} from 'aws-sdk/clients/dynamodb';
import {waterfall, map, mapLimit} from 'async';
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
import {ParallelScan} from '../scanQuery/parallelScan';
import {KeyTypes} from './types/keyTypes';
import {ISecondaryIndex} from '../schema/types/iSecondaryIndex';
import {ProjectionTypes} from '../schema/types/projectionTypes';
import {IMakeTableOptions} from './types/iMakeTableOptions';
import {IIndex} from '../schema/types/iIndex';

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
    public createItem(attributes: Object): Item {
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
        await this.sendRequest(DynamoDBRequestMethods.GET_ITEM, parameters)
            .then((success: GetItemOutput) => {
                let item = null;
                if (success.Item) {
                    item = this.createItem(Serializer.DeserializeItem(success.Item));
                }
            })
            .catch((error: AWSError) => {
                Log.Error(Table.name, 'getItem', 'Unable to get item');
                return Promise.reject(error);
            });

        return Promise.resolve(item);
    }
    // TODO: bien tester, pas sur du resultat
    // TODO: retourner PutItemOutput
    /**
     *
     * @param item the Item from PutItemInput
     * @param options
     */
    public async putItem(item: Object|Object[], options?: IPutItemOptions): Promise<any> {
        options = options || {};
        let itemPut;
        if (Array.isArray(item)) {
            //await map(item, async (data, callback) => {);
            const putItemPromises: any[] = [];
            item.forEach((singleItem: Object) => {
                putItemPromises.push(Table.CreateAndPutItem(this, singleItem, options || {}));
            });

            await Promise.all(putItemPromises);
        } else {
            await Table.CreateAndPutItem(this, item, options || {});
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
        await this.sendRequest(DynamoDBRequestMethods.UPDATE_ITEM, updateParameters)
            .then((success: UpdateItemOutput) => {
                let result = null;
                if (success.Attributes) {
                    result = this.createItem(Serializer.DeserializeItem(success.Attributes));
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
        await this.sendRequest(DynamoDBRequestMethods.DELETE_ITEM, deleteParameters)
            .then((success: DeleteItemOutput) => {
                let item = null;
                if (success.Attributes) {
                    item = this.createItem(Serializer.DeserializeItem(success.Attributes));
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

    public getParallelScanRequestForTable(totalSegments: number): ParallelScan {
        return new ParallelScan(this, totalSegments);
    }

    public runQuery(request: QueryInput): Promise<QueryOutput> {
        return this.runScanOrQuery(DynamoDBRequestMethods.QUERY, request);
    }

    public runScan(request: ScanInput): Promise<ScanOutput> {
        return this.runScanOrQuery(DynamoDBRequestMethods.SCAN, request);
    }

    public runBatchGetItems(request: BatchGetItemInput): Promise<BatchGetItemOutput> {
        return this.sendRequest(DynamoDBRequestMethods.BATCH_GET, request);
    }

    public async createTable(additionalOptions?: IMakeTableOptions): Promise<CreateTableOutput | any> {
        let createTableResult: CreateTableOutput;
        await this.sendRequest(DynamoDBRequestMethods.CREATE_TABLE, this.makeCreateTableRequest(additionalOptions))
            .then((success: CreateTableOutput) => {
                createTableResult = success;
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'createTable', 'Unable to perform "createTable" operation');
                return Promise.reject(error);
            });
        return Promise.resolve(createTableResult);
    }

    public async describeTable(): Promise<DescribeTableOutput | any> {
        let describeTableResult: DescribeTableOutput;
        await this.sendRequest(DynamoDBRequestMethods.DESCRIBE_TABLE, {TableName: this.getTableName()} as DescribeTableInput)
            .then((success: DescribeTableOutput) => {
                describeTableResult = success;
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'describeTable', 'Unable to perform "describeTable" operation');
                return Promise.reject(error);
            });
        return Promise.resolve(describeTableResult);
    }

    public async deleteTable(): Promise<DeleteTableOutput | any> {
        let deleteTableResult: DeleteTableOutput;
        await this.sendRequest(DynamoDBRequestMethods.DELETE_TABLE, {TableName: this.getTableName()} as DeleteTableInput)
            .then((success: DeleteTableOutput) => {
                deleteTableResult = success;
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'deleteTable', 'Unable to perform "deleteTable" operation');
                return Promise.reject(error);
            });
        return Promise.resolve(deleteTableResult);
    }

    public async updateTableThroughput(readCapacity: number, writeCapacity: number): Promise<UpdateTableOutput[] | any> {
        let updateResult: UpdateTableOutput[];
        await Promise.all([
            Table.SynchronizeIndexes(this),
            Table.UpdateTableCapacity(this, readCapacity, writeCapacity)
        ])
            .then((success: UpdateTableOutput[]) => {
                updateResult = success;
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'updateTableThroughput', 'Unable to update table throughput and synchronize indexes');
                return Promise.reject(error);
            });
        return Promise.resolve(updateResult);
    }

    private makeCreateTableRequest(additionalOptions?: IMakeTableOptions): CreateTableInput {
        const attributeDefinitions: AttributeDefinition[] = [];
        const localSecondaryIndexes: LocalSecondaryIndex[] = [];
        const globalSecondaryIndexes: GlobalSecondaryIndex[] = [];
        let keySchema: KeySchemaElement[];

        attributeDefinitions.push(Table.GetAttributeDefinition(this.schema, this.schema.getHashKeyName()));

        if (this.schema.getRangeKeyName()) {
            attributeDefinitions.push(Table.GetAttributeDefinition(this.schema, this.schema.getRangeKeyName()));
        }

        _.forEach(this.schema.getLocalIndexes(), (secondaryIndex: any /* ISecondaryIndex */) => { // TODO! wipe this lodash thingy out of here!
            attributeDefinitions.push(Table.GetAttributeDefinition(this.schema, secondaryIndex.rangeKeyName));
            localSecondaryIndexes.push(Table.GetLocalSecondaryIndex(this.schema, secondaryIndex));
        });

        _.forEach(this.schema.getGlobalIndexes(), (globalIndex: any, indexName: string) => {
            if (!_.find(attributeDefinitions, {AttributeName: globalIndex.hashKey})) {
                attributeDefinitions.push(Table.GetAttributeDefinition(this.schema, globalIndex.hashKeyName));
            }

            if (globalIndex.rangeKeyName && !_.find(attributeDefinitions, {AttributeName: globalIndex.rangeKeyName})) {
                attributeDefinitions.push(Table.GetAttributeDefinition(this.schema, globalIndex.rangeKeyName));
            }

            globalSecondaryIndexes.push(Table.GetGlobalSecondaryIndex(indexName, globalIndex));
        });

        keySchema = Table.GetKeySchema(this.schema.getHashKeyName(), this.schema.getRangeKeyName());

        const createTableRequest: CreateTableInput = {
            AttributeDefinitions: attributeDefinitions,
            TableName: this.getTableName(),
            KeySchema: keySchema,
            ProvisionedThroughput: {
                ReadCapacityUnits: additionalOptions.readCapacity || 1,
                WriteCapacityUnits: additionalOptions.writeCapacity || 1
            }
        };

        if (localSecondaryIndexes.length >= 1) {
            createTableRequest.LocalSecondaryIndexes = localSecondaryIndexes;
        }

        if (globalSecondaryIndexes.length >= 1) {
            createTableRequest.GlobalSecondaryIndexes = globalSecondaryIndexes;
        }

        if (additionalOptions.streamSpecification) {
            createTableRequest.StreamSpecification = additionalOptions.streamSpecification;
        }

        if (additionalOptions.billingMode) {
            createTableRequest.BillingMode = additionalOptions.billingMode;
        }

        if (additionalOptions.sseSpecification) {
            createTableRequest.SSESpecification = additionalOptions.sseSpecification;
        }

        if (additionalOptions.tags) {
            createTableRequest.Tags = additionalOptions.tags;
        }

        return createTableRequest;
    }

    private async runScanOrQuery(operation: DynamoDBRequestMethods, request: QueryInput | ScanInput): Promise<QueryOutput|ScanOutput> {
        let result: QueryOutput | ScanOutput;
        await this.sendRequest(operation, request)
            .then((success: QueryOutput | ScanOutput) => {
                result = Table.DeserializeItemsForQueryOrScan(this, success);
            })
            .catch((error: any) => {
                Log.Error(Table.name, `run${operation}`, `Unable to perform "${operation}" operation`);
                return Promise.reject(error);
            });

        return Promise.resolve(result);
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

    private static async CreateAndPutItem(table: Table, item: Object, options: IPutItemOptions): Promise<PutItemOutput | any> {
        let notifierResult: Object;
        await Table.NotifyBeforeEventListeners(table, TableEvents.CREATE, Table.PrepareItem)
            .then((result: Object) => {
                notifierResult = result;
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'CreateItemAndPut', 'An error occurred while notifying listeners for "create" event', [{name: 'Error', value: error}]);
                return Promise.reject(error);
            });

        const validatedSchema: ValidationResult<any> = (await table.schema.validate(notifierResult));
        if (validatedSchema.error) {
            validatedSchema.error.message = `${validatedSchema.error.message} on ${table.getTableName()}`;
            return Promise.reject(validatedSchema.error);
        }

        const nullOmittedAttributes = Utils.OmitNulls(notifierResult);
        let putItemRequest: PutItemInput = {
            TableName: table.getTableName(),
            Item: Serializer.SerializeItem(table.schema, nullOmittedAttributes)
        };

        if (options.expected) {
            Table.AddConditionExpressionToRequest(putItemRequest, options.expected);
            delete options.expected;
        }

        if (!options.overwrite) {
            const expected = _.chain([table.schema.getHashKeyName(), table.schema.getRangeKeyName()]).compact().reduce((accumulator: any, key: string) => {
                _.set(accumulator, `${key}.<>`, _.get(putItemRequest.Item, key));
                return accumulator;
            }, {}).value();
            Table.AddConditionExpressionToRequest(putItemRequest, expected);
        }

        delete options.overwrite;
        putItemRequest = {...putItemRequest, ...options.putItemRequest};

        let resultToReturn: PutItemOutput;
        await table.sendRequest(DynamoDBRequestMethods.PUT_ITEM, putItemRequest)
            .then((result: PutItemOutput) => {
                resultToReturn = result;
                table.afterEvent.emit(TableEvents.CREATE, result);
            })
            .catch((error: any) => {
                Log.Error(Table.name, 'createItem', 'An error occurred while putting item in DynamoDB');
                return Promise.reject(error);
            });

        return Promise.resolve(resultToReturn);
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

    private static DeserializeItemsForQueryOrScan(table: Table, requestResult: QueryOutput | ScanOutput): QueryOutput | ScanOutput {
        const result: QueryOutput | ScanOutput = {};
        if (requestResult.Items) {
            result.Items = requestResult.Items.map((item: AttributeMap) => table.createItem(Serializer.DeserializeItem(item)).getAttributes() as AttributeMap);
            delete requestResult.Items;
        }

        if (requestResult.LastEvaluatedKey) {
            result.LastEvaluatedKey = requestResult.LastEvaluatedKey;
            delete requestResult.LastEvaluatedKey;
        }

        return {...requestResult, ...result};
    }

    private static GetAttributeDefinition(schema: Schema, key: string): AttributeDefinition {
        return {
            AttributeName: key,
            AttributeType: schema.getDatatypes()[key]
        };
    }

    private static GetKeySchema(hashKeyName: string, rangeKeyName?: string): KeySchemaElement[] {
        const result: KeySchemaElement[] = [{
            AttributeName: hashKeyName,
            KeyType: KeyTypes.HASH
        }];

        if (rangeKeyName) {
            result.push({
                AttributeName: rangeKeyName,
                KeyType: KeyTypes.RANGE
            });
        }

        return result;
    }

    private static GetLocalSecondaryIndex(schema: Schema, secondaryIndex: IIndex): LocalSecondaryIndex {
        return {
            IndexName: secondaryIndex.name,
            KeySchema: Table.GetKeySchema(schema.getHashKeyName(), secondaryIndex.rangeKeyName),
            Projection: {
                ProjectionType: secondaryIndex.projection.ProjectionType || ProjectionTypes.ALL
            }
        }
    }

    private static GetGlobalSecondaryIndex(indexName: string, secondaryIndex: IIndex): GlobalSecondaryIndex {
        return {
            IndexName: indexName,
            KeySchema: Table.GetKeySchema(secondaryIndex.hashKeyName, secondaryIndex.rangeKeyName),
            Projection: {ProjectionType: secondaryIndex.projection.ProjectionType}, // TODO: put default parameter? ProjectionTypes.ALL?
            ProvisionedThroughput: {
                ReadCapacityUnits: secondaryIndex.readCapacity || 1,
                WriteCapacityUnits: secondaryIndex.writeCapacity || 1
            }
        };
    }

    private static async SynchronizeIndexes(table: Table): Promise<any> {
        const tableDescription: DescribeTableOutput = (await table.describeTable());
        const missing: Map<string, IIndex> = this.FindMissingGlobalIndexes(table, tableDescription);
        const promises: any[] = [];
        missing.forEach((value: IIndex, key: string) => {
            const attributeDefinitions: AttributeDefinition[] = [];
            attributeDefinitions.push(Table.GetAttributeDefinition(table.schema, value.hashKeyName));

            if (value.rangeKeyName && !attributeDefinitions.find((attribute) => attribute.AttributeName === value.rangeKeyName)) {
                attributeDefinitions.push(Table.GetAttributeDefinition(table.schema, value.rangeKeyName));
            }

            value.writeCapacity = value.writeCapacity || Math.ceil(tableDescription.Table.ProvisionedThroughput.WriteCapacityUnits * 1.5);

            Log.Info(Table.name, 'SynchronizeIndexes', 'Adding index to table', [
                {name: 'Table name', value: table.getTableName()},
                {name: 'Index name', value: value.name}
            ]);

            promises.push(table.sendRequest(DynamoDBRequestMethods.UPDATE_TABLE, {
                TableName: table.getTableName(),
                AttributeDefinitions: attributeDefinitions,
                GlobalSecondaryIndexUpdates: [{Create: Table.GetGlobalSecondaryIndex(key, value)}]
            } as UpdateTableInput));
        });

        return (await Promise.all(promises));
        /*return new Promise((resolve, reject) => { // TODO: remove this and everything inside and replace with better
            mapLimit(missing, 5, (item: IIndex, data, err) => {
                const attributeDefinitions: AttributeDefinition[] = [];
                if (!_.find(attributeDefinitions, {AttributeName: item.hashKeyName})) { // TODO: if attrDef = [], for sure it won't find anything....
                    attributeDefinitions.push(Table.GetAttributeDefinition(table.schema, item.hashKeyName));
                }

                if (item.rangeKeyName && !_.find(attributeDefinitions, {AttributeName: item.rangeKeyName})) {
                    attributeDefinitions.push(Table.GetAttributeDefinition(table.schema, item.rangeKeyName));
                }

                item.writeCapacity = item.writeCapacity || Math.ceil(tableDescription.Table.ProvisionedThroughput.WriteCapacityUnits * 1.5);

                Log.Info(Table.name, 'SynchronizeIndexes', 'Adding index to table', [
                    {name: 'Table name', value: table.getTableName()},
                    {name: 'Index name', value: item.name}
                ]);

                table.sendRequest
            });
        })*/
    }

    private static async UpdateTableCapacity(table: Table, readCapacity: number, writeCapacity: number): Promise<any> {
        const updateTableRequest: UpdateTableInput = {
            TableName: table.getTableName(),
            ProvisionedThroughput: {
                ReadCapacityUnits: readCapacity > 0 ? readCapacity : 1,
                WriteCapacityUnits: writeCapacity > 0 ? writeCapacity : 0
            }
        };

        return (await table.sendRequest(DynamoDBRequestMethods.UPDATE_TABLE, updateTableRequest));
    }

    private static FindMissingGlobalIndexes(table: Table, tableDescription: DescribeTableOutput): Map<string, IIndex> {
        if (!tableDescription) {
            return table.schema.getGlobalIndexes();
        }

        const existingIndexNames: string[] = tableDescription.Table.GlobalSecondaryIndexes.map((index: GlobalSecondaryIndexDescription) => index.IndexName);
        const missingIndexes: Map<string, IIndex> = new Map<string, IIndex>(table.schema.getGlobalIndexes());
        missingIndexes.forEach((currentValue: IIndex, indexName: string) => {
            if (!existingIndexNames.includes(currentValue.name)) {
                missingIndexes.delete(indexName);
            }
        });

        return missingIndexes;
        // OLD:
        /*return _.reduce(table.schema.getGlobalIndexes(), (accumulator: any, currentValue: IIndex, indexName: string) => {
            if (existingIndexNames.includes(currentValue.name)) {
                accumulator[indexName] = currentValue;
            }
            return accumulator;
        }, {});*/
    }
}
