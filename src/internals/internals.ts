import * as _ from 'lodash';
import {DynamoDB} from 'aws-sdk';
import {DocumentClient} from 'aws-sdk/clients/dynamodb';
import {Log} from '../util/log';
import {Model} from './model';
import {object, string, alternatives, func, array, ref, required, optional, number, forbidden, ObjectSchema, boolean} from '@hapi/joi';
import {DynamoDBItemTypes} from '../database/types/dynamodbItemTypes';

export class Internals {
    public static readonly SECONDARY_INDEX_SCHEMA: ObjectSchema = object().keys({
        hashKeyName: string().when('type', {is: 'local', then: ref('$hashKeyName'), otherwise: required()}),
        rangeKeyName: string().when('type', {is: 'local', then: required(), otherwise: optional()}),
        type: string().valid('local', 'global').required(),
        name: string().required(),
        projection: object(),
        readCapacity: number().when('type', {is: 'global', then: optional(), otherwise: forbidden()}),
        writeCapacity: number().when('type', {is: 'global', then: optional(), otherwise: forbidden()})
    });

    public static readonly CONFIGURATION_SCHEMA: ObjectSchema = object().keys({
        hashKeyName: string().required(),
        rangeKeyName: string(),
        tableName: alternatives().try(string(), func()),
        indexes: array().items(Internals.SECONDARY_INDEX_SCHEMA),
        schema: object(),
        timestamps: boolean().default(false),
        createdAt: alternatives().try(string(), boolean()),
        updatedAt: alternatives().try(string(), boolean()),
        validation: {
            abortEarly: boolean(),
            convert: boolean(),
            allowUnknown: boolean(),
            skipFunctions: boolean(),
            stripUnknown: boolean(),
            language: object(),
            presence: string().allow('optional', 'required', 'forbidden', 'ignore'),
            strip: boolean(),
            noDefaults: boolean()
        }
    }).required();

    private static dynamoDbDriver: DynamoDB;
    private static documentClient: DocumentClient;
    private static models: Map<string, Model> = new Map<string, Model>(); //TODO: extract and put it in modelManager

    public static InitializeDynamoDBDriver(endpoint: string, region: string, apiVersion: string='2012-08-10'): void {
        Internals.dynamoDbDriver = new DynamoDB({
            apiVersion: apiVersion,
            endpoint: endpoint,
            region: region
        });

        Internals.InitializeDocumentClient();
    }

    public static GetDynamoDBDriver(): DynamoDB {
        return Internals.dynamoDbDriver;
    }

    public static GetDocumentClient(): DocumentClient {
        return Internals.documentClient;
    }

    public static RegisterModel(name: string, model: Model): void {
        Log.Trace(Internals.name, 'RegisterModel', `Registering model ${name}`);
        Internals.models.set(name, model);
    }

    public static GetModel(name: string): Model {
        Log.Trace(Internals.name, 'GetModel', `Returning model value for ${name}`);
        return Internals.models.has(name) ? Internals.models.get(name) : null;
    }

    public static UnregisterModel(name: string): void {
        if (Internals.models.has(name)) {
            Log.Trace(Internals.name, 'UnregisterModel', `Unregistering model ${name}`);
            Internals.models.delete(name);
        }
    }

    public static ClearModels(): void {
        Log.Trace(Internals.name, 'ClearModels', 'Clearing models');
        Internals.models.clear();
    }

    public static ParseDynamoDBTypes(data: any): any {
        if (_.isPlainObject(data) && typeof data === 'object' && _.isPlainObject(data.children)) {
            return Internals.ParseDynamoDBTypes(data.children);
        }

        const mapped = _.reduce(data, (result, value, key) => {
            if (typeof value === 'object' && _.isPlainObject(value.children)) {
                result[key] = Internals.ParseDynamoDBTypes(value.children);
            } else {
                result[key] = Internals.FindDynamoDBTypeMetadata(value);
            }
            return result;
        }, {});

        return mapped;
    }

    public static FindDynamoDBTypeMetadata(data: any): DynamoDBItemTypes {
        const metadata = _.find(data.metadata, (data) => _.isString(data.dynamoDBType));

        if (metadata) {
            return metadata.dynamoDBType;
        } else {
            return Internals.ParseDynamoDBTypes(typeof data);
        }
    }

    public static InvokeDefaultFunctions(data: any): any {
        _.mapValues(data, (value) => {
            if (_.isPlainObject(value)) {
                return Internals.InvokeDefaultFunctions(value);
            } else {
                return value;
            }
        });
    }

    public static ParseJSTypeToDynamoDBType(type: string): DynamoDBItemTypes {
        switch (type) {
            case 'boolean': return DynamoDBItemTypes.BOOLEAN;
            case 'number': return DynamoDBItemTypes.NUMBER;
            case 'string': return DynamoDBItemTypes.STRING;
            case 'binary': return DynamoDBItemTypes.BINARY;
            case 'array': return DynamoDBItemTypes.LIST;
            default: return DynamoDBItemTypes.NULL;
        }
    }

    private static InitializeDocumentClient(): void {
        if (Internals.dynamoDbDriver) {
            Internals.documentClient = new DocumentClient({service: Internals.dynamoDbDriver});
            Log.Trace(Internals.name, 'InitializeDocumentClient', 'Initialized document client');
        } else {
            Log.Fatal(Internals.name, 'InitializeDocumentClient', 'Dynamo DB driver was not properly created');
        }
    }
}
