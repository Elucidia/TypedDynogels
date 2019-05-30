import * as _ from 'lodash';
import * as utils from 'util'
import {DocumentClient} from 'aws-sdk/clients/dynamodb';
import {Internals} from '../internals/internals';
import {DynamoDBItemTypes} from '../database/types/dynamodbItemTypes';
import {Utils} from '../util/utils';
import {Schema} from '../schema/schema';
import {Item} from '../database/item';
import {Log} from '../util/log';
import {ISerializeItemOptions} from './types/iSerializeItemOptions';
import {UpdateActions} from './types/updateActions';
import {DynamoDB} from 'aws-sdk';

export class Serializer {
    public static CreateSet(value: any, options?: DocumentClient.CreateSetOptions): DocumentClient.DynamoDbSet {
        if (_.isArray(value)) {
            return Internals.GetDocumentClient().createSet(value, options);
        } else {
            return Internals.GetDocumentClient().createSet([value], options);
        }
    }

    public static DeserializeAttribute(value: DocumentClient.DynamoDbSet | any) {
        /*if (_.isObject(value) && _.isFunction(value.detectType) && _.isArray(value.values)) {
            return value.values;
        } else {
            return value;
        }*/
        if (Utils.IsObject(value) && Array.isArray(value)) {
            return value.values;
        } else {
            return value;
        }
    }

    public static SerializeAttribute(value: any, type: DynamoDBItemTypes): any {
        if (!type) {
            return value;
        }

        if (_.isNull(value)) {
            return null;
        }

        switch (type) {
            case DynamoDBItemTypes.BINARY: return Serializer.StringToBinary(value);
            case DynamoDBItemTypes.BINARY_SET:
            case DynamoDBItemTypes.NUMBER_SET:
            case DynamoDBItemTypes.STRING_SET: return Serializer.CreateSet(value);
            case DynamoDBItemTypes.BOOLEAN: return Serializer.ToBoolean(value);
            default: return value;
        }
    }

    public static SerializeItem(schema: Schema, item: Object, options?: ISerializeItemOptions) {
        options = options || {
            expected: false,
            returnNulls: false
        };

        const datatypes = schema.getDatatypes();

        if (!item) {
            return null;
        }

        return _.reduce(item, (accumulator, currentValue, key) => {
            // Expect is deprecated by AWS
            /*if (options.expected && _.isObject(val) && _.isBoolean(val.Exists)) {
                result[key] = val;
                return result;
            }*/
            if (_.isPlainObject(currentValue)) {
                accumulator[key] = Serializer.SerializeItem(schema, currentValue, datatypes[key]);
                return accumulator;
            }

            const serializedAttribute: any = Serializer.SerializeAttribute(currentValue, datatypes[key]);

            // si c'est non-null || les nuls sont acceptés ? wtf cette condition
            if (!_.isNull(serializedAttribute) || options.returnNulls) {
                accumulator[key] = serializedAttribute;
            }

            return accumulator;
        }, {});
    }

    public static DeserializeItem(item) {
        if (_.isNull(item)) {
            return null;
        }

        let map;
        if (_.isArray(item)) {
            map = _.map;
        } else {
            map = _.mapValues;
        }

        return map(item, (value) => {
            let result;
            if (_.isPlainObject(value) || _.isArray(value)) {
                result = Serializer.DeserializeItem(value);
            } else {
                result = Serializer.DeserializeAttribute(value);
            }

            return result;
        })
    }

    public static SerializeItemForUpdate(schema: Schema, action: UpdateActions, item: Item): DynamoDB.AttributeValueUpdate {
        const datatypes = schema.getDatatypes();

        const data = Utils.OmitPrimaryKeys(schema, item);
        return _.reduce(data, (accumulator, value, key) => {
            if (_.isNull(value)) {
                accumulator[key] = {Action: UpdateActions.DELETE};
            } else {
                accumulator[key] = {Action: action, Value: Serializer.SerializeAttribute(value, datatypes[key])};
            }
            return accumulator;
        }, {});
    }

    /**
     * Receives AWS format key
     *
     * AWS format: {keyName: {S/N/whatever dynamodb type: keyValue}}
     *
     * @param schema
     * @param dynamoDBKey The hash key in AWS format
     */
    public static BuildKey(schema: Schema, dynamoDBKey: Object) {
        const keysObject: Object = {};
        const hashKeyName: string = schema.getHashKeyName();
        const rangeKeyName: string = schema.getRangeKeyName();

        if (_.isPlainObject(dynamoDBKey)) {
            keysObject[hashKeyName] = dynamoDBKey[hashKeyName];

            if (rangeKeyName && !_.isNull(dynamoDBKey[rangeKeyName]) && !_.isUndefined(dynamoDBKey[rangeKeyName])) {
                keysObject[rangeKeyName] = dynamoDBKey[rangeKeyName];
            }

            _.each(schema.getGlobalIndexes(), (index) => {
                if (_.has(dynamoDBKey, index.hashKeyName)) {
                    keysObject[index.hashKeyName] = dynamoDBKey[index.hashKeyName];
                }

                if (_.has(dynamoDBKey, index.rangeKeyName)) {
                    keysObject[index.rangeKeyName] = dynamoDBKey[index.rangeKeyName];
                }
            });

            _.each(schema.getSecondaryIndexes(), (index) => {
                if (_.has(dynamoDBKey, index.rangeKeyName)) {
                    keysObject[index.rangeKeyName] = dynamoDBKey[index.rangeKeyName];
                }
            });
        } else {
            Log.Error(Serializer.name, 'BuildKey', 'DynamoDBKey must be an object', [{name: 'Typeof dynamoDBKey', value: typeof dynamoDBKey}]);
            return null;
        }

        return Serializer.SerializeItem(schema, keysObject);
    }

    public static StringToBinary(value: string | any): Uint8Array | any {
        return _.isString(value) ? Utils.StringToBinary(value) : value;
    }

    public static ToBoolean(value: any): boolean {
        return (value && value !== 'false') ? true : false;
    }
}
