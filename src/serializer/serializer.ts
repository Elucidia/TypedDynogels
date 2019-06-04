import * as _ from 'lodash';
import * as utils from 'util'
import {DocumentClient, AttributeMap} from 'aws-sdk/clients/dynamodb';
import {Internals} from '../internals/internals';
import {DynamoDBItemTypes} from '../database/types/dynamodbItemTypes';
import {Utils} from '../util/utils';
import {Schema} from '../schema/schema';
import {Item} from '../database/item';
import {Log} from '../util/log';
import {ISerializeItemOptions} from './types/iSerializeItemOptions';
import {UpdateActions} from './types/updateActions';
import {DynamoDB} from 'aws-sdk';
import {IIndex} from '../schema/types/iIndex';

export class Serializer {
    public static CreateSet(value: any, options?: DocumentClient.CreateSetOptions): DocumentClient.DynamoDbSet {
        if (_.isArray(value)) {
            return Internals.GetDocumentClient().createSet(value, options);
        } else {
            return Internals.GetDocumentClient().createSet([value], options);
        }
    }

    // TODO: is this function has a meaning to stay?
    public static DeserializeAttribute(value: DocumentClient.DynamoDbSet | DocumentClient.DynamoDbSet[]) {
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

    public static DeserializeItem(item: AttributeMap) {
        if (_.isNull(item)) {
            return null;
        }

        let map: Function;
        if (_.isArray(item)) { // TODO: split in two functions (one for array, one for not array)
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
     * Takes a hashKey and an optional rangeKey. These can be either a simple value or an object. Outputs this format: {hashKeyName: hashKeyValue}
     * @param schema
     */
    public static BuildKey(schema: Schema, hashKeyValue: any, rangeKeyValue?: any) {
        const keysObject: Object = {};
        const hashKeyName: string = schema.getHashKeyName();
        const rangeKeyName: string = schema.getRangeKeyName();

        if (_.isPlainObject(hashKeyValue)) {
            keysObject[hashKeyName] = hashKeyValue[hashKeyName];

            if (rangeKeyName && !_.isNull(hashKeyValue[rangeKeyName]) && !_.isUndefined(hashKeyValue[rangeKeyName])) {
                keysObject[rangeKeyName] = hashKeyValue[rangeKeyName];
            }

            schema.getGlobalIndexes().forEach((index: IIndex) => {
                if (Object.keys(hashKeyValue).includes(index.hashKeyName)) {
                    keysObject[index.hashKeyName] = hashKeyValue[index.hashKeyName];
                }

                if (Object.keys(hashKeyValue).includes(index.rangeKeyName)) {
                    keysObject[index.rangeKeyName] = hashKeyValue[index.rangeKeyName];
                }
            });

            schema.getLocalIndexes().forEach((index: IIndex) => {
                if (Object.keys(hashKeyValue).includes(index.rangeKeyName)) {
                    keysObject[index.rangeKeyName] = hashKeyValue[index.rangeKeyName];
                }
            });
            /*_.each(schema.getGlobalIndexes(), (index) => {
                if (_.has(hashKeyValue, index.hashKeyName)) {
                    keysObject[index.hashKeyName] = dynamoDBKey[index.hashKeyName];
                }

                if (_.has(hashKeyValue, index.rangeKeyName)) {
                    keysObject[index.rangeKeyName] = dynamoDBKey[index.rangeKeyName];
                }
            });

            _.each(schema.getLocalIndexes(), (index) => {
                if (_.has(dynamoDBKey, index.rangeKeyName)) {
                    keysObject[index.rangeKeyName] = dynamoDBKey[index.rangeKeyName];
                }
            });*/
        } else {
            keysObject[schema.getHashKeyName()] = hashKeyValue;
            if (schema.getRangeKeyName() && rangeKeyValue) {
                keysObject[schema.getRangeKeyName()] = rangeKeyValue;
            }
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
