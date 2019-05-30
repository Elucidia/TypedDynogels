import * as _ from 'lodash';
import {ScanOutput} from 'aws-sdk/clients/dynamodb';
import {DynamoDB} from 'aws-sdk';
import {Readable} from 'stream';
import {Schema} from '../schema/schema';

export class Utils {
    public static IsObject(value: any): boolean {
        return value !== null && typeof value === 'object';
    }

    // ref: lodash.js L 12037
    public static IsPlainObject(value: any): boolean {
        if (Utils.IsObject(value)) {
            const prototype = Object.getPrototypeOf(value);
            if (prototype === null) {
                return true;
            }
            const ctor = (value as Object).hasOwnProperty.call(prototype, 'constructor') && prototype.constructor;
            return typeof ctor === 'function' && ctor instanceof ctor;
        }
    }

    public static IsString(value: any): boolean {
        return typeof value === 'string';
    }

    public static IsBoolean(value: any): boolean {
        return typeof value === 'boolean' && (value === false) || (value === true);
    }

    public static OmitNulls(data: any): Partial<any> {
        return _.omitBy(data, (value: any) => _.isNull(value) || _.isUndefined(value) || (_.isArray(value) && _.isEmpty(value)) || (_.isString(value) && _.isEmpty(value)));
    }

    /**
     * Merges the responses from a parallel scan call
     * @param responses
     * @param tableName
     */
    public static MergeResults(responses: ScanOutput[], tableName: string): ScanOutput {
        const accumultatedResult: ScanOutput = {
            ConsumedCapacity: {
                CapacityUnits: 0,
                TableName: tableName
            },
            Count: 0,
            Items: [],
            LastEvaluatedKey: null,
            ScannedCount: 0
        };

        const mergedResponses: ScanOutput = _.reduce(responses, (accumulator: ScanOutput, currentValue: ScanOutput) => {
            if (!currentValue) {
                return accumulator;
            }

            accumulator.Count += currentValue.Count || 0;
            accumulator.ScannedCount += currentValue.ScannedCount || 0;

            if (currentValue.ConsumedCapacity) {
                accumulator.ConsumedCapacity.CapacityUnits += currentValue.ConsumedCapacity.CapacityUnits || 0;
            }

            if (currentValue.Items) {
                accumulator.Items = accumulator.Items.concat(currentValue.Items);
            }

            if (currentValue.LastEvaluatedKey) {
                accumulator.LastEvaluatedKey = currentValue.LastEvaluatedKey;
            }

            return accumulator;
        }, accumultatedResult);

        if (mergedResponses.ConsumedCapacity.CapacityUnits === 0) {
            delete mergedResponses.ConsumedCapacity;
        }

        if (mergedResponses.ScannedCount === 0) {
            delete mergedResponses.ScannedCount;
        }

        return mergedResponses;
    }

    public static OmitPrimaryKeys(schema: Schema, parameters: any): any {
        return _.omit(parameters, schema.getHashKeyName(), schema.getRangeKeyName());
    }

    public static StringToBinary(value: string): Uint8Array {
        const len: number = value.length;
        const bin: Uint8Array = new Uint8Array(new ArrayBuffer(len));

        for (let i = 0; i < len; ++i) {
            bin[i] = value.charCodeAt(i);
        }

        return bin;
    }
}
