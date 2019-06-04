import * as _ from 'lodash';
import {AttributeValue} from 'aws-sdk/clients/dynamodbstreams';
import {BatchGetItemInput, BatchGetItemOutput, AttributeMap} from 'aws-sdk/clients/dynamodb';
import {Table} from '../database/table';
import {Serializer} from '../serializer/serializer';
import {Utils} from '../util/utils';
import {Log} from '../util/log';
import {Item} from '../database/item';

export class Batch {
    /**
     *
     * @param table
     * @param keys Va probablement falloir changer le type, ça doit etre un array [ {name: value} ] apres on convertit en dyndb
     */
    public static async GetItems(table: Table, keys: Object[], additionalOptions?: BatchGetItemInput): Promise<BatchGetItemOutput[]> {
        const bucketizedKeys: Object[] = Batch.Bucketize(keys);
        const promises: any[] = [];

        bucketizedKeys.forEach((bucket: Object[]) => {
            promises.push(Batch.InitialBatchGetItems(table, bucket, additionalOptions));
        });

        let result: BatchGetItemOutput[];
        await Promise.all(promises)
            .then((success: any[]) => {
                _.flatten(success); // replacement? https://stackoverflow.com/questions/10865025/merge-flatten-an-array-of-arrays
            })
            .catch((error: any) => {
                Log.Error(Batch.name, 'GetItems', 'Unable to perform "batchGetItems" operation');
                return Promise.reject(error);
            });
        return Promise.resolve(result);
    }

    private static BuildInitialGetItemsRequest(tableName: string, keys: Map<string, AttributeValue>, additionalOptions?: BatchGetItemInput): BatchGetItemInput {
        const request = {};
        request[tableName] = {...{Keys: keys}, ...additionalOptions};

        return {RequestItems: request};
    }

    // TODO: test!!!
    private static SerializeKeys(table: Table, keys: Object[]): Map<string, AttributeValue> {
        const serializedKeys: Map<string, AttributeValue> = new Map<string, AttributeValue>();
        keys.forEach((unserializedKey: Object) => {
            const keyName: string = Object.keys(unserializedKey)[0];
            serializedKeys.set(keyName, Serializer.BuildKey(table.getSchema(), unserializedKey));
        });

        return serializedKeys;
    }

    private static MergeResponses(tableName: string, responses: BatchGetItemOutput[]): BatchGetItemOutput {
        const base = {
            Responses: {},
            ConsumedCapacity: []
        };

        base.Responses[tableName] = [];

        return responses.reduce((accumulator: BatchGetItemOutput, currentValue: BatchGetItemOutput) => {
            if (currentValue.Responses && currentValue.Responses[tableName]) {
                accumulator[tableName] = accumulator.Responses[tableName].concat(currentValue.Responses[tableName]);
            }
            return accumulator;
        }, base);
    }

    private static async PaginatedRequest(table: Table, request: BatchGetItemInput): Promise<BatchGetItemOutput | any> {
        const responses: BatchGetItemOutput[] = [];
        await table.runBatchGetItems(request)
            .then((response: BatchGetItemOutput) => {
                responses.push(response);
                if (Batch.MoreKeysToProcess(response.UnprocessedKeys)) {
                    request.RequestItems = response.UnprocessedKeys;
                    Batch.PaginatedRequest(table, request); // TODO test if it works
                }
            })
            .catch((error: any) => {
                Log.Error(Batch.name, 'PaginatedRequest', 'An error occurred while getting batch items', [{name: 'Error', value: error}, {name: 'Request', value: request}]);
                return Promise.reject(error);
            });
        return Promise.resolve(Batch.MergeResponses(table.getTableName(), responses));
    }

    /**
     * Creates an array of non-formatted keys. Each index contains an array of a maximum of 100 keys
     * @param keys Non-formatted keys
     */
    private static Bucketize(keys: Object[]): Object[] {
        const buckets: Object[] = [];
        while (keys.length) {
            buckets.push(keys.splice(0, 100));
        }

        return buckets;
    }

    private static async InitialBatchGetItems(table: Table, keys: Object[], additionalOptions?: BatchGetItemInput): Promise<BatchGetItemOutput | any> {
        const serializedKeys: Map<string, AttributeValue> = Batch.SerializeKeys(table, keys);
        const request: BatchGetItemInput = Batch.BuildInitialGetItemsRequest(table.getTableName(), serializedKeys, additionalOptions);
        let items: Item[];
        await Batch.PaginatedRequest(table, request)
            .then((success: BatchGetItemOutput) => {
                const dynamoItems: AttributeMap[] = success.Responses[table.getTableName()];
                items = dynamoItems.map((item: AttributeMap) => table.createItem(Serializer.DeserializeItem(item)));
            });

        return Promise.resolve(items);
    }

    private static MoreKeysToProcess(request: any): boolean {
        return request !== null && _.isEmpty(request);
    }
}
