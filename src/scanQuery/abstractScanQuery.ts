import * as _ from 'lodash';
import {ScanInput, QueryInput, ExpressionAttributeValueMap, ExpressionAttributeNameMap} from 'aws-sdk/clients/dynamodb';
import {IFilterExpression} from '../expression/types/iFilterExpression';
import {Table} from '../database/table';
import {IScanQueryOptions} from './types/iScanQueryOptions';
import {IScanQueryFilter} from './types/iScanQueryFilter';
import {IKeyCondition} from './types/iKeyCondition';
import {Log} from '../util/log';
import {Serializer} from '../serializer/serializer';

/**
 * Abstract class containing functions used by both of Query and Scan classes
 * @type T must be either Query, Scan or ParallelScan
 * @type U must be either QueryInput or ScanInput
 */
export abstract class AbstractScanQuery<T, U extends QueryInput | ScanInput> {
    protected table: Table;
    protected options: IScanQueryOptions;
    protected request: U;
    private isQuery: boolean;

    constructor(table: Table, isQuery: boolean = false) {
        this.isQuery = isQuery;
        this.table = table;
        this.options = {loadAll: false};
    }

    public buildRequest(): U {
        return {...this.request, ...{TableName: this.table.getTableName()}};
    }

    // replace Scan.addFilterCondition and Query.addFilterCondition
    public addExpressionAttributesFromFilterExpressionToRequest(filterExpression: IFilterExpression): AbstractScanQuery<T, U> {
        const expressionAttributeNames = {...filterExpression.attributeNames, ...this.request.ExpressionAttributeNames}; // _.merge({}, condition.attributeNames, request.ExpressionAttributeNames);
        const expressionAttributeValues = {...filterExpression.attributeValues, ...this.request.ExpressionAttributeValues}; // _.merge({}, condition.attributeValues, request.ExpressionAttributeValues);

        if (_.isEmpty(expressionAttributeNames)) {
            this.request.ExpressionAttributeNames = expressionAttributeNames;
        }
        if (_.isEmpty(expressionAttributeValues)) {
            this.request.ExpressionAttributeValues = expressionAttributeValues;
        }

        if (!this.isQuery) {
            if (_.isString(this.request.FilterExpression)) {
                this.request.FilterExpression = `${this.request.FilterExpression} AND (${filterExpression.statement})`;
            } else {
                this.request.FilterExpression = `(${filterExpression.statement})`;
            }
        }

        return this;
    }

    public setLimit(limit: number): T {
        if (limit <= 0) {
            Log.Error(AbstractScanQuery.name, 'setLimit', 'Limit parameter must be greater than 0', [{name: 'Given limit', value: limit}]);
        } else {
            this.request.Limit = limit;
        }
        return this as unknown as T;
    }

    public setFilterExpression(filterExpression: string): T {
        this.request.FilterExpression = filterExpression;
        return this as unknown as T;
    }

    public setExpressionAttributeValues(values: ExpressionAttributeValueMap): T {
        this.request.ExpressionAttributeValues = values;
        return this as unknown as T;
    }

    public setExpressionAttributeNames(names: ExpressionAttributeNameMap): T {
        this.request.ExpressionAttributeNames = names;
        return this as unknown as T;
    }

    public setProjectExpression(projection: string): T {
        this.request.ProjectionExpression = projection;
        return this as unknown as T;
    }

    public setExclusiveStartKey(hashKey: Object, rangeKey: Object): T {
        this.request.ExclusiveStartKey = Serializer.BuildKey(this.table.getSchema(), {hashKey, rangeKey});
        return this as unknown as T;
    }

    public setAttributes(attributes: any | any[]): T {
        if (!_.isArray(attributes)) {
            attributes = [attributes];
        }

        const expressionAttributeNames: ExpressionAttributeNameMap = attributes.reduce((accumulator: any, currentValue: any) => {
            const path = `#${currentValue}`;
            accumulator[path] = currentValue;
            return accumulator;
        }, {});

        this.request.ProjectionExpression = Object.keys(expressionAttributeNames).join(',');
        this.request.ExpressionAttributeNames = {...expressionAttributeNames, ...this.request.ExpressionAttributeNames};

        return this as unknown as T;
    }

    public select(value): T {
        this.request.
    }

    public getRequest(): U {
        return this.request;
    }

    public setRequest(request: U): T {
        this.request = request;
        return this as unknown as T;
    }

    public abstract where(keyName: string): IKeyCondition | IScanQueryFilter;
}
