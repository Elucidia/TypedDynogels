import * as _ from 'lodash';
import {ConditionExpressionOperators} from '../database/types/conditionExpressionOperators';
import {QueryInput, QueryOutput} from 'aws-sdk/clients/dynamodb';
import {Expressions} from '../expression/expressions';
import {IKeyCondition} from './types/iKeyCondition';
import {IScanQueryFilter} from './types/iScanQueryFilter';
import {Table} from '../database/table';
import {isNullOrUndefined} from 'util';
import {IScanQueryOptions} from './types/iScanQueryOptions';
import {Utils} from '../util/utils';
import {IFilterExpression} from '../expression/types/iFilterExpression';

export class Query { // TODO: diviser les op de builder dans une autre classe?
    private queryRequest: QueryInput;
    private table: Table;
    private hashKey: Object;
    private options: IScanQueryOptions;

    constructor(table: Table, hashKey: Object) {
        this.table = table;
        this.hashKey = hashKey;
        this.options.loadAll = false;
    }

    public addKeyCondition(filterExpression: IFilterExpression): Query {
        this.queryRequest = Query.AddExpressionAttributesFromConditionToRequest(this.queryRequest, filterExpression);

        if (_.isString(this.queryRequest.KeyConditionExpression)) {
            this.queryRequest.KeyConditionExpression = `${this.queryRequest.KeyConditionExpression} AND (${filterExpression.statement})`;
        } else {
            this.queryRequest.KeyConditionExpression = `(${filterExpression.statement})`;
        }

        return this;
    }

    /*public addFilterExpression(filterExpression: IFilterExpression): Query {
        this.queryRequest = Query.AddExpressionAttributesFromConditionToRequest(this.queryRequest, filterExpression);

        if (_.isString(this.queryRequest.FilterExpression)) {
            this.queryRequest.FilterExpression = `${this.queryRequest.FilterExpression} AND (${filterExpression.statement})`;
        } else {
            this.queryRequest.FilterExpression = `(${filterExpression.statement})`;
        }

        return this;
    }*/

    public ascending(): Query {
        this.queryRequest.ScanIndexForward = true;
        return this;
    }

    public buildKey(): IFilterExpression {
        let key: string = this.table.getSchema().getHashKeyName();
        if (Query.IsUsingGlobalIndex(this)) {
            key = this.table.getSchema().getGlobalIndexes()[this.queryRequest.IndexName].hashKeyName;
        }

        return Expressions.BuildFilterExpression(key, ConditionExpressionOperators.EQUALS, Object.keys(this.queryRequest.ExpressionAttributeValues), this.hashKey);
    }

    public buildRequest(): QueryInput {
        return {...this.queryRequest, ...{TableName: this.table.getTableName()}};
    }

    public descending(): Query {
        this.queryRequest.ScanIndexForward = false;
        return this;
    }

    public async execute(): Promise<QueryOutput | any> {
        this.addKeyCondition(this.buildKey());

    }

    public filter(keyName: string): IScanQueryFilter {
        return Query.BuildQueryFilter(this, keyName);
    }

    public usingIndex(indexName: string): Query {
        this.queryRequest.IndexName = indexName;
        return this;
    }

    public setConsistentRead(consistentRead: boolean): Query {
        if (!Utils.IsBoolean(consistentRead)) {
            consistentRead = true;
        }

        this.queryRequest.ConsistentRead = consistentRead;
        return this;
    }

    public where(keyName: string): IKeyCondition {
        return Query.BuildKeyConditions(this, keyName);
    }

    private static IsUsingGlobalIndex(query: Query): boolean {
        return query.queryRequest.IndexName && !isNullOrUndefined(query.table.getSchema().getGlobalIndexes()[query.queryRequest.IndexName]);
    }

    private static BuildKeyConditions(query: Query, keyName: string): IKeyCondition {
        return {
            equals: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.EQUALS),
            eq: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.EQUALS),
            lte: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.LESS_THAN_OR_EQUALS),
            lt: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.LESS_THAN),
            gte: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.GREATER_THAN_OR_EQUALS),
            gt: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.GREATER_THAN),
            beginsWith: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.BEGINS_WITH),
            between: Query.MakeSingleKeyCondition(query, keyName, ConditionExpressionOperators.BETWEEN)
        };
    }



    private static MakeSingleKeyCondition(query: Query, keyName: string, ...operator: ConditionExpressionOperators[]): Query {
        return query.addKeyCondition(
            Expressions.BuildFilterExpression.apply(null,
                [keyName, operator.join(' '), Object.keys(query.queryRequest.ExpressionAttributeValues)].concat([].slice.call(arguments))));
    }

    private static AddExpressionAttributesFromConditionToRequest(request: QueryInput, filterExpression: IFilterExpression): QueryInput {
        const expressionAttributeNames = {...filterExpression.attributeNames, ...request.ExpressionAttributeNames}; // _.merge({}, condition.attributeNames, request.ExpressionAttributeNames);
        const expressionAttributeValues = {...filterExpression.attributeValues, ...request.ExpressionAttributeValues}; // _.merge({}, condition.attributeValues, request.ExpressionAttributeValues);

        if (_.isEmpty(expressionAttributeNames)) {
            request.ExpressionAttributeNames = expressionAttributeNames;
        }
        if (_.isEmpty(expressionAttributeValues)) {
            request.ExpressionAttributeValues = expressionAttributeValues;
        }

        return request;
    }
}
