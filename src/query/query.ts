import * as _ from 'lodash';
import {ConditionExpressionOperators} from '../database/types/conditionExpressionOperators';
import {QueryInput} from 'aws-sdk/clients/dynamodb';
import {IQueryCondition} from './types/iQueryCondition';
import {Expressions} from '../expression/expressions';
import {IKeyCondition} from './types/iKeyCondition';

export class Query {
    private queryRequest: QueryInput;

    public addKeyCondition(condition: IQueryCondition): Query {
        this.queryRequest = Query.AddExpressionAttributesFromConditionToRequest(this.queryRequest, condition);

        if (_.isString(this.queryRequest.KeyConditionExpression)) {
            this.queryRequest.KeyConditionExpression = `${this.queryRequest.KeyConditionExpression} AND (${condition.statement})`;
        } else {
            this.queryRequest.KeyConditionExpression = `(${condition.statement})`;
        }

        return this;
    }

    public addFilterExpression(condition: IQueryCondition): Query {

    }

    private static KeyCondition(query: Query, keyName: string): IKeyCondition {
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

    private static MakeSingleKeyCondition(query: Query, keyName: string, operator: ConditionExpressionOperators) {
        return query.addKeyCondition(
            Expressions.BuildFilterExpression.apply(null,
                [keyName, operator, Object.keys(query.queryRequest.ExpressionAttributeValues)].concat([].slice.call(arguments))));
    }

    private static AddExpressionAttributesFromConditionToRequest(request: QueryInput, condition: IQueryCondition): QueryInput {
        const expressionAttributeNames = {...condition.attributeNames, ...request.ExpressionAttributeNames}; // _.merge({}, condition.attributeNames, request.ExpressionAttributeNames);
        const expressionAttributeValues = {...condition.attributeValues, ...request.ExpressionAttributeValues}; // _.merge({}, condition.attributeValues, request.ExpressionAttributeValues);

        if (_.isEmpty(expressionAttributeNames)) {
            request.ExpressionAttributeNames = expressionAttributeNames;
        }
        if (_.isEmpty(expressionAttributeValues)) {
            request.ExpressionAttributeValues = expressionAttributeValues;
        }

        return request;
    }
}
