import {ConditionExpressionOperators} from '../database/types/conditionExpressionOperators';
import {AbstractScanQuery} from './abstractScanQuery';
import {IScanQueryFilter} from './types/iScanQueryFilter';
import {Expressions} from '../expression/expressions';

export class ScanQueryUtils {
    // replace internals.keyCondition
    public static BuildScanQueryFilter(scanQuery: AbstractScanQuery, keyName: string): IScanQueryFilter {
        return {
            equals: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.EQUALS),
            eq: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.EQUALS),
            ne: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.NOT_EQUALS),
            lte: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.LESS_THAN_OR_EQUALS),
            lt: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.LESS_THAN),
            gte: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.GREATER_THAN_OR_EQUALS),
            gt: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.GREATER_THAN),
            null: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.ATTRIBUTE_NOT_EXISTS),
            exists: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.ATTRIBUTE_EXISTS),
            contains: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.CONTAINS),
            notContains: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.NOT, ConditionExpressionOperators.CONTAINS),
            in: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.IN),
            beginsWith: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.BEGINS_WITH),
            between: ScanQueryUtils.MakeSingleQueryFilter(scanQuery, keyName, ConditionExpressionOperators.BETWEEN)
        };
    }

    private static MakeSingleQueryFilter(scanQuery: AbstractScanQuery, keyName: string, ...operator: ConditionExpressionOperators[]): AbstractScanQuery {
        return scanQuery.addExpressionAttributesFromFilterExpressionToRequest(
            Expressions.BuildFilterExpression.apply(null, [keyName, operator.join(' '), Object.keys(scanQuery.getRequest().ExpressionAttributeValues)].concat([].slice.call(arguments)))
        );
    }
}
