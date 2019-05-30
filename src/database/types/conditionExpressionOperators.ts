export enum ConditionExpressionOperators {
    ATTRIBUTE_EXISTS        = 'attribute_exists',
    ATTRIBUTE_NOT_EXISTS    = 'attribute_not_exists',
    ATTRIBUTE_TYPE          = 'attribute_type',
    CONTAINS                = 'contains',
    BEGINS_WITH             = 'begins_with',
    SIZE                    = 'size',
    EQUALS                  = '=',
    NOT_EQUALS              = '<>',
    LESS_THAN               = '<',
    LESS_THAN_OR_EQUALS     = '<=',
    GREATER_THAN            = '>',
    GREATER_THAN_OR_EQUALS  = '>=',
    BETWEEN                 = 'BETWEEN',
    IN                      = 'IN',
    AND                     = 'AND',
    OR                      = 'OR',
    NOT                     = 'NOT'
}
