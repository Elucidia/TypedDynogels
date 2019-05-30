import {Query} from '../query';

export interface IKeyCondition {
    equals:     Query;
    eq:         Query;
    lte:        Query;
    lt:         Query;
    gte:        Query;
    gt:         Query;
    beginsWith: Query;
    between:    Query;
}
