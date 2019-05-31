import {AbstractScanQuery} from '../abstractScanQuery';

export interface IScanQueryFilter {
    equals:         AbstractScanQuery;
    eq:             AbstractScanQuery;
    ne:             AbstractScanQuery;
    lte:            AbstractScanQuery;
    lt:             AbstractScanQuery;
    gte:            AbstractScanQuery;
    gt:             AbstractScanQuery;
    null:           AbstractScanQuery;
    exists:         AbstractScanQuery;
    contains:       AbstractScanQuery;
    notContains:    AbstractScanQuery;
    in:             AbstractScanQuery;
    beginsWith:     AbstractScanQuery;
    between:        AbstractScanQuery;
}
