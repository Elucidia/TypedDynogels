import {AbstractScanQuery} from './abstractScanQuery';
import {Table} from '../database/table';
import {ScanInput, ScanOutput} from 'aws-sdk/clients/dynamodb';
import {IKeyCondition} from './types/iKeyCondition';
import {ScanQueryUtils} from './scanQueryUtils';
import {IScanQueryFilter} from './types/iScanQueryFilter';

export class Scan extends AbstractScanQuery {
    constructor(table: Table) {
        super(table);
    }

    public setSegments(segment: number, totalSegments: number): Scan {
        (this.request as ScanInput).Segment = segment;
        (this.request as ScanInput).TotalSegments = totalSegments;
        return this;
    }

    public where(keyName: string): IScanQueryFilter {
        return ScanQueryUtils.BuildScanQueryFilter(this, keyName);
    }

    public async execute(): Promise<ScanOutput | any> {

    }
}
