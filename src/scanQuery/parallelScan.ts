import * as _ from 'lodash';
import {Scan} from './scan';
import {Table} from '../database/table';
import {ScanOutput} from 'aws-sdk/clients/dynamodb';
import {Readable} from 'stream';
import {AbstractScanQuery} from './abstractScanQuery';

export class ParallelScan extends Scan {
    private totalSegments: number;

    constructor(table: Table, totalSegments: number) {
        super(table);
        this.totalSegments = totalSegments;
    }

    public async execute(): Promise<ScanOutput | any> {
        const combinedStream: Readable = new Readable({objectMode: true});

        const scanPromises = [];
        for (let i = 0; i < this.totalSegments; ++i) {
            let scan: Scan = new Scan(this.table);
            scan.setRequest(_.cloneDeep(super.request));
            scan = scan.setSegments(i, this.totalSegments).setLoadAll();
        }
    }
}
