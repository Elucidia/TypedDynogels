import {SecondaryIndexTypes} from './secondaryIndexTypes';
import {ProjectionTypes} from './projectionTypes';

export interface ISecondaryIndex {
    name: string;
    type: SecondaryIndexTypes;
    hashKeyName?: string;
    rangeKeyName?: string;
    projection?: ProjectionTypes;
    readCapacity?: number;
    writeCapacity?: number;
}
