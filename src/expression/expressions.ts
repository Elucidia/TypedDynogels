import {Actions} from './types/actions';
import {Schema} from '../schema/schema';
import {Item} from '../database/item';
import {Utils} from '../util/utils';
import {ISerializedUpdateExpression} from './types/iSerializedUpdateExpression';
import {Serializer} from '../serializer/serializer';

export class Expressions {
    // aka regexMap
    private static readonly actionWordsRegex: Object = {
        'SET': /SET\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
        'ADD': /ADD\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
        'REMOVE': /REMOVE\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
        'DELETE': /DELETE\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
    };

    private static readonly splitOperandsRegex: RegExp = /\s*(?![^(]*\)),\s*/;

    public static Parse(stringToParse: string): string {
        // original _.reduce(internals.actionWords, (result, actionWord) => {
        return Object.keys(Actions).reduce((accumulator: string, currentActionWord: string) => {
            accumulator[currentActionWord] = Expressions.Match(currentActionWord as Actions, stringToParse);
            return accumulator;
        });
    }

    public static SerializeUpdateExpression(schema: Schema, item: Item) {
        const datatypes = schema.getDatatypes();
        const strippedPrimaryKeysItem = Utils.OmitPrimaryKeys(schema, item);
        const memo: ISerializedUpdateExpression = {
            expressions: {},
            attributeNames: {},
            values: {}
        };

        memo.expressions = Object.keys(Actions).reduce((accumulator: string, currentActionWord: string) => {
            accumulator[currentActionWord] = [];
            return accumulator;
        });

        const serializedExpression: ISerializedUpdateExpression = strippedPrimaryKeysItem.reduce((accumulator: ISerializedUpdateExpression, value: any, key: any) => {
            const valueKey: string = `:${key}`;
            const nameKey: string = `#${key}`;

            accumulator.attributeNames[nameKey] = key;
            if (value === null || (Utils.IsString(value) && value.length === 0)) {
                accumulator.expressions[Actions.REMOVE].push(nameKey);
            } else if (Utils.IsPlainObject(value) && value.$add) {
                accumulator.expressions[Actions.ADD].push(`${nameKey} ${valueKey}`);
                accumulator.values[valueKey] = Serializer.SerializeAttribute(value.$add, datatypes[key]);
            }
        }, memo);
    }

    private static Match(actionWord: Actions, stringToMatch: string): string[] {
        const match: RegExpExecArray = Expressions.actionWordsRegex[actionWord].exec(stringToMatch);
        if (match && match.length >= 2) {
            return match[1].split(Expressions.splitOperandsRegex);
        } else {
            return null;
        }
    }
}
