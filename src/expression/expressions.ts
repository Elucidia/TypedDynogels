import * as _ from 'lodash';
import {Actions} from './types/actions';
import {Schema} from '../schema/schema';
import {Item} from '../database/item';
import {Utils} from '../util/utils';
import {ISerializedUpdateExpression} from './types/iSerializedUpdateExpression';
import {Serializer} from '../serializer/serializer';
import {ConditionExpressionOperators} from '../database/types/conditionExpressionOperators';
import {IFilterExpression} from './types/iFilterExpression';

export class Expressions {
    // aka regexMap
    private static readonly actionWordsRegex: Object = {
        'SET': /SET\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
        'ADD': /ADD\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
        'REMOVE': /REMOVE\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
        'DELETE': /DELETE\s*(.+?)\s*(SET|ADD|REMOVE|DELETE|$)/,
    };

    private static readonly splitOperandsRegex: RegExp = /\s*(?![^(]*\)),\s*/;

    public static Parse(stringToParse: string): Object {
        // original _.reduce(internals.actionWords, (result, actionWord) => {
        return Object.keys(Actions).reduce((accumulator: any, currentActionWord: string) => {
            accumulator[currentActionWord] = Expressions.Match(currentActionWord as Actions, stringToParse);
            return accumulator;
        }, {});
    }

    public static SerializeUpdateExpression(schema: Schema, item: Item) {
        const datatypes = schema.getDatatypes();
        const strippedPrimaryKeysItem = Utils.OmitPrimaryKeys(schema, item);
        const baseSerializedExpression: ISerializedUpdateExpression = {
            expressions: {},
            attributeNames: {},
            values: {}
        };

        baseSerializedExpression.expressions = Object.keys(Actions).reduce((accumulator: string, currentActionWord: string) => {
            accumulator[currentActionWord] = [];
            return accumulator;
        });

        const serializedExpression: ISerializedUpdateExpression = strippedPrimaryKeysItem.reduce((accumulator: ISerializedUpdateExpression, value: any, key: any) => {
            const valueKey: string = `:${key}`;
            const nameKey: string = `#${key}`;

            accumulator.attributeNames[nameKey] = key;
            if (value === null || (Utils.IsString(value) && value.length === 0)) {
                accumulator.expressions[Actions.REMOVE].push(nameKey);
            } else if (Utils.IsPlainObject(value)) {
                if (value.$add) {
                    accumulator.expressions[Actions.ADD].push(`${nameKey} ${valueKey}`);
                    accumulator.values[valueKey] = Serializer.SerializeAttribute(value.$add, datatypes[key]);
                } else if (value.$del) {
                    accumulator.expressions[Actions.DELETE].push(`${nameKey} ${valueKey}`);
                    accumulator.values[valueKey] = Serializer.SerializeAttribute(value.$del, datatypes[key]);
                }
            } else {
                accumulator.expressions[Actions.SET].push(`${nameKey} ${valueKey}`);
                accumulator.values[valueKey] = Serializer.SerializeAttribute(value, datatypes[key]);
            }
            return accumulator;
        }, baseSerializedExpression);

        return serializedExpression;
    }

    public static Stringify(expressions: any): string {
        return _.reduce(expressions, (accumulator: any, currentValue: any, key: string) => {
            if (currentValue.length > 0) {
                if (Array.isArray(currentValue)) {
                    accumulator.push(`${key} ${currentValue.join(', ')}`);
                } else {
                    accumulator.push(`${key} ${currentValue}`);
                }
            }
            return accumulator;
        }, []).join(' ');
    }

    public static BuildFilterExpression(key: string, operator: ConditionExpressionOperators, existingValueNames: string[], value1: any, value2: any = null): IFilterExpression {
        if (operator === ConditionExpressionOperators.IN) {
            return Expressions.BuildInFilterExpression(key, existingValueNames, value1);
        }

        let formattedValue1 = Expressions.FormatAttributeValue(value1);
        const formattedValue2 = Expressions.FormatAttributeValue(value2);

        if (operator === ConditionExpressionOperators.ATTRIBUTE_EXISTS) {
            if (!formattedValue1) {
                operator = ConditionExpressionOperators.ATTRIBUTE_NOT_EXISTS;
            }
            formattedValue1 = null;
        }

        const keys: string[] = key.split('.');
        const path: string = `#${keys.join('.#').replace(/[^\W.#]/g, '')}`;
        const value1Name: string = Expressions.GetUniqueAttributeValueName(key, existingValueNames);
        const value2Name: string = Expressions.GetUniqueAttributeValueName(key, [value1Name].concat(existingValueNames));

        let statement: string = '';
        if (Expressions.IsFunctionOperator(operator)) {
            if (value1) {
                statement = `${operator}(${path}, ${value1Name})`;
            } else {
                statement = `${operator}(${path})`;
            }
        } else if (operator === ConditionExpressionOperators.BETWEEN) {
            statement = `${path} BETWEEN ${value1Name} AND ${value2Name}`;
        } else {
            statement = [path, operator, value1Name].join(' ');
        }

        const attributeValues = {};

        if (value1) {
            attributeValues[value1Name] = value1;
        }

        if (value2) {
            attributeValues[value2Name] = value2;
        }

        const attributeNames = {};
        keys.forEach((key) => {
            attributeNames[`#${key.replace(/[^\w.]/g, '')}`] = key;
        });

        return {
            attributeNames: attributeNames,
            statement: statement,
            attributeValues: attributeValues
        };
    }

    private static Match(actionWord: Actions, stringToMatch: string): string[] {
        const match: RegExpExecArray = Expressions.actionWordsRegex[actionWord].exec(stringToMatch);
        if (match && match.length >= 2) {
            return match[1].split(Expressions.splitOperandsRegex);
        } else {
            return null;
        }
    }

    // TODO is it usefull?
    private static FormatAttributeValue(value: any): any {
        if (_.isDate(value)) {
            return value.toISOString();
        }

        return value;
    }

    private static BuildInFilterExpression(key: string, existingValueNames: string[], values: string[]): IFilterExpression {
        const path: string = `#${key}`;
        const attributeNames = {};
        attributeNames[path.split('.')[0]] = key.split('.')[0];

        const attributeValues = values.reduce((accumulator: any, currentValue: string) => {
            const existing: string[] = Object.keys(accumulator).concat(existingValueNames);
            const uniqueAttributeValueName: string = Expressions.GetUniqueAttributeValueName(key, existing);
            accumulator[uniqueAttributeValueName] = Expressions.FormatAttributeValue(currentValue);
            return accumulator;
        }, {});

        return {
            attributeNames: attributeNames,
            statement: `${path} IN (${Object.keys(attributeValues)})`,
            attributeValues: attributeValues
        };
    }

    private static GetUniqueAttributeValueName(key: string, existingValueNames: string[]): string {
        const cleanedKey: string = key.replace(/\./g, '_').replace(/\W/g, '');
        let potentialName: string = `:${cleanedKey}`;
        let index: number = 1;

        while (existingValueNames.includes(potentialName)) {
            index++;
            potentialName = `:${cleanedKey}_${index}`;
        }
        return potentialName;
    }

    private static IsFunctionOperator(operator: string): boolean {
        return ['attribute_exists', 'attribute_not_exists', 'attribute_type', 'begins_with', 'contains', 'NOT contains', 'size'].includes(operator);
    }
}
