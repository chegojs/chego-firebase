
import { Row } from '../api/firebaseTypes';
import { isQueryResult } from './utils';
import { AnyButFunction, Obj, QuerySyntaxTemplate, Property, FilterResultEnum, QuerySyntaxEnum } from '@chego/chego-api';
import { isAlias, isRowId } from '@chego/chego-tools';


export const getQueryResultValues = (data: AnyButFunction): AnyButFunction[] => {
    const results: AnyButFunction[] = [];
    Object.values(data).forEach((table: Obj) =>
        Object.values(table).forEach((row: Obj) => results.push(...Object.values(row)))
    );
    return results;
}

const parseValue = (value: AnyButFunction): AnyButFunction => {
    if (typeof value === 'string') {
        const dateInMilliseconds: number = Date.parse(value);
        if (!isNaN(dateInMilliseconds)) {
            return dateInMilliseconds;
        }
    }
    return value;
}

const isIn = (a: AnyButFunction, ...values: AnyButFunction[]): boolean => { 
    const expression = parseValue(a);
     for(const value of values) {
         if(expression === parseValue(value)) {
             return true;
         }
     }
     return false;
};
const isEq = (a: AnyButFunction, b: AnyButFunction): boolean =>
    typeof a === 'object' && typeof b === 'object'
        ? JSON.stringify(a) === JSON.stringify(b)
        : parseValue(a) === parseValue(b);

const isGt = (a: AnyButFunction, b: AnyButFunction): boolean => parseValue(a) > parseValue(b);
const isLt = (a: AnyButFunction, b: AnyButFunction): boolean => parseValue(a) < parseValue(b);
const isBetween = (a: AnyButFunction, min: AnyButFunction, max: AnyButFunction): boolean =>
    parseValue(a) >= parseValue(min) && parseValue(a) <= parseValue(max);

const isLikeString = (a: string, b: string): boolean =>
    new RegExp(`^${b.replace(/(?<!\\)\%/g, '.*').replace(/(?<!\\)\_/g, '.')}$`, 'g').test(a);

const runCondition = (condition: (...args: AnyButFunction[]) => boolean, ...values: any[]): boolean => {
    const data: AnyButFunction[] = [];
    values.forEach((value: any) => {
        if (isQueryResult(value)) {
            const values: AnyButFunction[] = getQueryResultValues(value.getData());
            data.push(...values);
        } else {
            data.push(value);
        }
    });
    return condition(...data);
}

const select: QuerySyntaxTemplate = (property: Property) => (content: any) => (row: Row) => {
    if (row.table.name === property.table.name) {
        if (isAlias(property)) {
            content[property.alias] = row.content[property.name];
        }
        else if (isRowId(property)) {
            content[property.alias] = row.key;
        }
        else if (property.name === '*') {
            content = Object.assign(content, row.content);
        }
        else {
            content[property.name] = row.content[property.name];
        }
    }
    return content;
}

const conditionTemplate = (condition: (...args: AnyButFunction[]) => boolean, row: Row, property: Property, ...values: any[]) =>
    row.table.name === property.table.name
        ? isRowId(property)
            ? Number(runCondition(condition, row.key, ...values))
            : Number(runCondition(condition, row.content[property.name], ...values))
        : FilterResultEnum.Skipped;

const whereIn: QuerySyntaxTemplate = (...values: any[]) => (property: Property) => (row: Row) =>
        conditionTemplate(isIn, row, property, ...values);

const eq: QuerySyntaxTemplate = (value: any) => (property: Property) => (row: Row) =>
    conditionTemplate(isEq, row, property, value);

const isNull: QuerySyntaxTemplate = () => (property: Property) => eq(null)(property);

const gt: QuerySyntaxTemplate = (value: number | string) => (property: Property) => (row: Row) =>
    conditionTemplate(isGt, row, property, value);

const lt: QuerySyntaxTemplate = (value: number | string) => (property: Property) => (row: Row) =>
    conditionTemplate(isLt, row, property, value);

const between: QuerySyntaxTemplate = (min: number, max: number) => (property: Property) => (row: Row) =>
    conditionTemplate(isBetween, row, property, min, max);

const like: QuerySyntaxTemplate = (value: any) => (property: Property) => (row: Row) =>
    typeof value === 'string'
        ? conditionTemplate(isLikeString, row, property, value)
        : conditionTemplate(isEq, row, property, value);

const exists: QuerySyntaxTemplate = (value: any) => () => () => {
    const data = value.getData();
    return Array.isArray(data) ? data.length : FilterResultEnum.Skipped;
}

const and: QuerySyntaxTemplate = () => () => () => '&&';
const or: QuerySyntaxTemplate = () => () => () => '||';
const not: QuerySyntaxTemplate = () => () => () => '!';

const openParentheses: QuerySyntaxTemplate = () => () => () => '(';
const closeParentheses: QuerySyntaxTemplate = () => () => () => ')';

export const templates: Map<QuerySyntaxEnum, QuerySyntaxTemplate> = new Map<QuerySyntaxEnum, QuerySyntaxTemplate>([
    [QuerySyntaxEnum.Select, select],
    [QuerySyntaxEnum.EQ, eq],
    [QuerySyntaxEnum.Null, isNull],
    [QuerySyntaxEnum.GT, gt],
    [QuerySyntaxEnum.LT, lt],
    [QuerySyntaxEnum.And, and],
    [QuerySyntaxEnum.Or, or],
    [QuerySyntaxEnum.Not, not],
    [QuerySyntaxEnum.OpenParentheses, openParentheses],
    [QuerySyntaxEnum.CloseParentheses, closeParentheses],
    [QuerySyntaxEnum.Between, between],
    [QuerySyntaxEnum.Like, like],
    [QuerySyntaxEnum.In, whereIn],
    [QuerySyntaxEnum.Exists, exists],
]);