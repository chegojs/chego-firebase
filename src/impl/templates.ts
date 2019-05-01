
import { Row } from '../api/firebaseTypes';
import { isQueryResult } from './utils';
import { AnyButFunction, Obj, QuerySyntaxTemplate, Property, FilterResultEnum, QuerySyntaxEnum } from '@chego/chego-api';
import { isAlias, isRowId } from '@chego/chego-tools';


export const getQueryResultValues = (data:AnyButFunction):AnyButFunction[] => {
    const results:AnyButFunction[] = [];
    Object.values(data).forEach((table:Obj) =>
        Object.values(table).forEach((row:Obj) => results.push(...Object.values(row)))
    );
    return results;
}

const parseValue = (value:AnyButFunction):AnyButFunction => {
    if(typeof value === 'string') {
        const dateInMilliseconds:number = Date.parse(value);
        if(!isNaN(dateInMilliseconds)) {
            return dateInMilliseconds;
        }
    }
    return value;
}

const isEq = (a:AnyButFunction,b:AnyButFunction):boolean => parseValue(a) === parseValue(b);
const isGt = (a:AnyButFunction,b:AnyButFunction):boolean => parseValue(a) > parseValue(b);
const isLt = (a:AnyButFunction,b:AnyButFunction):boolean => parseValue(a) < parseValue(b);
const isBetween = (a:AnyButFunction, min:AnyButFunction, max:AnyButFunction):boolean => 
    parseValue(a) >= parseValue(min) && parseValue(a) <= parseValue(max);

const runCondition = (condition:(...args:AnyButFunction[])=>boolean, ...values:any[]):boolean => {
    const data:AnyButFunction[] = [];
    values.forEach((value:any) => { 
        if(isQueryResult(value)) {
            const values:AnyButFunction[] = getQueryResultValues(value.getData());
            data.push(...values);
        } else {
            data.push(value);
        }
    });
    return condition(...data);
}

const select: QuerySyntaxTemplate = (property: Property) => (content:any) => (row: Row) => {
    if (row.table.name === property.table.name) {
        if (isAlias(property)) {
            content[property.alias] = row.content[property.name];
        }
        else if (isRowId(property)) {
            content[property.alias] = row.key;
        }
        else if(property.name === '*') {
            content = Object.assign(content, row.content);
        }
        else {
            content[property.name] = row.content[property.name];
        }
    }
    return content;
}

const eq:QuerySyntaxTemplate = (value:any) => (property:Property) => (row:Row) => 
    row.table.name === property.table.name
        ? isRowId(property)
            ? Number(runCondition(isEq, row.key, value))
            : Number(runCondition(isEq, row.content[property.name], value))
        :  FilterResultEnum.Skipped;

const isNull:QuerySyntaxTemplate = () => (property:Property) => eq(null)(property);

const gt: QuerySyntaxTemplate = (value: number | string) => (property:Property) => (row: Row) =>
    row.table.name === property.table.name
        ? isRowId(property)
        ? Number(runCondition(isGt, row.key, value))
        : Number(runCondition(isGt, row.content[property.name], value))
        : FilterResultEnum.Skipped;

const lt: QuerySyntaxTemplate = (value: number | string) => (property:Property) => (row: Row) =>
    row.table.name === property.table.name
        ? isRowId(property)
        ? Number(runCondition(isLt, row.key, value))
        : Number(runCondition(isLt, row.content[property.name], value))
        : FilterResultEnum.Skipped;

const between: QuerySyntaxTemplate = (min: number, max: number) => (property:Property) => (row: Row) =>
    row.table.name === property.table.name
        ? isRowId(property)
        ? Number(runCondition(isBetween, row.key, min, max))
        : Number(runCondition(isBetween, row.content[property.name], min, max))
        : FilterResultEnum.Skipped;

const and:QuerySyntaxTemplate = () => () => () => '&&';
const or:QuerySyntaxTemplate = () => () => () => '||';
const not:QuerySyntaxTemplate = () => () => () => '!';

const openParentheses:QuerySyntaxTemplate = () => () => () => '(';
const closeParentheses:QuerySyntaxTemplate = () => () => () => ')';

export const templates:Map<QuerySyntaxEnum, QuerySyntaxTemplate> = new Map<QuerySyntaxEnum, QuerySyntaxTemplate>([
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
]);