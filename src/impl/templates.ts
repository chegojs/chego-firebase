import { AnyButFunction, Obj, Property, FilterResultEnum, QuerySyntaxEnum, Fn, CustomCondition, Between } from '@chego/chego-api';
import { isAlias, isRowId, isCustomCondition } from '@chego/chego-tools';
import { isQueryResult, Row } from '@chego/chego-database-boilerplate';

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
    for (const value of values) {
        if (expression === parseValue(value)) {
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

const select: Fn<any> = (property: Property, content: any, row: Row) => {
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

const conditionTemplate = (condition: (...args: AnyButFunction[]) => boolean, row: Row, property: Property | CustomCondition, ...values: any[]) => {
    if (row.table.name === property.table.name) {
        if (isCustomCondition(property)) {
            return Number(runCondition(condition, property.condition(row.content), ...values))
        } else {
            return isRowId(property)
                ? Number(runCondition(condition, row.key, ...values))
                : Number(runCondition(condition, row.content[property.name], ...values));
        }
    }
}

const whereIn: Fn<Fn<number>> = (property: Property | CustomCondition, ...values: any[]) => (row: Row) =>
    conditionTemplate(isIn, row, property, ...values);

const eq: Fn<Fn<number>> = (property: Property | CustomCondition, value: any) => (row: Row) =>
    conditionTemplate(isEq, row, property, value);

const isNull: Fn<Fn<number>> = (property: Property | CustomCondition) => eq(property, null);

const gt: Fn<Fn<number>> = (property: Property | CustomCondition, value: number | string) => (row: Row) =>
    conditionTemplate(isGt, row, property, value);

const lt: Fn<Fn<number>> = (property: Property | CustomCondition, value: number | string) => (row: Row) =>
    conditionTemplate(isLt, row, property, value);

const between: Fn<Fn<number>> = (property: Property | CustomCondition, between: Between) => (row: Row) =>
    conditionTemplate(isBetween, row, property, between.min, between.max);

const like: Fn<Fn<number>> = (property: Property | CustomCondition, value: any) => (row: Row) =>
    typeof value === 'string'
        ? conditionTemplate(isLikeString, row, property, value)
        : conditionTemplate(isEq, row, property, value);

const exists: Fn<Fn<number>> = (property: Property, value: any) => () => {
    const data = value.getData();
    return Array.isArray(data) ? data.length : FilterResultEnum.Skipped;
}

const and: Fn<Fn<string>> = () => () => '&&';
const or: Fn<Fn<string>> = () => () => '||';
const not: Fn<Fn<string>> = () => () => '!';

const openParentheses: Fn<Fn<string>> = () => () => '(';
const closeParentheses: Fn<Fn<string>> = () => () => ')';

export const templates: Map<QuerySyntaxEnum, Fn<Fn<number>> | Fn<Fn<string>>> = new Map<QuerySyntaxEnum, Fn<Fn<number>> | Fn<Fn<string>>>([
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