import { Fn, FilterResultEnum, QuerySyntaxEnum } from '@chego/chego-api';
import { Row, Expressions, isExpressionScope } from '@chego/chego-database-boilerplate';
import { IConditions, Condition } from '../api/api';
import { templates } from './templates';

const buildFormula = (row: Row) => (results: Array<string|number>, condition: Condition) => {
    if(Array.isArray(condition)) {
        results.push('(',...condition.reduce(buildFormula(row),[]),')');
    } else {
        const conditionResult:number|string = condition(row);
        if (conditionResult !== FilterResultEnum.Skipped) {
            results.push(conditionResult);
        }
    }
    return results;
}

const injectOperator = (operatorTemplate:Fn<string>):Fn<any[]> => (list:Condition[], data: Condition[], i:number):Condition[] => {
    if(i > 0) {
        list.push(operatorTemplate)
    }
    list.push(data);
    return list;
}

const parseExpressionsToFunctions = (list:Condition[], data: Expressions):Condition[] => {
    if(Array.isArray(data)) {
        const conditions: Condition[] = data.reduce(parseExpressionsToFunctions,[]);
        list.push(conditions);
    } else if (isExpressionScope(data)) {
        const operatorTemplate:Fn<string> = <Fn<string>>templates.get(data.type)();
        const conditions:Condition[] = 
            data.expressions
            .reduce(parseExpressionsToFunctions, [])
            .reduce(injectOperator(operatorTemplate),[]);
        list.push(conditions);
    } else {
        if (data.not) {
            list.push(templates.get(QuerySyntaxEnum.Not)());
        }
        const template = templates.get(data.type);
        list.push(template(data.custom ? data.custom : data.property, data.value));
    }
    return list;
}

const dropSquareBrackets = (formulaParts:Condition[], current:any):Condition[] => {
    if(Array.isArray(current)) {
        if(current.length > 1) {
            formulaParts.push(...current.reduce(dropSquareBrackets,[]));
        } else {
            formulaParts.push(current[0]);
        }
    } else {
        formulaParts.push(current);
    }
    return formulaParts;
}

export const newConditions = (expressions: Expressions[]): IConditions => {
    const conditions: Condition[] = expressions.reduce(parseExpressionsToFunctions,[]).reduce(dropSquareBrackets,<any>[]);
    return {
        test(row: Row): boolean {
            const formula: string = conditions.reduce(buildFormula(row), []).join('');
            const isValidOperation: boolean = /^.*[0-1].*$/.test(formula);
            const meetsConditions: boolean = isValidOperation ? Boolean(new Function(`return ${formula}`)()) : true;
            return meetsConditions;
        }
    }
}