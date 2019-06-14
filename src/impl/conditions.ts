import { Fn, FilterResultEnum, ExpressionOrExpressionScope, QuerySyntaxEnum } from '@chego/chego-api';
import { FormulaRegEx, Row } from '@chego/chego-database-boilerplate';
import { IConditions } from '../api/firebaseInterfaces';
import { isExpressionScope, isFunction } from '@chego/chego-tools';
import { templates } from './templates';


const removeRoundBracketsIfJustNumber: FormulaRegEx = { pattern: /\(([0-1])\)/g, replacer: "$1" };
const removeOperatorIfFirst: FormulaRegEx = { pattern: /(\&{2}|\|{2})+(\)|$)/g, replacer: "$2" };
const removeOperatorIfLast: FormulaRegEx = { pattern: /(\(|^)+(\&{2}|\|{2})/g, replacer: "$1" };
const removeBlankRoundBrackets: FormulaRegEx = { pattern: /\(\)/g, replacer: "" };
const formulaExpressions = [removeRoundBracketsIfJustNumber, removeOperatorIfFirst, removeOperatorIfLast, removeBlankRoundBrackets];

const replaceWithExpression = (result: string, regex: FormulaRegEx) => result.replace(regex.pattern, regex.replacer);
const matchExpression = (formula: string) => (result: boolean, regex: FormulaRegEx) => formula.match(regex.pattern) ? true : result;
const cleanFormula = (formula: string): string => {
    const cleanedFormula: string = formulaExpressions.reduce(replaceWithExpression, formula);
    return formulaExpressions.reduce(matchExpression(cleanedFormula), false) ? cleanFormula(cleanedFormula) : cleanedFormula;
}

const testConditions = (row: Row) => (results: any[], condition: Fn<number> | string) => {
    if (isFunction(condition)) {
        const conditionResult:number = (<Fn<number>>condition)(row);
        if (conditionResult !== FilterResultEnum.Skipped) {
            results.push(conditionResult);
        }
    } else {
        results.push(condition);
    }
    return results;
}

const injectOperators = (template: string) => (list: Array<Fn<any>|string>, current: Fn<any>, i: number) => {
    if (Number.isInteger(i % 2)) {
        list.push(template);
    }
    list.push(current);
    return list;
}

const parseExpressionsToFunctions = (list: any[], current: ExpressionOrExpressionScope) => {
    const template = templates.get(current.type);
    if (isExpressionScope(current)) {
        const temp:string= <string>templates.get(current.type)();
        const conditions: any[] = current.expressions.reduce(parseExpressionsToFunctions, []).reduce(injectOperators(temp), []);
        list.push(
            templates.get(QuerySyntaxEnum.OpenParentheses)(),
            ...conditions,
            templates.get(QuerySyntaxEnum.CloseParentheses)()
        );
    } else {
        if (current.not) {
            list.push(templates.get(QuerySyntaxEnum.Not)());
        }
        list.push(template(current.property, current.value));
    }
    return list;
}

export const newConditions = (data: ExpressionOrExpressionScope[]): IConditions => {
    const conditions: any[] = data.reduce(parseExpressionsToFunctions, []);
    return {
        test(row: Row): boolean {
            const formula: string = cleanFormula(conditions.reduce(testConditions(row), []).join(''));
            const isValidOperation: boolean = /^.*[0-1].*$/.test(formula);
            const meetsConditions: boolean = isValidOperation ? Boolean(new Function(`return ${formula}`)()) : true;
            return meetsConditions;
        }
    }
}