import { FormulaRegEx, Row } from '../api/firebaseTypes';
import { Fn, FilterResultEnum } from '@chego/chego-api';
import { IConditions } from '../api/firebaseInterfaces';


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

const testConditions = (row: Row) => (results: any[], condition: Fn) => {
    const conditionResult = condition(row);
    if (conditionResult !== FilterResultEnum.Skipped) {
        results.push(conditionResult);
    }
    return results;
}

export const newConditions = (): IConditions => {
    const functions: Fn[] = [];
    return {
        add(...conditions: Fn[]): void {
            functions.push(...conditions);
        },
        test(row: Row): boolean {
            const formula: string = cleanFormula(functions.reduce(testConditions(row), []).join(''));
            const isValidOperation: boolean = /^.*[0-1].*$/.test(formula);
            const meetsConditions: boolean = isValidOperation ? Boolean(new Function(`return ${formula}`)()) : true;
            return meetsConditions;
        }
    }
}