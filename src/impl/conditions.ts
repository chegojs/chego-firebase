import { FormulaRegEx, Row } from '../api/firebaseTypes';
import { QuerySyntaxEnum, Property, Fn, PropertyOrLogicalOperatorScope, FilterResultEnum } from '@chego/chego-api';
import { templates } from './templates';
import { isLogicalOperatorScope, newLogicalOperatorScope } from '@chego/chego-tools';
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

export const getFunctionTemplate = (type: QuerySyntaxEnum, property?: Property, ...values: any[]): Fn => {
    const template = templates.get(type);
    if (template) {
        return template(...values)(property);
    }
    throw new Error(`No template for ${QuerySyntaxEnum[type]}`);
}

const isEqualityOrRelationalOperator = (type: QuerySyntaxEnum): boolean => type === QuerySyntaxEnum.EQ || type === QuerySyntaxEnum.GT || type === QuerySyntaxEnum.LT;

export const buildStatementFunctions = (type: QuerySyntaxEnum, keychain?: PropertyOrLogicalOperatorScope[], values?: any[]): Fn[] => {
    const functions: Fn[] = [];
    const keychainLength: number = keychain && keychain.length || 0;
    const valuesLength: number = values && values.length || 0;
    // &&, ||, !, (, )
    if (keychainLength === 0) {
        return [getFunctionTemplate(type)];
    }
    for (let i = 0; i < keychainLength; i++) {
        const data: PropertyOrLogicalOperatorScope = keychain[i];
        if (isLogicalOperatorScope(data)) {
            functions.push(getFunctionTemplate(data.type), ...buildStatementFunctions(type, data.properties, values));
        } else {
            // eq, lt, gt
            if (isEqualityOrRelationalOperator(type)) {
                for (let q = 0; q < valuesLength; q++) {
                    if (isLogicalOperatorScope(values[q])) {
                        functions.push(getFunctionTemplate(values[q].type), ...buildStatementFunctions(type, [data], values[q].properties));
                    } else {
                        functions.push(getFunctionTemplate(type, data, values[q]));
                    }
                }
            } else {
                // &&, || glue
                if (i > 0) {
                    functions.push(getFunctionTemplate(data.type));
                }
                functions.push(Array.isArray(values) 
                    ? getFunctionTemplate(type, data, ...values)
                    : getFunctionTemplate(type, data, values)
                );
            }
        }
    }

    return keychainLength > 1
        ? [
            getFunctionTemplate(QuerySyntaxEnum.OpenParentheses),
            ...functions,
            getFunctionTemplate(QuerySyntaxEnum.CloseParentheses)
        ]
        : functions;
};

export const testConditions = (row: Row) => (results: any[], condition: Fn) => {
    const conditionResult = condition(row);
    if (conditionResult !== FilterResultEnum.Skipped) {
        results.push(conditionResult);
    }
    return results;
}

const isAndOr = (type:QuerySyntaxEnum):boolean => type === QuerySyntaxEnum.And || type === QuerySyntaxEnum.Or

export const newConditions = (): IConditions => {
    const history: QuerySyntaxEnum[] = [];
    const functions: Fn[] = [];
    let keychain: PropertyOrLogicalOperatorScope[] = [];

    return {
        update(type: QuerySyntaxEnum, args?: any[]): void {
            const lastType: QuerySyntaxEnum = history[history.length - 1];
            const penultimateType: QuerySyntaxEnum = history[history.length - 2];
            if (type === QuerySyntaxEnum.Where) {
                if (isAndOr(lastType) && penultimateType === QuerySyntaxEnum.Where) {
                    const lastKey: PropertyOrLogicalOperatorScope = keychain[keychain.length - 1];
                    if (!isLogicalOperatorScope(lastKey)) {
                        throw new Error(`Key ${lastKey} should be LogialOperatorScope type!`)
                    }
                    lastKey.properties.push(...args);
                } else {
                    keychain = [...args];
                }
            } else {
                if (isAndOr(type) && lastType === QuerySyntaxEnum.Where) {
                    keychain.push(newLogicalOperatorScope(type));
                } else {
                    functions.push(...buildStatementFunctions(type, keychain.slice(), args));
                }
            }
            history.push(type);
        },
        test(row: Row): boolean {
            const formula: string = cleanFormula(functions.reduce(testConditions(row), []).join(''));
            const isValidOperation: boolean = /^.*[0-1].*$/.test(formula);
            const meetsConditions: boolean = isValidOperation ? Boolean(new Function(`return ${formula}`)()) : true;
            return meetsConditions;
        }
    }
}
