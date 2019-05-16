import { QuerySyntaxEnum, Fn } from '@chego/chego-api';
import { isLogicalOperatorScope, isProperty, isTable } from '@chego/chego-tools';

const validateWhere = (values: any[]) => {
    if (values.length === 0) {
        throw new Error('Empty WHERE clausule')
    }
    if (isLogicalOperatorScope(values[0])) {
        throw new Error('First condition key is logical operator')
    }
    noArgsValidation(values);
}

const noArgsValidation = (...args: any[]) => {
    if (args.length === 0) {
        throw new Error('No arguments!');
    }
}

const validateSet = (...args: any[]) => {
    if (args.length > 1) {
        throw new Error('Too many arguments');
    }
    noArgsValidation(...args);
}

const validateEQ = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateLT = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateGT = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateLimit = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateBetween = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateFrom = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateExists = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateHaving = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateUnion = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateOrderBy = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateTo = (...args: any[]) => {
    noArgsValidation(...args);
}
const validateUpdate = (...args: any[]) => {
    noArgsValidation(...args);
}
const validateIn = (...args: any[]) => {
    noArgsValidation(...args);
}
const validateInsert = (...args: any[]) => {
    noArgsValidation(...args);
}

const validateJoin = (...args: any[]): void => {
    if (!isTable(args[0])) {
        throw new Error(`given argument is not a Property object`);
    }
    noArgsValidation(...args);
}

const validateUsing = (...args: any[]): void => {
    if (!isProperty(args[0])) {
        throw new Error(`given argument is not a Property object`);
    }
    noArgsValidation(...args);
}

const validateOn = (...args: any[]) => {
    for(const arg of args) {
        if (!isProperty(arg)) {
            throw new Error(`given argument is not a Property object`);
        }
    }
    noArgsValidation(...args);
}

export const validators = new Map<QuerySyntaxEnum, Fn>([
    [QuerySyntaxEnum.Where, validateWhere],
    [QuerySyntaxEnum.Set, validateSet],
    [QuerySyntaxEnum.Join, validateJoin],
    [QuerySyntaxEnum.FullJoin, validateJoin],
    [QuerySyntaxEnum.LeftJoin, validateJoin],
    [QuerySyntaxEnum.RightJoin, validateJoin],
    [QuerySyntaxEnum.On, validateOn],
    [QuerySyntaxEnum.EQ, validateEQ],
    [QuerySyntaxEnum.LT, validateLT],
    [QuerySyntaxEnum.GT, validateGT],
    [QuerySyntaxEnum.Limit, validateLimit],
    [QuerySyntaxEnum.Between, validateBetween],
    [QuerySyntaxEnum.From, validateFrom],
    [QuerySyntaxEnum.Exists, validateExists],
    [QuerySyntaxEnum.Having, validateHaving],
    [QuerySyntaxEnum.Union, validateUnion],
    [QuerySyntaxEnum.OrderBy, validateOrderBy],
    [QuerySyntaxEnum.To, validateTo],
    [QuerySyntaxEnum.Update, validateUpdate],
    [QuerySyntaxEnum.In, validateIn],
    [QuerySyntaxEnum.Insert, validateInsert],
    [QuerySyntaxEnum.Using, validateUsing],
]);