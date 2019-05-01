import { QuerySyntaxEnum, Fn } from '@chego/chego-api';
import { isLogicalOperatorScope } from '@chego/chego-tools';

export const validateWhere = (values:any[]) => {
    if(values.length === 0) {
        throw new Error('Empty WHERE clausule')
    }
    if(isLogicalOperatorScope(values[0])) {
        throw new Error('First condition key is logical operator')
    }
}

export const validators = new Map<QuerySyntaxEnum, Fn>([
    [QuerySyntaxEnum.Where, validateWhere]
]);