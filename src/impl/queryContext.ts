
import { IQueryContext } from '../api/firebaseInterfaces';
import { newConditions } from './conditions';
import { IQueryResult, AnyButFunction, QuerySyntaxEnum } from '@chego/chego-api';

const newQueryResult = ():IQueryResult => {
    let result:AnyButFunction;

    return {
        setData(value:AnyButFunction):void {
            result = value;
        },
        getData():AnyButFunction {
            return result;
        }
    }
}

export const newQueryContext = (type:QuerySyntaxEnum):IQueryContext => ({
    type,
    result:newQueryResult(),
    data:[],
    tables:[],
    joins:[],
    limit:null,
    orderBy:[],
    groupBy:[],
    functions:[],
    conditions:newConditions()
});