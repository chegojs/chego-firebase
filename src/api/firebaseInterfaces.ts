import {QuerySyntaxEnum, IQueryResult, Table, Limit, SortingData, FunctionData} from '@chego/chego-api';
import { Row, Join } from './firebaseTypes';

export interface IConditions {
    update(type:QuerySyntaxEnum, args?:any):void;
    test(row:Row):boolean;
}

export interface IQueryContext {
    type:QuerySyntaxEnum;
    result:IQueryResult;
    data:any[];
    tables:Table[];
    joins:Join[];
    limit:Limit;
    orderBy:SortingData[];
    groupBy:SortingData[];
    functions:FunctionData[];
    conditions:IConditions;
}