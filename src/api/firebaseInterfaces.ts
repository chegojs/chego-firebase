import { QuerySyntaxEnum, IQueryResult, Table, Limit, SortingData, FunctionData, Fn } from '@chego/chego-api';
import { Row, Join } from './firebaseTypes';

export interface IConditions {
    add(...conditions: Fn[]):void;
    test(row:Row):boolean;
}

export interface IQueryContextBuilder {
    with(type: QuerySyntaxEnum, params: any[]): void;
    build(): IQueryContext;
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