import { QuerySyntaxEnum, IQueryResult, Table, Limit, SortingData, FunctionData, Fn, Property } from '@chego/chego-api';
import { Row, Join } from './firebaseTypes';

export interface IConditions {
    add(...conditions: Fn[]):void;
    test(row:Row):boolean;
}

export interface IJoinBuilder {
    withOn(property:Property): IJoinBuilder;
    build(): Join;
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
    unions:any[];
    limit:Limit;
    orderBy:SortingData[];
    groupBy:SortingData[];
    functions:FunctionData[];
    conditions:IConditions;
}