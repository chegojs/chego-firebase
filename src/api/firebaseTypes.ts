import { Table, QuerySyntaxEnum, Property } from '@chego/chego-api';

export type Row = { table:Table, key:string, content:any, scheme:string[] };
export type DataMap = Map<string, Row[]>;
export type JoinType = QuerySyntaxEnum.Join | QuerySyntaxEnum.FullJoin | QuerySyntaxEnum.LeftJoin | QuerySyntaxEnum.RightJoin;
export type Join = { type:JoinType, propertyA:Property, propertyB:Property };
export type FormulaRegEx = { pattern:RegExp, replacer: string }
export type CompileFunction = (ref:firebase.database.Reference, data:any) => Promise<any>;
export type OutputDataSnapshot = {[tableName:string]:any[]}
export type InputDataSnapshot = {[tableName:string]:object}