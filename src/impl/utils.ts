import { DataMap, Row, JoinType, Join, Union } from '../api/firebaseTypes';
import { Property, SortingOrderEnum, IQueryResult } from '@chego/chego-api';
import { newProperty } from '@chego/chego-tools';
import { IJoinBuilder } from '../api/firebaseInterfaces';


export const createEmptyObject = (keys: string[]) => keys.reduce((acc: any, c: string) => { acc[c] = null; return acc; }, {});

export const newDataMap = (iterable?: any[]): DataMap => new Map<string, Row[]>(iterable);

export const newRow = ({ table = null, key = '', scheme = [], content = {} }: Row): Row => ({
    table, key, scheme, content
});

export const newJoin = (type:JoinType, property:Property): Join => ({type, propertyB:property, propertyA:newProperty({})});
export const newUnion = (distinct:boolean, data:IQueryResult): Union => ({distinct, data});

export const newJoinBuilder = (type:JoinType, property:Property): IJoinBuilder => {
    let propertyA:Property;

    const builder: IJoinBuilder = {
        withOn:(property:Property) : IJoinBuilder => {
            propertyA = property;
            return builder;
        },
        build:() => ({type, propertyB:property, propertyA})
    }
    return builder;
}

export const combineRows = (rowA: Row, rowB: Row): Row => {
    const content: any = Object.assign({}, rowA.content);

    for (const key in rowB.content) {
        if (content[key]) {
            content[`${rowB.table.name}.${key}`] = rowB.content[key];
        } else {
            content[key] = rowB.content[key];
        }
    }
    return { table: rowA.table, key: rowA.key, content, scheme: Object.keys(content) };
}

export const parseStringToSortingOrderEnum = (value: string): SortingOrderEnum => {
    const order: string = value && value.toUpperCase();
    return order
        ? (<any>SortingOrderEnum)[order] ? (<any>SortingOrderEnum)[order] : SortingOrderEnum.ASC
        : SortingOrderEnum.ASC;
}

export const isQueryResult = (value: any): value is IQueryResult => value && (<IQueryResult>value).getData !== undefined;
export const basicSort = (a: any, b: any, direction: SortingOrderEnum) => direction * ((a < b) ? -1 : (a > b) ? 1 : 0);

export const isNumeric = (n:any):boolean => !isNaN(parseFloat(n)) && isFinite(n);