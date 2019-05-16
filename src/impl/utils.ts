import { DataMap, Row } from '../api/firebaseTypes';
import { SortingOrderEnum, IQueryResult } from '@chego/chego-api';


export const createEmptyObject = (keys: string[]) => keys.reduce((acc: any, c: string) => { acc[c] = null; return acc; }, {});

export const newDataMap = (iterable?: any[]): DataMap => new Map<string, Row[]>(iterable);

export const newRow = ({ table = null, key = '', scheme = [], content = {} }: Row): Row => ({
    table, key, scheme, content
});

export const parseStringToSortingOrderEnum = (value: string): SortingOrderEnum => {
    const order: string = value && value.toUpperCase();
    return order
        ? (<any>SortingOrderEnum)[order] ? (<any>SortingOrderEnum)[order] : SortingOrderEnum.ASC
        : SortingOrderEnum.ASC;
}

export const isQueryResult = (value: any): value is IQueryResult => value && (<IQueryResult>value).getData !== undefined;
export const basicSort = (a: any, b: any, direction: SortingOrderEnum) => direction * ((a < b) ? -1 : (a > b) ? 1 : 0);

export const isNumeric = (n:any):boolean => !isNaN(parseFloat(n)) && isFinite(n);