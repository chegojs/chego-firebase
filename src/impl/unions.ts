import { Union } from './../api/firebaseTypes';
import { Property, Table, IQueryResult } from '@chego/chego-api';
import { Row, DataMap, OutputDataSnapshot } from '../api/firebaseTypes';
import { newRow } from './utils';
import { getLabel } from '@chego/chego-tools';
import { IQueryContext } from '../api/firebaseInterfaces';

const replaceScheme = (scheme: string[], table: Table) => (list: any[], row: any): Row[] => {
    const rowScheme: string[] = Object.keys(row)
    const content: any = scheme.reduce((result: any, prop: string, i: number) => {
        const key: string = rowScheme[i];
        return Object.assign(result, { [prop]: row[key] });
    }, {});
    list.push(newRow({ table, scheme, content, key: '' }));
    return list;
}

const getPropertyLabel = (list: string[], property: Property) => (list.push(getLabel(property)), list);

const getDefaultScheme = (queryContext: IQueryContext, queryResult: DataMap): string[] => {
    const defaultTable: Table = queryContext.tables[0];
    return (queryContext.data.length)
        ? queryContext.data.reduce(getPropertyLabel, [])
        : queryResult.get(getLabel(defaultTable))[0].scheme;
}

const isInResults = (toCompare: any, results: DataMap): boolean => {
    let result: boolean = false;
    results.forEach((rows: Row[]) => {
        result = rows.reduce((is: boolean, row: Row) => {
            Object.values(row.content).forEach((value: any) => {
                if (value === toCompare) {
                    is = true;
                }
            });
            return is;
        }, false);
    });
    return result;
}

const selectDistinctValues = (results: DataMap, res: any, union: Union) => {
    const data: any = union.data.getData();
    for (const table of Object.keys(data)) {
        const rows:any[] = data[table];
        rows.forEach((row:any, i:number) => {
            Object.keys(row).forEach((key: string) => {
                if (isInResults(row[key], results)) {
                    delete row[key];
                }
            });
            if (Object.keys(row).length === 0) {
                rows.splice(i,1)
            }
        });
    }
    return Object.assign(res, data);
}

const combineResults = (results: DataMap) => (res: any, union: Union) =>
    union.distinct
        ? selectDistinctValues(results, res, union)
        : Object.assign(res, union.data.getData());

const checkIfDistinct = (result: boolean, union: Union) => union.distinct ? true : result;

const newUniqueValues = (scheme: string[]) => {
    const entries: any[] = scheme.reduce((map: any[], key: string) => (map.push([key, []]), map), []);
    return new Map<string, any[]>(entries);
}

const filterUniqueResults = (scheme:string[], uniqueValuesMap: Map<string, any[]>) => (row: Row) => {
    for (const key of scheme) {
        const uniqueValues: any[] = uniqueValuesMap.get(key);
        const value: any = row.content[key];
        if (uniqueValues.indexOf(row.content[key]) === -1) {
            uniqueValues.push(value);
            return true;
        } else {
            return false;
        }
    }
}

export const storeOnlyUniqueEntriesIfRequired = (queryContext: IQueryContext) => (queryResult: DataMap): DataMap => {
    const storeOnlyUnique: boolean = queryContext.unions.reduce(checkIfDistinct, false);
    if (storeOnlyUnique) {
        const defaultTable: Table = queryContext.tables[0];
        const defaultLabel: string = getLabel(defaultTable);
        const scheme: string[] = (queryContext.data.length)
            ? queryContext.data.reduce(getPropertyLabel, [])
            : queryResult.get(defaultLabel)[0].scheme;
        const rows: Row[] = queryResult.get(defaultLabel).filter(filterUniqueResults(scheme, newUniqueValues(scheme)));
        queryResult.set(defaultLabel, rows);
    }
    return queryResult;
}

export const applyUnionsIfAny = (queryContext: IQueryContext) => (queryResult: DataMap): DataMap => {
    if (queryContext.unions.length) {
        const defaultTable: Table = queryContext.tables[0];
        const defaultLabel: string = getLabel(defaultTable);
        const scheme: string[] = getDefaultScheme(queryContext, queryResult);
        const union: OutputDataSnapshot = queryContext.unions.reduce(combineResults(queryResult), {});
        for (const table of Object.keys(union)) {
            const rows: Row[] = union[table].reduce(replaceScheme(scheme, defaultTable), []);
            if (queryResult.has(defaultLabel)) {
                queryResult.get(defaultLabel).push(...rows);
            } else {
                queryResult.set(defaultLabel, rows);
            }
        }
    }
    return queryResult;
}

export const newUnion = (distinct:boolean, data:IQueryResult): Union => ({distinct, data});