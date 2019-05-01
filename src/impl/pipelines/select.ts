import { OutputDataSnapshot, InputDataSnapshot } from '../../api/firebaseTypes';
import { joinTablesIfRequired } from '../joins';
import { orderResultsIfRequired } from '../orderBy';
import { applyMySQLFunctionsIfAny } from '../mySQLFunctions';
import { groupResultsIfRequired } from '../groupBy';
import { templates } from '../templates';
import { IQueryContext } from '../../api/firebaseInterfaces';
import { Row, DataMap } from '../../api/firebaseTypes';
import { newDataMap, newRow } from '../utils';
import * as firebase from 'Firebase';
import { Limit, Property, Table, QuerySyntaxEnum } from '@chego/chego-api';

export const parseRowsToArray = (result: any[], row: Row): any[] => (result.push(Object.assign({}, row.content)), result);
export const parseRowsToObject = (result: any, row: Row): any => (Object.assign(result, { [row.key]: row.content }), result);

export const useLimitToFirst = (limit: Limit): boolean => limit.count ? limit.count >= 0 : limit.offsetOrCount >= 0;

export const useLimitToLast = (limit: Limit): boolean => limit.count ? limit.count < 0 : limit.offsetOrCount < 0;

export const shouldFilterRowContent = (properties: Property[]): boolean => properties && properties.length > 0 && properties[0].name !== '*';

export const isWithinLimits = (index: number, limit: Limit): boolean =>
    limit
        ? limit.count
            ? index >= limit.offsetOrCount && index < limit.count
            : index < limit.offsetOrCount
        : true;

export const parseDataSnapshotToRows = (table: Table, data: any): Row[] => {
    const rows: Row[] = [];
    let content: any;
    for (const key in data) {
        content = data[key];
        rows.push(newRow({
            table,
            key,
            scheme: Object.keys(content),
            content
        }))
    }
    return rows;
}

export const executeQuery = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<DataMap> => {
    const map: DataMap = newDataMap();
    for (const table of queryContext.tables) {
        const tableContent: any = await getTableContent(ref, table, queryContext.limit);
        map.set(table.name, parseDataSnapshotToRows(table, tableContent));
    }
    return Promise.resolve(map);
}

export const filterQueryResultsIfRequired = (queryContext: IQueryContext) => (queryResult: DataMap): DataMap => {
    const parsedResult: DataMap = newDataMap();
    const select = templates.get(QuerySyntaxEnum.Select);
    let tableRows: Row[];
    let withinLimits: boolean;

    queryResult.forEach((rows: Row[], tableName: string) => {
        tableRows = rows.filter((row: Row, index: number) => {
            withinLimits = isWithinLimits(index, queryContext.limit);
            if (queryContext.conditions.test(row) && withinLimits) {
                if (shouldFilterRowContent(queryContext.data) && queryContext.type === QuerySyntaxEnum.Select) {
                    row.content = queryContext.data.reduce((content: any, property: Property) => select(property)(content)(row), {});
                }
                return true;
            }
            return false;
        });
        parsedResult.set(tableName, tableRows);
    });
    return parsedResult;
}

export const convertMapToInputData = (tablesMap: DataMap): InputDataSnapshot => {
    const results: InputDataSnapshot = {};
    tablesMap.forEach((rows: Row[], table: string) => {
        Object.assign(results, { [table]: rows.reduce(parseRowsToObject, {}) });
    }, results);
    return results;
}
export const convertMapToOutputData = (tablesMap: DataMap): OutputDataSnapshot => {
    const results: OutputDataSnapshot = {};
    tablesMap.forEach((rows: Row[], table: string) => {
        Object.assign(results, { [table]: rows.reduce(parseRowsToArray, []) });
    }, results);
    return results;
}

export const getTableContent = async (ref: firebase.database.Reference, table: Table, limit?: Limit): Promise<any> =>
    new Promise(resolve => {
        const query: firebase.database.Reference = ref.child(table.name);

        if (limit) {
            if (useLimitToFirst(limit)) {
                query.limitToFirst(limit.count ? limit.count + limit.offsetOrCount : limit.offsetOrCount);
            }
            else if (useLimitToLast(limit)) {
                query.limitToLast(-1 * (limit.count ? limit.count : limit.offsetOrCount));
            }
        }
        query.once('value', (snapshot: firebase.database.DataSnapshot) => resolve(snapshot.val()));
    });

export const runSelectPipeline = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<any> =>
    new Promise((resolve, reject) => executeQuery(ref, queryContext)
        .then(joinTablesIfRequired(ref, queryContext))
        .then(filterQueryResultsIfRequired(queryContext))
        .then(applyMySQLFunctionsIfAny(queryContext))
        .then(convertMapToOutputData)
        .then(groupResultsIfRequired(queryContext))
        .then(orderResultsIfRequired(queryContext))
        .then(resolve)
        .catch(reject)
    );