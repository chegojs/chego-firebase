import { joinTablesIfRequired } from '../joins';
import { orderResultsIfRequired } from '../orderBy';
import { applyMySQLFunctionsIfAny } from '../mySQLFunctions';
import { groupResultsIfRequired } from '../groupBy';
import { templates } from '../templates';
import * as firebase from 'Firebase';
import { Limit, Property, Table, QuerySyntaxEnum } from '@chego/chego-api';
import { applyUnionsIfAny, storeOnlyUniqueEntriesIfRequired } from '../unions';
import { newRow, IQueryContext, newDataMap, Row, DataMap, InputDataSnapshot, OutputDataSnapshot } from '@chego/chego-database-boilerplate';
import { newConditions } from '../conditions';

export const parseRowsToArray = (result: any[], row: Row): any[] => (result.push(Object.assign({}, row.content)), result);
export const parseRowsToObject = (result: any, row: Row): any => (Object.assign(result, { [row.key]: row.content }), result);

export const useLimitToFirst = (limit: Limit): boolean => limit.count ? limit.count >= 0 : limit.offsetOrCount >= 0;

export const useLimitToLast = (limit: Limit): boolean => limit.count ? limit.count < 0 : limit.offsetOrCount < 0;

export const shouldFilterRowContent = (properties: Property[]): boolean => properties && properties.length > 0 && properties[0].name !== '*';

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
    const conditions = newConditions(queryContext.expressions);
    queryResult.forEach((rows: Row[], tableName: string) => {
        tableRows = rows.filter((row: Row) => {
            if (conditions.test(row)) {
                if (shouldFilterRowContent(queryContext.data) && queryContext.type === QuerySyntaxEnum.Select) {
                    row.content = queryContext.data.reduce((content: any, property: Property) => select(property, content, row), {});
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

export const spliceQueryResultsIfRequired = (limit: Limit) => (data: any): any => {
    if (limit) {
        const range: number[] = limit.count
            ? [limit.offsetOrCount, limit.count]
            : limit.offsetOrCount < 0
                ? [limit.offsetOrCount]
                : [0, limit.offsetOrCount];

        for (const table of Object.keys(data)) {
            data[table] = data[table].slice(...range)
        }
    }
    return data;
}



export const getTableContent = async (ref: firebase.database.Reference, table: Table, limit?: Limit): Promise<any> =>
    new Promise(resolve =>
        ref.child(table.name).once('value', (snapshot: firebase.database.DataSnapshot) => resolve(snapshot.val()))
    );

export const onlyTemporaryProperties = (tempProps: Map<string, Property[]>, current: Property) => {
    if (current.temporary) {
        if (tempProps.has(current.table.name)) {
            tempProps.get(current.table.name).push(current);
        } else {
            tempProps.set(current.table.name, [current]);
        }
    }
    return tempProps;
}

export const removeTemporaryProperties = (tempPropertiesMap: Map<string, Property[]>) => (rows: Row[], table: string, map: DataMap): void => {
    const tempProps: Property[] = tempPropertiesMap.get(table);
    if (tempProps) {
        for (const prop of tempProps) {
            for (const row of rows) {
                delete row.content[prop.name];
            }
        }
    }
}

export const removeTemporaryPropertiesIfAny = (queryContext: IQueryContext) => (data: DataMap): DataMap => {
    if (queryContext.data.length) {
        const tempProperties: Map<string, Property[]> = new Map<string, Property[]>();
        queryContext.data.reduce(onlyTemporaryProperties, tempProperties);
        data.forEach(removeTemporaryProperties(tempProperties));
    }
    return data;
}

export const runSelectPipeline = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<any> =>
    new Promise((resolve, reject) => executeQuery(ref, queryContext)
        .then(joinTablesIfRequired(ref, queryContext))
        .then(storeOnlyUniqueEntriesIfRequired(queryContext))
        .then(applyUnionsIfAny(queryContext))
        .then(filterQueryResultsIfRequired(queryContext))
        .then(applyMySQLFunctionsIfAny(queryContext))
        .then(removeTemporaryPropertiesIfAny(queryContext))
        .then(convertMapToOutputData)
        .then(groupResultsIfRequired(queryContext))
        .then(orderResultsIfRequired(queryContext))
        .then(spliceQueryResultsIfRequired(queryContext.limit))
        .then(resolve)
        .catch(reject)
    );