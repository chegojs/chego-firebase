import { createEmptyObject, combineRows, newRow, isNumeric } from './utils';
import { getTableContent, parseDataSnapshotToRows } from './pipelines/select';
import { IQueryContext } from '../api/firebaseInterfaces';
import { Row, Join, DataMap } from '../api/firebaseTypes';
import { AnyButFunction, QuerySyntaxEnum, Fn } from '@chego/chego-api';
import { isRowId, getLabel } from '@chego/chego-tools';

const shouldJoinRows = (rowA:Row, rowB:Row, join:Join):boolean => {
    const a:AnyButFunction = (isRowId(join.propertyA)) ? rowA.key : rowA.content[getLabel(join.propertyA)];
    const b:AnyButFunction = (isRowId(join.propertyB)) ? rowB.key : rowB.content[getLabel(join.propertyB)];
    return (isNumeric(a) && isNumeric(b)) ? Number(a) === Number(b) : a === b;
}

const doSideJoin = (primaryRows: Row[], secondaryRows: Row[], join: Join) => {
    const combinedRows: Row[] = [];
    const initRow: Row = secondaryRows[0];
    const scheme: string[] = Object.keys(initRow.content);
    const emptyRow: Row = newRow({
        table: initRow.table,
        key: initRow.key,
        scheme,
        content: createEmptyObject(scheme)
    });
    let rowToAssign: Row;
    primaryRows.forEach((rowA: Row) => {
        rowToAssign = emptyRow;
        secondaryRows.forEach((rowB: Row) => {
            if (shouldJoinRows(rowA,rowB,join)) {
                rowToAssign = rowB;
            }
        });
        combinedRows.push(combineRows(rowA, rowToAssign));
    });
    return combinedRows;
}

const doLeftJoin = (rowsA: Row[], rowsB: Row[], join: Join): any => doSideJoin(rowsA, rowsB, join);
const doRightJoin = (rowsA: Row[], rowsB: Row[], join: Join): any => doSideJoin(rowsB, rowsA, join);

const doJoin = (rowsA: Row[], rowsB: Row[], join: Join): Row[] => {
    const combinedRows: Row[] = [];

    rowsA.forEach((rowA: Row) => {
        rowsB.forEach((rowB: Row) => {
            if (shouldJoinRows(rowA,rowB,join)) {
                combinedRows.push(combineRows(rowA, rowB));
            }
        });
    });
    return combinedRows;
}

const doFullJoin = (rowsA: Row[], rowsB: Row[], join: Join): Row[] => {
    const leftJoin: Row[] = doSideJoin(rowsA, rowsB, join);
    const rightJoin: Row[] = doSideJoin(rowsB, rowsA, join);

    return [...leftJoin, ...rightJoin];
}

const joinFunctions: Map<QuerySyntaxEnum, Fn> = new Map<QuerySyntaxEnum, Fn>([
    [QuerySyntaxEnum.Join, doJoin],
    [QuerySyntaxEnum.FullJoin, doFullJoin],
    [QuerySyntaxEnum.LeftJoin, doLeftJoin],
    [QuerySyntaxEnum.RightJoin, doRightJoin]
]);

const mergeTableB2TableA = (join: Join, results: DataMap) => (tableBContent: any) => {
    const rowsA: Row[] = results.get(join.propertyA.table.name);
    const rowsB: Row[] = parseDataSnapshotToRows(join.propertyB.table, tableBContent);
    const joinFn: Fn = joinFunctions.get(join.type);
    const combinedRows: Row[] = joinFn ? joinFn(rowsA, rowsB, join) : [];
    results.set(join.propertyA.table.name, combinedRows);
    return results;
}

export const joinTablesIfRequired = (ref: firebase.database.Reference, queryContext: IQueryContext) => async (results: DataMap): Promise<DataMap> => {
    for (const join of queryContext.joins) {
        await getTableContent(ref, join.propertyB.table, queryContext.limit)
            .then(mergeTableB2TableA(join, results));
    }
    return Promise.resolve(results);
}
