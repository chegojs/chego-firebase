import { Row, DataMap } from '../../api/firebaseTypes';
import { IQueryContext } from "../../api/firebaseInterfaces";
import * as firebase from 'Firebase';
import { executeQuery, filterQueryResultsIfRequired, convertMapToInputData } from './select';
import { sendUpdatedContent } from './update';
import { Property } from '@chego/chego-api';

const nullifyRows = (rows: Row[], row: Row): Row[] => [...rows, Object.assign(row, { content: null })];

const nullifyRowsContent = (keysToRemove: Property[]) => (rows: Row[], row: Row): Row[] => {
    for (const key of keysToRemove) {
        if(row.scheme.indexOf(key.name) > -1) {
            row.content[key.name] = null;
        }
    }
    return [...rows, row];
}

export const containsSelectAllShorthand = (properties: Property[]) => properties.reduce((result:boolean,property:Property) => {
    if(property.name === '*') {
        result = true;
    }
    return result;
},false);

export const shouldNullifyEntireRows = (properties: Property[]) => properties.length === 0 || containsSelectAllShorthand(properties);

export const nullifyData = (properties: Property[]) => (data: DataMap): DataMap => {
    const action: (rows: Row[], row: Row) => Row[] = shouldNullifyEntireRows(properties) ? nullifyRows : nullifyRowsContent(properties);
    data.forEach((rows: Row[], table: string) => {
        const nullifiedData: Row[] = rows.reduce(action, []);
        data.set(table, nullifiedData);
    });
    return data;
}

export const runDeletePipeline = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<any> =>
    new Promise((resolve, reject) => executeQuery(ref, queryContext)
        .then(filterQueryResultsIfRequired(queryContext))
        .then(nullifyData(queryContext.data))
        .then(convertMapToInputData)
        .then(sendUpdatedContent(ref))
        .then(resolve)
        .catch(reject)
    );