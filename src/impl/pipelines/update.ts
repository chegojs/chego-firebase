import * as firebase from 'Firebase';
import { filterQueryResultsIfRequired, executeQuery, convertMapToInputData } from "./select";
import { IQueryContext } from "../../api/firebaseInterfaces";
import { Row, DataMap } from '../../api/firebaseTypes';

const withErrorMessage = (errors:Map<string,Error>):string => {
            const message:string[] = [];
            errors.forEach((error: Error, table: string) => {
                message.push(`"${table}" table update failed: ${error.message}`)
            });
            return message.join('\n');
        }

export const updateContent = (newContent: any) => (data: DataMap): DataMap => {
    data.forEach((rows: Row[]) => {
        rows.map((row:Row)=>{
            for(const key of Object.keys(newContent)) {
                row.content[key] = newContent[key];
            }
        });
    });
    return data;
}

export const sendUpdatedContent = (ref: firebase.database.Reference) => (data:any) => (new Promise(async (resolve, reject) => {
    const errors:Map<string,Error> = new Map<string,Error>();
    for(const tableName of Object.keys(data)) {
        const table: firebase.database.Reference = ref.child(tableName);
        if(!table) {
            return reject(`Table "${tableName}" not found!`);
        }
        await table.update(data[tableName], (error:Error) => {
            if(error) {
                errors.set(tableName, error);
            }
        });
    }
    return (errors.size === 0) ? resolve(true) : reject(withErrorMessage(errors));
}));

export const runUpdatePipeline = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<any> =>
    new Promise((resolve, reject) => executeQuery(ref, queryContext)
        .then(filterQueryResultsIfRequired(queryContext))
        .then(updateContent(queryContext.data))
        .then(convertMapToInputData)
        .then(sendUpdatedContent(ref))
        .then(resolve)
        .catch(reject)
    );