import * as firebase from 'Firebase';
import { IQueryContext } from '@chego/chego-database-boilerplate';
import { Obj, ItemWithCustomId, Fn } from '@chego/chego-api';
import { isItemWithCustomId } from '@chego/chego-tools';

const withErrorMessage = (errors: Map<string, Error>): string => {
    const message: string[] = [];
    errors.forEach((error: Error, table: string) => {
        message.push(`Adding an entry to the "${table}" failed: ${error.message}`)
    });
    return message.join('\n');
}

const insertWithGeneratedId = async (ref: firebase.database.Reference, table:string, entry:Obj, callback:(error: Error)=>void) => 
    ref.child(table).push(entry, callback);
const insertWithCustomId = async (ref: firebase.database.Reference, table:string, entry:ItemWithCustomId, callback:(error: Error)=>void) => 
    ref.child(table).update({[entry.id]:entry.item}, callback);

const collectErrorIfOccurred = (table:string, errors:Map<string, Error>) => (error: Error) => {
    if (error) {
        errors.set(table, error);
    }
}

const getInsertionFunction = (entry:any):Fn<Promise<any>> => isItemWithCustomId(entry) ? insertWithCustomId : insertWithGeneratedId;

export const runInsertPipeline = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<any> => new Promise(async (resolve, reject) => {
    const errors: Map<string, Error> = new Map<string, Error>();
    for (const table of queryContext.tables) {
        for (const entry of queryContext.data) {
            const insert = getInsertionFunction(entry);
            await insert(ref, table.name, entry, collectErrorIfOccurred(table.name, errors));
        }
    }
    return (errors.size === 0) ? resolve(true) : reject(withErrorMessage(errors));
});