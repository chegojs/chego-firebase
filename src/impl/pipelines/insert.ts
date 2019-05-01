import { IQueryContext } from "../../api/firebaseInterfaces";
import * as firebase from 'Firebase';

const withErrorMessage = (errors: Map<string, Error>): string => {
    const message: string[] = [];
    errors.forEach((error: Error, table: string) => {
        message.push(`Adding an entry to the "${table}" failed: ${error.message}`)
    });
    return message.join('\n');
}

export const runInsertPipeline = async (ref: firebase.database.Reference, queryContext: IQueryContext): Promise<any> => new Promise(async (resolve, reject) => {
    const errors: Map<string, Error> = new Map<string, Error>();
    for (const table of queryContext.tables) {
        for (const entry of queryContext.data) {
            await ref.push(entry, (error: Error) => {
                if (error) {
                    errors.set(table.name, error);
                }
            });
        }
    }
    return (errors.size === 0) ? resolve(true) : reject(withErrorMessage(errors));
});