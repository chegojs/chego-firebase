import * as firebase from 'firebase';
import { pipelines } from './pipelines/pipelines';
import { IDatabaseDriver, IQuery } from '@chego/chego-api';
import { newExecutor } from '@chego/chego-database-boilerplate'

export const chegoFirebase = (): IDatabaseDriver => {
    let initialized: boolean = false;
    const driver: IDatabaseDriver = {
        initialize(config: any): IDatabaseDriver {
            firebase.initializeApp(config);
            initialized = true;
            return driver;
        },
        execute: async (queries: IQuery[]): Promise<any> => new Promise(async (resolve, reject) => {
            if (!initialized) {
                throw new Error('Driver not initialized');
            }
            
            const ref: firebase.database.Reference = firebase.app().database().ref();
            return newExecutor()
                .withDBRef(ref)
                .withPipelines(pipelines)
                .execute(queries)
                .then(resolve)
                .catch(reject);
        }),
        connect: (): Promise<any> => new Promise((resolve) => {
            firebase.database().goOnline();
            resolve();
        }),
        disconnect: (): Promise<any> => new Promise((resolve) => {
            firebase.database().goOffline();
            resolve();
        })
    }
    return driver;
}
