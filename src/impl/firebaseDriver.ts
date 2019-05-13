import { IQueryContextBuilder } from './../api/firebaseInterfaces';
import * as firebase from 'firebase';
import { validators } from './validators';
import { pipelines } from './pipelines/pipelines';
import { IQueryContext } from '../api/firebaseInterfaces';
import { CompileFunction } from '../api/firebaseTypes';
import { IQueryScheme, IQuerySchemeArray, IQuerySchemeElement, IDatabaseDriver, IQuery, Fn } from '@chego/chego-api';
import { isQueryScheme } from '@chego/chego-tools';
import { newQueryContextBuilder } from './contextBuilder';

const parseScheme = (scheme: IQueryScheme): IQueryContext[] => {
    let queryScope: IQueryContext[] = [];
    const contextBuilder:IQueryContextBuilder = newQueryContextBuilder();
    const schemeArr: IQuerySchemeArray = scheme.toArray();

    schemeArr.map((element: IQuerySchemeElement) => {
        let args: any = element.params;
        if (Array.isArray(element.params) && isQueryScheme(element.params[0])) {
            const subQueryScope: IQueryContext[] = parseScheme(element.params[0]);
            queryScope = [...subQueryScope, ...queryScope];
            const subQuery: IQueryContext = subQueryScope[0];
            args = [subQuery.result];
        }
        if (validators.has(element.type)) {
            validators.get(element.type)(...args);
        }
        contextBuilder.with(element.type, args);
    });
    queryScope.push(contextBuilder.build());
    return queryScope;
};

const compileQuery = async (query: IQueryContext) => {
    const compile: CompileFunction = query && pipelines.get(query.type);
    const ref: firebase.database.Reference = firebase.app().database().ref();

    if (compile) {
        return compile(ref, query)
            .then(result => {
                query.result.setData(result);
                return result;
            })
            .catch(error => { throw error; });
    }
    throw new Error('compilator not found');
}

const buildQueryScope = (query: IQuery) => () => {
    const queryScope: IQueryContext[] = parseScheme(query.scheme);

    if (!queryScope) {
        throw new Error('Empty QueryScope');
    }
    
    return Promise.resolve(queryScope);
}
const executeQueryScope = (queryScope: IQueryContext[]) => 
    queryScope.reduce((queries, query) =>
        queries.then(() => compileQuery(query)), Promise.resolve())

export const chegoFirebase = (): IDatabaseDriver => {
    let initialized: boolean = false;
    const driver: IDatabaseDriver = {
        initialize(config: any): IDatabaseDriver {
            firebase.initializeApp(config);
            initialized = true;
            return driver;
        },
        execute: async (queries: IQuery[]): Promise<any> => new Promise((resolve, reject) => {
            if (!initialized) {
                throw new Error('Driver not initialized');
            }

            return queries.reduce((queries, query) =>
                queries.then(buildQueryScope(query)).then(executeQueryScope),
                Promise.resolve())
                .then(resolve)
                .catch(reject)
        }),
        connect(callback:Fn) {
            firebase.database().goOnline();
            if(callback) {
                callback();
            }
        },
        disconnect(callback:Fn) {
            firebase.database().goOffline();
            if(callback) {
                callback();
            }
        },
    }
    return driver;
}
