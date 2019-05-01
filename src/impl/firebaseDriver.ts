import * as firebase from 'firebase';
import { validators } from './validators';
import { handles } from './handles';
import { pipelines } from './pipelines/pipelines';
import { newQueryContext } from './queryContext';
import { IQueryContext } from '../api/firebaseInterfaces';
import { CompileFunction } from '../api/firebaseTypes';
import { QuerySyntaxEnum, IQueryScheme, IQuerySchemeArray, IQuerySchemeElement, IDatabaseDriver, IQuery } from '@chego/chego-api';
import { isQueryScheme } from '@chego/chego-tools';



const isPrimaryCommand = (type:QuerySyntaxEnum) => type === QuerySyntaxEnum.Select 
|| type === QuerySyntaxEnum.Update 
|| type === QuerySyntaxEnum.Insert 
|| type === QuerySyntaxEnum.Delete;

const parseScheme = (scheme:IQueryScheme):IQueryContext[] => {
    let queryScope:IQueryContext[] = [];
    let queryContext:IQueryContext;
    const schemeArr:IQuerySchemeArray = scheme.toArray();
    schemeArr.map((element:IQuerySchemeElement, index:number) => {
        if(isPrimaryCommand(element.type)) {
            queryContext = newQueryContext(element.type);
            queryScope.unshift(queryContext);
        }
        let args:any = element.params;
        if(Array.isArray(element.params) && isQueryScheme(element.params[0])) {
            const subQueryScope:IQueryContext[] = parseScheme(element.params[0]);
            queryScope =  [...subQueryScope, ...queryScope];
            const subQuery:IQueryContext = subQueryScope[0];
            args = [subQuery.result];
        }
        if(validators.has(element.type)) {
            validators.get(element.type)(args);
        }
        if(handles.has(element.type)) {
            handles.get(element.type)({ queryContext, args, index });
        }
    });
    return queryScope;
};

const compileQuery = async (query:IQueryContext) => {
    const compile:CompileFunction = query && pipelines.get(query.type);
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

const onComplete = (result:any):Promise<any> => {
    firebase.database().goOffline();
    return Promise.resolve(result);
}

const onFailure = (error:Error):Promise<any> => {
    firebase.database().goOffline();
    throw error;
}
export const chegoFirebase = ():IDatabaseDriver => {
    let initialized:boolean = false;
    const driver = {
        initialize(config:any):IDatabaseDriver {
            firebase.initializeApp(config);
            initialized = true;
            return driver;
        },
        execute:(query:IQuery): Promise<any> => {
            if(!initialized) {
                throw new Error('Driver not initialized');
            }
            const queryScope:IQueryContext[] = parseScheme(query.scheme);

            if(!queryScope) {
                throw new Error('Empty QueryScope');
            }

            return queryScope.reduce((queries, query) => 
                queries.then(() => compileQuery(query))
            , Promise.resolve())
            .then(onComplete)
            .catch(onFailure)
        }
    }
    return driver;
}
