import { newJoin, parseStringToSortingOrderEnum } from './utils';
import { IQueryContext } from '../api/firebaseInterfaces';
import { Join, JoinType } from '../api/firebaseTypes';
import { Table, FunctionData, Property, PropertyOrLogicalOperatorScope, QuerySyntaxEnum, SortingData, Fn } from '@chego/chego-api';
import { isAliasString, newTable, isAlias, isMySQLFunction, isProperty, isLogicalOperatorScope, combineReducers, newLimit, newSortingData, parseStringToProperty, mergePropertiesWithLogicalAnd } from '@chego/chego-tools';

const ifStringThenParseToTable = (tables:Table[], table:any) => {
    if(typeof table === 'string') {
        if(isAliasString(table)) {
            const data:string[] = table.replace(/ {1,}/g, ' ').split(' AS ');
            return tables.concat(newTable(data[0], data[1]));
        }
        return tables.concat(newTable(table));
    }
    return tables.concat(table);
}

const ifAliasThenParseToTable = (tables:Table[], table:any) => (isAlias(table)) 
        ? tables.concat(newTable(table.name, table.alias))
        : tables.concat(table);

const handleMySqlFunctions = (mySqlFunctions:FunctionData[]) => (keys:Property[], data:Property | FunctionData) => {
    if(isMySQLFunction(data)) {
        mySqlFunctions.push(data);
        return keys.concat(data.properties);
    }
    return keys.concat(data);
}

const ifEmptyTableSetDefault = (defaultTable:Table) => (list:PropertyOrLogicalOperatorScope[], data:PropertyOrLogicalOperatorScope) => {
    if(isProperty(data) && !data.table) {
        data.table = defaultTable;
    }
    return list.concat(data);
}

const ifLogicalOperatorScopeThenParseItsKeys = (defaultTable:Table) => (list:PropertyOrLogicalOperatorScope[], data:PropertyOrLogicalOperatorScope) => {
    if(isLogicalOperatorScope(data)) {
        data.properties.reduce(
            combineReducers(
                ifEmptyTableSetDefault(defaultTable),
                ifLogicalOperatorScopeThenParseItsKeys(defaultTable),
            ),[]);
    }
    return list.concat(data);
}


const handleStatement = (type:QuerySyntaxEnum, queryContext:IQueryContext, args?:any[]) => {
    queryContext.conditions.update(type, args);
}

export const handleAnd = ({queryContext}:{queryContext:IQueryContext}) => 
    handleStatement(QuerySyntaxEnum.And, queryContext);

export const handleBetween = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => 
    handleStatement(QuerySyntaxEnum.Between, queryContext, args);

export const handleDelete = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => {
    queryContext.data = args;
}

export const handleEqualTo = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => 
    handleStatement(QuerySyntaxEnum.EQ, queryContext, args);

export const handleFrom = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => {
    queryContext.tables = args.reduce(combineReducers(
        ifStringThenParseToTable,
        ifAliasThenParseToTable
    ),[]);
    queryContext.data.reduce(ifEmptyTableSetDefault(queryContext.tables[0]),[]);
}

export const handleGT = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => 
    handleStatement(QuerySyntaxEnum.GT, queryContext, args[0]);

export const handleInsert = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => {
    queryContext.data = args;
}

export const handleLike = () => {
    console.log(`need to implement "like"`);
}

export const handleLimit = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => {
    queryContext.limit = newLimit(args[0], args[1]);
}

export const handleLT = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => 
    handleStatement(QuerySyntaxEnum.LT, queryContext, args[0]);

export const handleNot = ({queryContext}:{queryContext:IQueryContext}) => 
    handleStatement(QuerySyntaxEnum.Not, queryContext);

export const handleNull = ({queryContext}:{queryContext:IQueryContext}) => 
    handleStatement(QuerySyntaxEnum.Null, queryContext);

export const handleOr = ({queryContext}:{queryContext:IQueryContext}) => 
    handleStatement(QuerySyntaxEnum.Or, queryContext);

const parseStringToSortingData = (defaultTable:Table) => (data:SortingData[], entry:string):SortingData[] => {
    const entryParts = entry.replace(/ {1,}/g, ' ').split(' ');

    if(entryParts.length > 2) {
        throw new Error(`There is something wrong with this order by "${entry}"`);
    }
    data.push(newSortingData(
        parseStringToProperty(entryParts[0], defaultTable),
        parseStringToSortingOrderEnum(entryParts[1])
    ));
    return data;
}
export const handleOrderBy = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}):void => {
    queryContext.orderBy = args.reduce(parseStringToSortingData(queryContext.tables[0]), []);
}

export const handleOpenParentheses = ({queryContext}:{queryContext:IQueryContext}) => 
    handleStatement(QuerySyntaxEnum.OpenParentheses, queryContext);


export const handleCloseParentheses = ({queryContext}:{queryContext:IQueryContext}) => 
    handleStatement(QuerySyntaxEnum.CloseParentheses, queryContext);


export const handleSelect = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => {
    queryContext.data = args.reduce(handleMySqlFunctions(queryContext.functions), []);
}

export const handleSet = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}) => {
    if(args.length > 1) {
        throw new Error('Too many arguments');
    }
    queryContext.data = args[0];
}

export const handleTo = ({queryContext, args}:{queryContext:IQueryContext, args:string[]}) => {
    queryContext.tables = args.reduce(combineReducers(
        ifStringThenParseToTable,
        ifAliasThenParseToTable
    ),[]);
}

export const handleUpdate = ({queryContext, args}:{queryContext:IQueryContext, args:string[]}) => {
    queryContext.tables = args.reduce(combineReducers(
        ifStringThenParseToTable,
        ifAliasThenParseToTable
    ),[]);
}

export const handleWhere = ({queryContext, args}:{queryContext:IQueryContext, args:PropertyOrLogicalOperatorScope[]}) => {
    const defaultTable:Table = queryContext.tables[0];
    const keychain:PropertyOrLogicalOperatorScope[] = args.reduce(
        combineReducers(
            ifEmptyTableSetDefault(defaultTable),
            ifLogicalOperatorScopeThenParseItsKeys(defaultTable),
            mergePropertiesWithLogicalAnd
    ), []);
    queryContext.conditions.update(QuerySyntaxEnum.Where, keychain);
}

export const handleOn = ({queryContext, args}:{queryContext:IQueryContext, args:Property[]}) => {
    const latestJoin:Join = queryContext.joins && queryContext.joins[queryContext.joins.length - 1];
    const data = args[0];

    if(!latestJoin) {
        throw new Error(`"latestJoin" is undefined`)
    }

    if(!isProperty(data)) {
        throw new Error(`given argument is not a Property object`)
    }
    latestJoin.propertyA = data;
}

export const handleJoin = (type:JoinType) => ({queryContext, args}:{queryContext:IQueryContext, args:Property[]}) => {
    const data = args[0];
    if(!isProperty(data)) {
        throw new Error(`given argument is not a Property object`);
    }
    queryContext.joins.push(newJoin(type,data));
}

export const handleUnion = () => {
    console.log(`need to implement "union"`);
}

export const handleGroupBy = ({queryContext, args}:{queryContext:IQueryContext, args:any[]}):void => {
    queryContext.groupBy = args.reduce(parseStringToSortingData(queryContext.tables[0]), []);
}

export const handles = new Map<QuerySyntaxEnum, Fn>([
    [QuerySyntaxEnum.And, handleAnd],
    [QuerySyntaxEnum.Between, handleBetween],
    [QuerySyntaxEnum.Delete, handleDelete],
    [QuerySyntaxEnum.EQ, handleEqualTo],
    [QuerySyntaxEnum.From, handleFrom],
    [QuerySyntaxEnum.GT, handleGT],
    [QuerySyntaxEnum.Insert, handleInsert],
    [QuerySyntaxEnum.Like, handleLike],
    [QuerySyntaxEnum.Limit, handleLimit],
    [QuerySyntaxEnum.LT, handleLT],
    [QuerySyntaxEnum.Not, handleNot],
    [QuerySyntaxEnum.Null, handleNull],
    [QuerySyntaxEnum.Or, handleOr],
    [QuerySyntaxEnum.OrderBy, handleOrderBy],
    [QuerySyntaxEnum.Select, handleSelect],
    [QuerySyntaxEnum.Set, handleSet],
    [QuerySyntaxEnum.To, handleTo],
    [QuerySyntaxEnum.Update, handleUpdate],
    [QuerySyntaxEnum.Where, handleWhere],
    [QuerySyntaxEnum.On, handleOn],
    [QuerySyntaxEnum.LeftJoin, handleJoin(QuerySyntaxEnum.LeftJoin)],
    [QuerySyntaxEnum.RightJoin, handleJoin(QuerySyntaxEnum.RightJoin)],
    [QuerySyntaxEnum.Join, handleJoin(QuerySyntaxEnum.Join)],
    [QuerySyntaxEnum.FullJoin, handleJoin(QuerySyntaxEnum.FullJoin)],
    [QuerySyntaxEnum.Union, handleUnion],
    [QuerySyntaxEnum.GroupBy, handleGroupBy],
    [QuerySyntaxEnum.OpenParentheses, handleOpenParentheses],
    [QuerySyntaxEnum.CloseParentheses, handleCloseParentheses],
]);