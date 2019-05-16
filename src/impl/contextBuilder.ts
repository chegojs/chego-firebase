import { JoinType } from './../api/firebaseTypes';
import { PropertyOrLogicalOperatorScope, QuerySyntaxEnum, Fn, Table, FunctionData, Property, AnyButFunction, SortingData, QuerySyntaxTemplate } from '@chego/chego-api';
import { IQueryContext, IQueryContextBuilder } from '../api/firebaseInterfaces';
import { newQueryContext } from './queryContext';
import { combineReducers, mergePropertiesWithLogicalAnd, isLogicalOperatorScope, isProperty, newLogicalOperatorScope, isMySQLFunction, isAliasString, newTable, isAlias, newSortingData, parseStringToProperty, newLimit } from '@chego/chego-tools';
import { parseStringToSortingOrderEnum, newJoinBuilder } from './utils';
import { templates } from './templates';

const isPrimaryCommand = (type: QuerySyntaxEnum) => type === QuerySyntaxEnum.Select
    || type === QuerySyntaxEnum.Update
    || type === QuerySyntaxEnum.Insert
    || type === QuerySyntaxEnum.Delete;

const ifStringThenParseToTable = (tables: Table[], table: any) => {
    if (typeof table === 'string') {
        if (isAliasString(table)) {
            const data: string[] = table.replace(/ {1,}/g, ' ').split(' AS ');
            return tables.concat(newTable(data[0], data[1]));
        }
        return tables.concat(newTable(table));
    }
    return tables.concat(table);
}

const ifAliasThenParseToTable = (tables: Table[], table: any) => (isAlias(table))
    ? tables.concat(newTable(table.name, table.alias))
    : tables.concat(table);

const ifEmptyTableSetDefault = (defaultTable: Table) => (list: PropertyOrLogicalOperatorScope[], data: PropertyOrLogicalOperatorScope) => {
    if (isProperty(data) && !data.table) {
        data.table = defaultTable;
    }
    return list.concat(data);
}

const ifLogicalOperatorScopeThenParseItsKeys = (defaultTable: Table) => (list: PropertyOrLogicalOperatorScope[], data: PropertyOrLogicalOperatorScope) => {
    if (isLogicalOperatorScope(data)) {
        data.properties.reduce(
            combineReducers(
                ifEmptyTableSetDefault(defaultTable),
                ifLogicalOperatorScopeThenParseItsKeys(defaultTable),
            ), []);
    }
    return list.concat(data);
}

const handleMySqlFunctions = (mySqlFunctions: FunctionData[]) => (keys: Property[], data: Property | FunctionData) => {
    if (isMySQLFunction(data)) {
        mySqlFunctions.push(data);
        return keys.concat(data.properties);
    }
    return keys.concat(data);
}

const parseStringToSortingData = (defaultTable: Table) => (data: SortingData[], entry: string): SortingData[] => {
    const entryParts = entry.replace(/ {1,}/g, ' ').split(' ');

    if (entryParts.length > 2) {
        throw new Error(`There is something wrong with this order by "${entry}"`);
    }
    data.push(newSortingData(
        parseStringToProperty(entryParts[0], defaultTable),
        parseStringToSortingOrderEnum(entryParts[1])
    ));
    return data;
}

const isAndOr = (type: QuerySyntaxEnum): boolean => type === QuerySyntaxEnum.And || type === QuerySyntaxEnum.Or;

const useTemplate = (type: QuerySyntaxEnum, property?: Property, ...values: any[]): Fn => {
    const template = templates.get(type);
    if (template) {
        return template(...values)(property);
    }
    throw new Error(`No template for ${QuerySyntaxEnum[type]}`);
}


const handleValuesPerCondition = (type: QuerySyntaxEnum, template: QuerySyntaxTemplate, values?: any[]) =>
    (functions: Fn[], data: PropertyOrLogicalOperatorScope, i: number) => {
        if (i > 0) {
            functions.push(useTemplate(data.type));
        }
        functions.push(template(...values)(data));
        return functions;
    }

const handleConditionPerValue = (type: QuerySyntaxEnum, template: QuerySyntaxTemplate, values?: any[]) =>
    (functions: Fn[], data: PropertyOrLogicalOperatorScope) => {
        if (isLogicalOperatorScope(data)) {
            functions.push(useTemplate(data.type), ...handleCondition(type, handleConditionPerValue, data.properties, values));
        } else {
            for (const value of values) {
                if (isLogicalOperatorScope(value)) {
                    functions.push(useTemplate(value.type), ...handleCondition(type, handleConditionPerValue, [data], value.properties));
                } else {
                    functions.push(template(value)(data));
                }
            }
        }
        return functions;
    }

const handleCondition = (type: QuerySyntaxEnum, reducer: Fn, keychain?: PropertyOrLogicalOperatorScope[], values?: any[]): Fn[] => {
    const template = templates.get(type);
    const functions: Fn[] = keychain.reduce(reducer(type, template, values), []);

    return functions.length > 1
        ? [
            useTemplate(QuerySyntaxEnum.OpenParentheses),
            ...functions,
            useTemplate(QuerySyntaxEnum.CloseParentheses)
        ]
        : functions;
}

export const newQueryContextBuilder = (): IQueryContextBuilder => {
    let keychain: PropertyOrLogicalOperatorScope[] = [];
    let tempJoinBuilder: any;
    const queryContext: IQueryContext = newQueryContext();
    const history: QuerySyntaxEnum[] = [];

    const handleSelect = (...args: any[]): void => {
        queryContext.data = args.reduce(handleMySqlFunctions(queryContext.functions), []);
    }

    const handleInsert = (...args: any[]): void => {
        queryContext.data = args;
    }

    const handleUpdate = (...args: any[]): void => {
        queryContext.tables = args.reduce(combineReducers(
            ifStringThenParseToTable,
            ifAliasThenParseToTable
        ), []);
    }

    const handleDelete = (...args: any[]): void => {
        queryContext.data = args;
    }

    const handleFrom = (...args: any[]): void => {
        queryContext.tables = args.reduce(combineReducers(
            ifStringThenParseToTable,
            ifAliasThenParseToTable
        ), []);
        queryContext.data.reduce(ifEmptyTableSetDefault(queryContext.tables[0]), []);
    }

    const handleOrderBy = (...args: any[]): void => {
        queryContext.orderBy = args.reduce(parseStringToSortingData(queryContext.tables[0]), []);
    }

    const handleGroupBy = (...args: any[]): void => {
        queryContext.groupBy = args.reduce(parseStringToSortingData(queryContext.tables[0]), []);
    }

    const handleJoin = (type: JoinType) => (...args: any[]): void => {
        tempJoinBuilder = newJoinBuilder(type, args[0])
    }

    const handleOn = (...args: any[]): void => {
        if (!tempJoinBuilder) {
            throw new Error(`"latestJoin" is undefined`)
        }
        queryContext.joins.push(tempJoinBuilder.withOn(args[0]).build());
        tempJoinBuilder = null;
    }

    const handleTo = (...args: any[]): void => {
        queryContext.tables = args.reduce(combineReducers(
            ifStringThenParseToTable,
            ifAliasThenParseToTable
        ), []);
    }

    const handleSet = (...args: AnyButFunction[]): void => {
        queryContext.data = args;
    }

    const handleKeychain = (type: QuerySyntaxEnum) => (...args: any[]): void => {
        const lastType: QuerySyntaxEnum = history[history.length - 1];
        const penultimateType: QuerySyntaxEnum = history[history.length - 2];
        const defaultTable: Table = queryContext.tables[0];
        const keys: PropertyOrLogicalOperatorScope[] = args.reduce(
            combineReducers(
                ifEmptyTableSetDefault(defaultTable),
                ifLogicalOperatorScopeThenParseItsKeys(defaultTable),
                mergePropertiesWithLogicalAnd
            ), []);

        if (isAndOr(lastType) && penultimateType === type) {
            const lastKey: PropertyOrLogicalOperatorScope = keychain[keychain.length - 1];
            if (!isLogicalOperatorScope(lastKey)) {
                throw new Error(`Key ${lastKey} should be LogialOperatorScope type!`)
            }
            lastKey.properties.push(...keys);
        } else {
            keychain = [...keys];
        }
    }

    const handleLimit = (...args: number[]): void => {
        queryContext.limit = newLimit(args[0], args[1]);
    }

    const handleLogicalOperator = (type: QuerySyntaxEnum) => (): void => {
        const lastType: QuerySyntaxEnum = history[history.length - 1];
        if (lastType === QuerySyntaxEnum.Where) {
            keychain.push(newLogicalOperatorScope(type));
        } else {
            queryContext.conditions.add(useTemplate(type));
        }
    }

    const handleParentheses = (type: QuerySyntaxEnum) => (): void => {
        queryContext.conditions.add(useTemplate(type));
    }

    const handleNot = (): void => {
        queryContext.conditions.add(useTemplate(QuerySyntaxEnum.Not));
    }

    const handleBetween = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.Between, handleValuesPerCondition, keychain.slice(), args));
    }

    const handleEQ = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.EQ, handleConditionPerValue, keychain.slice(), args));
    }

    const handleLT = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.LT, handleConditionPerValue, keychain.slice(), args));
    }

    const handleGT = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.GT, handleConditionPerValue, keychain.slice(), args));
    }

    const handleLike = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.Like, handleConditionPerValue, keychain.slice(), args));
    }

    const handleNull = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.Null, handleConditionPerValue, keychain.slice(), args));
    }

    const handleUnion = (...args: any[]): void => {
        // TODO
    }

    const handleExists = (...args: any[]): void => {
        // TODO
    }

    const handleIn = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.In, handleValuesPerCondition, keychain.slice(), args));
    }

    const handles = new Map<QuerySyntaxEnum, Fn>([
        [QuerySyntaxEnum.Delete, handleDelete],
        [QuerySyntaxEnum.Insert, handleInsert],
        [QuerySyntaxEnum.Select, handleSelect],
        [QuerySyntaxEnum.Update, handleUpdate],
        [QuerySyntaxEnum.From, handleFrom],
        [QuerySyntaxEnum.Where, handleKeychain(QuerySyntaxEnum.Where)],
        [QuerySyntaxEnum.To, handleTo],
        [QuerySyntaxEnum.Set, handleSet],
        [QuerySyntaxEnum.Limit, handleLimit],
        [QuerySyntaxEnum.Between, handleBetween],
        [QuerySyntaxEnum.EQ, handleEQ],
        [QuerySyntaxEnum.GT, handleGT],
        [QuerySyntaxEnum.Like, handleLike],
        [QuerySyntaxEnum.LT, handleLT],
        [QuerySyntaxEnum.Null, handleNull],
        [QuerySyntaxEnum.Not, handleNot],
        [QuerySyntaxEnum.And, handleLogicalOperator(QuerySyntaxEnum.And)],
        [QuerySyntaxEnum.Or, handleLogicalOperator(QuerySyntaxEnum.Or)],
        [QuerySyntaxEnum.LeftJoin, handleJoin(QuerySyntaxEnum.LeftJoin)],
        [QuerySyntaxEnum.RightJoin, handleJoin(QuerySyntaxEnum.RightJoin)],
        [QuerySyntaxEnum.Join, handleJoin(QuerySyntaxEnum.Join)],
        [QuerySyntaxEnum.FullJoin, handleJoin(QuerySyntaxEnum.FullJoin)],
        [QuerySyntaxEnum.On, handleOn],
        [QuerySyntaxEnum.OrderBy, handleOrderBy],
        [QuerySyntaxEnum.GroupBy, handleGroupBy],
        [QuerySyntaxEnum.OpenParentheses, handleParentheses(QuerySyntaxEnum.OpenParentheses)],
        [QuerySyntaxEnum.CloseParentheses, handleParentheses(QuerySyntaxEnum.CloseParentheses)],
        [QuerySyntaxEnum.Union, handleUnion],
        [QuerySyntaxEnum.Exists, handleExists],
        [QuerySyntaxEnum.Having, handleKeychain(QuerySyntaxEnum.Having)],
        [QuerySyntaxEnum.In, handleIn]
    ]);

    const builder: IQueryContextBuilder = {
        with: (type: QuerySyntaxEnum, params: any[]): void => {
            const handle = handles.get(type);
            if (handle) {
                handle(...params);
            }
            if (isPrimaryCommand(type)) {
                queryContext.type = type;
            }
            history.push(type);
        },
        build: (): IQueryContext => queryContext
    }
    return builder;
}