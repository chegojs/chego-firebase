import { JoinType } from './../api/firebaseTypes';
import { PropertyOrLogicalOperatorScope, QuerySyntaxEnum, Fn, Table, FunctionData, Property, AnyButFunction, SortingData } from '@chego/chego-api';
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

const handleCondition = (type: QuerySyntaxEnum, keychain?: PropertyOrLogicalOperatorScope[], args?: any[]): Fn[] => {
    const functions: Fn[] = [];
    const keychainLength: number = keychain && keychain.length || 0;
    const valuesLength: number = args && args.length || 0;
    const template = templates.get(type);

    for (let i = 0; i < keychainLength; i++) {
        const data: PropertyOrLogicalOperatorScope = keychain[i];
        if (isLogicalOperatorScope(data)) {
            functions.push(useTemplate(data.type), ...handleCondition(type, data.properties, args));
        } else {
            for (let q = 0; q < valuesLength; q++) {
                if (isLogicalOperatorScope(args[q])) {
                    functions.push(useTemplate(args[q].type), ...handleCondition(type, [data], args[q].properties));
                } else {
                    functions.push(template(args[q])(data));
                }
            }
        }
    }

    return keychainLength > 1
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

    const handleWhere = (...args: any[]): void => {
        const lastType: QuerySyntaxEnum = history[history.length - 1];
        const penultimateType: QuerySyntaxEnum = history[history.length - 2];
        const defaultTable: Table = queryContext.tables[0];
        const keys: PropertyOrLogicalOperatorScope[] = args.reduce(
            combineReducers(
                ifEmptyTableSetDefault(defaultTable),
                ifLogicalOperatorScopeThenParseItsKeys(defaultTable),
                mergePropertiesWithLogicalAnd
            ), []);

        if (isAndOr(lastType) && penultimateType === QuerySyntaxEnum.Where) {
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
        const functions: Fn[] = [];
        const keychainLength: number = keychain && keychain.length || 0;
        const template = templates.get(QuerySyntaxEnum.Between);
        for (let i = 0; i < keychainLength; i++) {
            const data: Property = <Property>keychain[i];
            if (i > 0) {
                functions.push(useTemplate(data.type));
            }
            functions.push(template(...args)(data));
        }

        if (keychainLength > 1) {
            queryContext.conditions.add(
                useTemplate(QuerySyntaxEnum.OpenParentheses),
                ...functions,
                useTemplate(QuerySyntaxEnum.CloseParentheses))
        } else {
            queryContext.conditions.add(...functions)
        }
    }

    const handleEQ = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.EQ, keychain.slice(), args));
    }

    const handleLT = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.LT, keychain.slice(), args));
    }

    const handleGT = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.GT, keychain.slice(), args));
    }

    const handleLike = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.Like, keychain.slice(), args));
    }

    const handleNull = (...args: any[]): void => {
        queryContext.conditions.add(...handleCondition(QuerySyntaxEnum.Null, keychain.slice(), args));
    }

    const handleUnion = (...args: any[]): void => {
        // TODO
    }

    const handleExists = (...args: any[]): void => {
        // TODO
    }

    const handleHaving = (...args: any[]): void => {
        // TODO
    }

    const handles = new Map<QuerySyntaxEnum, Fn>([
        [QuerySyntaxEnum.Delete, handleDelete],
        [QuerySyntaxEnum.Insert, handleInsert],
        [QuerySyntaxEnum.Select, handleSelect],
        [QuerySyntaxEnum.Update, handleUpdate],
        [QuerySyntaxEnum.From, handleFrom],
        [QuerySyntaxEnum.Where, handleWhere],
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
        [QuerySyntaxEnum.Having, handleHaving]
    ]);

    const builder: IQueryContextBuilder = {
        with: (type: QuerySyntaxEnum, params: any[]): void => {
            const handle = handles.get(type);
            if (handle) {
                handle(...params);
            }
            if(isPrimaryCommand(type)) {
                queryContext.type = type;
            }
            history.push(type);
        },
        build: (): IQueryContext => queryContext
    }
    return builder;
}