import { FunctionData, Property, QuerySyntaxEnum, Fn } from '@chego/chego-api';
import { IQueryContext, Row, DataMap } from '@chego/chego-database-boilerplate';

const parseMin = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    let min: any = rows[0].content[keyName];

    if (!min) {
        return rows;
    }

    const result: Row = rows[0]; /*clone(rows[0]);*/

    rows.forEach((row: Row) => {
        if (row.content[keyName] < min) {
            min = row.content[keyName];
        }
    });

    delete result.content[keyName];
    result.content[fnData.alias] = min;
    return [result];
}

const parseMax = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    let max: any = rows[0].content[keyName];

    if (!max) {
        return rows;
    }

    const result: Row = rows[0]; /*clone(rows[0]);*/

    rows.forEach((row: Row) => {
        if (row.content[keyName] > max) {
            max = row.content[keyName];
        }
    });

    delete result.content[keyName];
    result.content[fnData.alias] = max;
    return [result];
}

const parseCount = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    const result: Row = rows[0]; /*clone(rows[0]);*/
    delete result.content[keyName];
    result.content[fnData.alias] = rows.length;
    return [result];
}

const isNumeric = (n: any) => !isNaN(parseFloat(n)) && isFinite(n);

const parseSum = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    let sum: number = 0;
    let field: number;
    const result: Row = rows[0]; /*clone(rows[0]);*/

    rows.forEach((row: Row) => {
        field = row.content[keyName];
        if (isNumeric(field)) {
            sum += field;
        }
    });

    delete result.content[keyName];
    result.content[fnData.alias] = sum;
    return [result];
}
const parseAvg = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    let sum: number = 0;
    let field: number;
    const result: Row = rows[0]; /*clone(rows[0]);*/

    rows.forEach((row: Row) => {
        field = row.content[keyName];
        if (isNumeric(field)) {
            sum += field;
        }
    });

    delete result.content[keyName];
    result.content[fnData.alias] = sum / rows.length;
    return [result];
}
const parsePow = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    let field: number;

    rows.forEach((row: Row) => {
        field = row.content[keyName];
        row.content[fnData.alias] = isNumeric(field) ? Math.pow(field, fnData.exponent) : 0;
        delete row.content[keyName];
    });

    return rows;
}

const parseSqrt = (rows: Row[], fnData: FunctionData): Row[] => {
    const keyName: string = fnData.properties[0].name;
    let field: number;

    rows.forEach((row: Row) => {
        field = row.content[keyName];
        row.content[fnData.alias] = isNumeric(field) ? Math.sqrt(field) : 0;
        delete row.content[keyName];
    });

    return rows;
}

const parseLeast = (rows: Row[], fnData: FunctionData): Row[] => {
    return rows.reduce((acc: Row[], row: Row) => {
        let min: any = row.content[fnData.properties[0].name]
        min = fnData.properties.reduce((acc: any, tdk: Property) => {
            if (row.content[tdk.name] < min) {
                acc = row.content[tdk.name]
            }
            return acc;
        }, min);
        // remove origins
        row.content[fnData.alias] = min;
        acc.push(row);
        return acc;
    }, []);
}

const parseGreatest = (rows: Row[], fnData: FunctionData): Row[] => {
    return rows.reduce((acc: Row[], row: Row) => {
        let max: any = row.content[fnData.properties[0].name]
        max = fnData.properties.reduce((acc: any, tdk: Property) => {
            if (row.content[tdk.name] > max) {
                acc = row.content[tdk.name]
            }
            return acc;
        }, max);
        // remove origins
        row.content[fnData.alias] = max;
        acc.push(row);
        return acc;
    }, []);
}

const parseCoalesce = (rows: Row[], fnData: FunctionData): Row[] => {
    return rows.reduce((acc: Row[], row: Row) => {
        let result: any = null;
        for (const property of fnData.properties) {
            if (row.content[property.name]) {
                result = row.content[property.name];
                break;
            }
        }
        // remove origins
        row.content[fnData.alias] = result;
        acc.push(row);
        return acc;
    }, []);
}

const mysqlFunctions: Map<QuerySyntaxEnum, Fn<Row[]>> = new Map<QuerySyntaxEnum, Fn<Row[]>>([
    [QuerySyntaxEnum.Min, parseMin],
    [QuerySyntaxEnum.Max, parseMax],
    [QuerySyntaxEnum.Sum, parseSum],
    [QuerySyntaxEnum.Sqrt, parseSqrt],
    [QuerySyntaxEnum.Pow, parsePow],
    [QuerySyntaxEnum.Avg, parseAvg],
    [QuerySyntaxEnum.Least, parseLeast],
    [QuerySyntaxEnum.Greatest, parseGreatest],
    [QuerySyntaxEnum.Coalesce, parseCoalesce]
]);

const applyMySQLFunctions = (functions: FunctionData[], tableName?: string) => (rows: Row[], key: string, map: DataMap): void => {
    let parsedRows: Row[] = [];
    functions.forEach((fnData: FunctionData) => {
        const mySQLFn: Fn<Row[]> = mysqlFunctions.get(fnData.type);
        if (mySQLFn && fnData.properties[0].table.name === (tableName ? tableName : key)) {
            parsedRows = [...parsedRows, ...mySQLFn(rows, fnData)];
        }
    });
    map.set(key, parsedRows);
}

export const applyMySQLFunctionsIfAny = (queryContext: IQueryContext) => (data: DataMap): DataMap => {
    if (queryContext.functions.length) {
        data.forEach(applyMySQLFunctions(queryContext.functions));
    }
    return data;
}