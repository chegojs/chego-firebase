import { Property, QuerySyntaxEnum, Fn, MinData, MaxData, CountData, SumData, AvgData, PowData, SqrtData, LeastData, GreatestData, CoalesceData, FunctionData, Atan2Param, AtanData, AsinData, AsciiData, Atan2Data, BinData, BinaryData, CastAsBinaryData, CastAsDateData, CastAsDatetimeData, CastAsTimeData, CastAsSignedData, CastAsUnsignedData, CeilData, CharLengthData, ConcatData, ConcatWsData, CosData, CotData, DegreesData, DivData, ExpData, FieldData, FloorData, LnData, LogData, Log10Data, Log2Data, MidData, ModData, RandData, RadiansData, PiData, SinData, TanData, RoundData, TruncateData, SignData, FindInSetData, FormatData, InsertData, InsertParam, InstrData, LcaseData, LeftData, UcaseData, LengthData, PositionData, LPadData, PadParam, LTrimData, RTrimData, TrimData, RepeatData, ReplaceData, ReverseData, SpaceData, StrcmpData, SubstrData, SubstrIndexData } from '@chego/chego-api';
import { IQueryContext, Row, DataMap } from '@chego/chego-database-boilerplate';
import { clone, isProperty } from '@chego/chego-tools';

enum CastDateType {
    Date,
    Datetime,
    Time
}

// Helpers

const ctg = (x: number): number => 1 / Math.tan(x);
const degrees = (radians: number): number => radians * (180 / Math.PI);
const radians = (degrees: number): number => degrees * (Math.PI / 180);
const div = (x: number, y: number): number => x / y;
const mod = (x: number, y: number): number => x % y;
const pi = (): number => Math.PI;
const round = (x: number, decimal: number): number => {
    const d: number = Math.pow(100, decimal);
    return Math.round(x * d) / d;
}

const formatNumber = (value: number, decimal: number): string => value.toFixed(decimal).replace(/\d(?=(\d{3})+\.)/g, '$&,');
const useMathFn = (mathFn: (...args: any[]) => number | string, alias: string, param: any, ...rest: any[]) => (rows: Row[], row: Row) => {
    if (isProperty(param)) {
        const value: any = row.content[param.name];
        row.content[alias] = isNumeric(value) ? mathFn(value, ...rest) : 0;
    } else {
        row.content[alias] = isNumeric(param) ? mathFn(param, ...rest) : 0;
    }
    return [...rows, row];
}

const countRows = (key: string) => (count: number, row: Row) => row.content[key] ? count++ : count;

const isNumeric = (n: any) => !isNaN(parseFloat(n)) && isFinite(n);

const sumRows = (key: string) => (sum: number, row: Row) => 
    isNumeric(row.content[key]) ? sum += row.content[key] : sum;

const getFirstNotNullValue = (row: Row, params: any[]) => {
    for (const param of params) {
        if (isProperty(param) && row.content[param.name]) {
            return row.content[param.name];
        } else if (param) {
            return param;
        }
    }
    return null;
}

const getEdgeValue = (params: any[], alias: string, reducer: any) => (acc: Row[], row: Row) => {
    let value: any = row.content[params[0].name];
    value = params.reduce(reducer(row, value), value);
    row.content[alias] = value;
    acc.push(row);
    return acc;
}
const leastRows = (row: Row, min: number) => (acc: number | string, property: Property) => {
    if (row.content[property.name] < min) {
        acc = row.content[property.name]
    }
    return acc;
}
const greatestRows = (row: Row, max: number) => (acc: number | string, property: Property) => {
    if (row.content[property.name] > max) {
        acc = row.content[property.name]
    }
    return acc;
}

const coalesceRows = (params: any[], alias: string) => (acc: Row[], row: Row) => {
    row.content[alias] = getFirstNotNullValue(row, params);
    acc.push(row);
    return acc;
}

const getAscii = (param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = String(isProperty(param) ? row.content[param.name] : param).charCodeAt(0);
    return [...rows, row];
}

const getNumericValue = (row: Row, param: any) => isProperty(param.x)
    ? isNumeric(row.content[param.x.name]) ? row.content[param.x.name] : 0
    : isNumeric(param) ? param : 0;

const getAtan2 = (param: Atan2Param, alias: string) => (rows: Row[], row: Row) => {
    const x: number = getNumericValue(row, param.x);
    const y: number = getNumericValue(row, param.y);
    row.content[alias] = Math.atan2(y, x);

    return [...rows, row];
}
const getBin = (param: any, alias: string) => (rows: Row[], row: Row) => {
    if (isProperty(param)) {
        const value: any = row.content[param.name];
        row.content[alias] = isNumeric(value) ? parseInt(value, 10).toString(2) : 0;
    } else {
        row.content[alias] = isNumeric(param) ? Math.atan(param) : 0;
    }
    return [...rows, row];
}
const parseBin = (rows: Row[], fnData: BinData): Row[] => rows.reduce(getBin(fnData.param, fnData.alias), []);

const getBinary = (param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = isProperty(param) ? row.content[param.name] : param;
    return [...rows, row];
}

const concatParams = (row: Row) => (result: string, param: any) => `${result}${isProperty(param) ? row.content[param.name] : param}`;
const castConcat = (alias: string, params: any[], separator: string = '') => (rows: Row[], row: Row) => {
    row.content[alias] = params.reduce(concatParams(row), separator);
    return [...rows, row];
}


const getField = (search: any, values: any[], alias: string) => (rows: Row[], row: Row) => {
    const phrase: number = Number(isProperty(search) ? row.content[search.name] : search);
    let index: number = 0;
    for (const i in values) {
        const current: any = values[i];
        const value: number = Number(isProperty(current) ? row.content[current.name] : search);
        if (phrase === value) {
            index = Number(i);
            break;
        }
    }
    row.content[alias] = index;
    return [...rows, row];
}
const findInSet = (search: any, set: any, alias: string) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(search) ? row.content[search.name] : search);
    const list: string[] = String(isProperty(set) ? row.content[set.name] : set).split(',');
    const index: number = list.indexOf(phrase);
    row.content[alias] = index === -1 ? 0 : index;
    return [...rows, row];
}

const insertStrings = (param: InsertParam, alias: string) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(param.value) ? row.content[param.value.name] : param.value);
    const newString: string = String(isProperty(param.toInsert) ? row.content[param.toInsert.name] : param.toInsert);
    row.content[alias] = base.slice(0, param.position) + newString + base.slice(param.position + Math.abs(length));
    return [...rows, row];
}

const getSubstringIndex = (alias: string, search: any, value: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(search) ? row.content[search.name] : search);
    const base: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = base.indexOf(phrase) + 1;
    return [...rows, row];
}

const lcase = (alias: string, value: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.toLocaleLowerCase();
    return [...rows, row];
}

const ucase = (alias: string, value: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.toLocaleUpperCase();
    return [...rows, row];
}

const sliceString = (alias: string, value: any, start: number, charsCount: number) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.slice(start, charsCount);
    return [...rows, row];
}

const stringLength = (alias: string, value: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.length;
    return [...rows, row];
}

const padString = (alias: string, left: boolean, param: PadParam) => (rows: Row[], row: Row) => {
    const value: string = String(isProperty(param.value) ? row.content[param.value.name] : param.value);
    const value2: string = String(isProperty(param.value2) ? row.content[param.value2.name] : param.value2);
    row.content[alias] = left ? value.padStart(param.length, value2) : value.padEnd(param.length, value2);
    return [...rows, row];
}

const trimString = (alias: string, value: any, regex: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.replace(regex, '');
    return [...rows, row];
}

const getSubstring = (alias: string, value: any, start: number, length: number) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = base.slice(start, start + length);
    return [...rows, row];
}

const repeatString = (alias: string, value: any, count: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    const size: number = Number(isProperty(count) ? row.content[count.name] : count);
    row.content[alias] = Array<string>(size).fill(phrase).join('');
    return [...rows, row];
}

const replaceString = (alias: string, value: any, from: any, to: any) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(value) ? row.content[value.name] : value);
    const phrase: string = String(isProperty(from) ? row.content[from.name] : from);
    const replacement: string = String(isProperty(to) ? row.content[to.name] : to);
    row.content[alias] = base.replace(new RegExp(phrase, 'g'), replacement);
    return [...rows, row];
}

const reverseString = (alias: string, value: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.split('').reverse().join('');
    return [...rows, row];
}

const compareStrings = (alias: string, value: any, value2: any) => (rows: Row[], row: Row) => {
    const str1: string = String(isProperty(value) ? row.content[value.name] : value);
    const str2: string = String(isProperty(value2) ? row.content[value2.name] : value2);
    row.content[alias] = str1 === str2;
    return [...rows, row];
}

const getSubstringBeforeDelimiter = (alias: string, value: any, delimiter: string, count: number) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = base.split(delimiter, count).join(delimiter).length;
    return [...rows, row];
}

const parseInteger = (unsigned: boolean, value: number) => isNumeric(value) && ((unsigned && value > 0) || !unsigned) ? Math.round(value) : 0;

const castInteger = (unsigned: boolean, param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = parseInteger(unsigned, isProperty(param) ? Number(row.content[param.name]) : Number(param));
    return [...rows, row];
}
const castCharLength = (param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = isProperty(param) ? String(row.content[param.name]).length : String(param).length;
    return [...rows, row];
}


const daysInMonth = (month: number, year: number) => {
    switch (month) {
        case 2:
            return (year % 4 === 0 && year % 100) || year % 400 === 0 ? 29 : 28;
        case 4:
        case 6:
        case 9:
        case 11:
            return 30;
        default:
            return 31
    }
}

const isValidDate = (day: number, month: number, year: number) =>
    month >= 0 && month < 12 && day > 0 && day <= daysInMonth(month, year);

const parseYears = (value: string) => {
    const yearsNr = Number(value);
    if (yearsNr < 0 || yearsNr > 9999) {
        return null;
    } else if (value.length === 1) {
        return (yearsNr === 0) ? `0000` : `200${value}`;
    } else if (value.length === 2) {
        return (yearsNr > 69) ? `19${value}` : `20${value}`;
    } else if (value.length === 3) {
        return `0${value}`;
    } else {
        return value;
    }
}

const parseMonths = (value: string) => {
    const months = Number(value);
    return (months >= 0 || months <= 12)
        ? (months < 10)
            ? `0${months}`
            : value
        : null;
}

const parseDays = (value: string, month: number, year: number) => {
    const day = Number(value);
    if (isValidDate(day, month, year)) {
        return (day >= 0 || day <= 31)
            ? (day < 10)
                ? `0${day}`
                : value
            : null
    }
    return null;
}

const parseDate = (date: string[]): string => {
    if (date) {
        const dateParts: string[] = date[0].split(/\D+/);
        const year: string = parseYears(dateParts[0]);
        const month: string = parseMonths(dateParts[1]);
        const day: string = parseDays(dateParts[2], Number(month), Number(year));
        return (year && month && day) ? `${year}-${month}-${day}` : null;
    }
    return null;
}

const parseTimeElement = (value: string, minValue: number, maxValue: number): string => {
    const i: number = Number(value);
    return i
        ? (i >= minValue && i <= maxValue)
            ? (i < 10) ? `0${i}` : value
            : null
        : '00';
}

const parseTime = (time: string[]): string => {
    if (time) {
        const timeParts: string[] = time[0].split(/\D+/);
        const hh: string = parseTimeElement(timeParts[0], 0, 23);
        const mm: string = parseTimeElement(timeParts[1], 0, 59);
        const ss: string = parseTimeElement(timeParts[2], 0, 59);
        return (hh && mm && ss) ? `${hh}:${mm}:${ss}` : null;
    }
    return null;
}

const castDate = (param: any, alias: string, type: CastDateType) => (rows: Row[], row: Row) => {
    const value = isProperty(param) ? row.content[param.name] : param;
    const dateMatch: string[] = value.match(/\d+[^A-Za-z0-9\s\:]+\d+[^A-Za-z0-9\s\:]+\d+/);
    const timeMatch: string[] = value.match(/\d{2}\:\d{2}\:\d{2}/);

    if (type === CastDateType.Date) {
        row.content[alias] = parseDate(dateMatch);
    } else if (type === CastDateType.Time) {
        row.content[alias] = parseTime(timeMatch);
    } else {
        const date: string = parseDate(dateMatch);
        const time: string = parseTime(timeMatch);
        row.content[alias] = (date && time) ? `${date} ${time}` : null;
    }

    return [...rows, row];
}

// Parsers

const parseMin = (rows: Row[], fnData: MinData): Row[] => {
    const result: Row = clone(rows[0]);
    if (isProperty(fnData.param)) {
        let min: any = rows[0].content[fnData.param.name];
        if (!min) {
            return rows;
        }
        for (const row of rows) {
            if (row.content[fnData.param.name] < min) {
                min = row.content[fnData.param.name];
            }
        }
        result.content[fnData.alias] = min;
    } else {
        result.content[fnData.alias] = fnData.param;
    }
    return [result];
}

const parseMax = (rows: Row[], fnData: MaxData): Row[] => {
    const result: Row = clone(rows[0]);
    if (isProperty(fnData.param)) {
        let max: any = rows[0].content[fnData.param.name];
        if (!max) {
            return rows;
        }
        for (const row of rows) {
            if (row.content[fnData.param.name] > max) {
                max = row.content[fnData.param.name];
            }
        }
        result.content[fnData.alias] = max;
    } else {
        result.content[fnData.alias] = fnData.param;
    }
    return [result];
}
const parseCount = (rows: Row[], fnData: CountData): Row[] => {
    const result: Row = clone(rows[0]);
    if (isProperty(fnData.param)) {
        result.content[fnData.alias] = rows.reduce(countRows(fnData.param.name), 0);
    } else {
        result.content[fnData.alias] = rows.length;
    }
    return [result];
}

const parseSum = (rows: Row[], fnData: SumData): Row[] => {
    const result: Row = clone(rows[0]);
    if (isProperty(fnData.param)) {
        result.content[fnData.alias] = rows.reduce(sumRows(fnData.param.name), 0);
    } else {
        result.content[fnData.alias] = isNumeric(fnData.param) ? Number(fnData.param) * rows.length : 0;
    }
    return [result];
}
const parseAvg = (rows: Row[], fnData: AvgData): Row[] => {
    const result: Row = clone(rows[0]);
    if (isProperty(fnData.param)) {
        result.content[fnData.alias] = rows.reduce(sumRows(fnData.param.name), 0) / rows.length;
    } else {
        result.content[fnData.alias] = isNumeric(fnData.param) ? Number(fnData.param) / rows.length : 0;
    }
    return [result];
}

const parsePow = (rows: Row[], fnData: PowData): Row[] =>
    rows.reduce(useMathFn(Math.pow, fnData.alias, fnData.param.value, fnData.param.exponent), []);

const parseSqrt = (rows: Row[], fnData: SqrtData): Row[] =>
    rows.reduce(useMathFn(Math.sqrt, fnData.alias, fnData.param), []);

const parseLeast = (rows: Row[], fnData: LeastData): Row[] =>
    rows.reduce(getEdgeValue(fnData.param, fnData.alias, leastRows), []);

const parseGreatest = (rows: Row[], fnData: GreatestData): Row[] =>
    rows.reduce(getEdgeValue(fnData.param, fnData.alias, greatestRows), []);

const parseCoalesce = (rows: Row[], fnData: CoalesceData): Row[] =>
    rows.reduce(coalesceRows(fnData.param, fnData.alias), []);

const parseAscii = (rows: Row[], fnData: AsciiData): Row[] =>
    rows.reduce(getAscii(fnData.param, fnData.alias), []);

const parseAsin = (rows: Row[], fnData: AsinData): Row[] =>
    rows.reduce(useMathFn(Math.asin, fnData.alias, fnData.param), []);

const parseAtan = (rows: Row[], fnData: AtanData): Row[] =>
    rows.reduce(useMathFn(Math.atan, fnData.alias, fnData.param), []);

const parseAtan2 = (rows: Row[], fnData: Atan2Data): Row[] =>
    rows.reduce(getAtan2(fnData.param, fnData.alias), []);
// mock
const parseBinary = (rows: Row[], fnData: BinaryData): Row[] =>
    rows.reduce(getBinary(fnData.param, fnData.alias), []);
// mock
const parseCastAsBinary = (rows: Row[], fnData: CastAsBinaryData): Row[] =>
    rows.reduce(getBinary(fnData.param, fnData.alias), []);
// mock
const parseCastAsChar = (rows: Row[], fnData: CastAsBinaryData): Row[] =>
    rows.reduce(getBinary(fnData.param, fnData.alias), []);

const parseCastAsDate = (rows: Row[], fnData: CastAsDateData): Row[] =>
    rows.reduce(castDate(fnData.param, fnData.alias, CastDateType.Date), []);

const parseCastAsDatetime = (rows: Row[], fnData: CastAsDatetimeData): Row[] =>
    rows.reduce(castDate(fnData.param, fnData.alias, CastDateType.Datetime), []);

const parseCastAsTime = (rows: Row[], fnData: CastAsTimeData): Row[] =>
    rows.reduce(castDate(fnData.param, fnData.alias, CastDateType.Time), []);

const parseCastAsSigned = (rows: Row[], fnData: CastAsSignedData): Row[] =>
    rows.reduce(castInteger(false, fnData.param, fnData.alias), []);

const parseCastAsUnsigned = (rows: Row[], fnData: CastAsUnsignedData): Row[] =>
    rows.reduce(castInteger(true, fnData.param, fnData.alias), []);

const parseConcat = (rows: Row[], fnData: ConcatData): Row[] =>
    rows.reduce(castConcat(fnData.alias, fnData.param), []);

const parseConcatWs = (rows: Row[], fnData: ConcatWsData): Row[] =>
    rows.reduce(castConcat(fnData.alias, fnData.param.values, fnData.param.separator), []);

const parseCharLength = (rows: Row[], fnData: CharLengthData): Row[] =>
    rows.reduce(castCharLength(fnData.param, fnData.alias), []);

const parseSubstringIndex = (rows: Row[], fnData: SubstrIndexData): Row[] =>
    rows.reduce(getSubstringBeforeDelimiter(fnData.alias, fnData.param.value, fnData.param.delimiter, fnData.param.count), []);

const parseSubstr = (rows: Row[], fnData: SubstrData): Row[] =>
    rows.reduce(getSubstring(fnData.alias, fnData.param.value, fnData.param.start, fnData.param.length), []);

const parseStrcmp = (rows: Row[], fnData: StrcmpData): Row[] =>
    rows.reduce(compareStrings(fnData.alias, fnData.param.value, fnData.param.value2), []);

const parseSpace = (rows: Row[], fnData: SpaceData): Row[] =>
    rows.reduce(repeatString(fnData.alias, ' ', fnData.param), []);

const parseReverse = (rows: Row[], fnData: ReverseData): Row[] =>
    rows.reduce(reverseString(fnData.alias, fnData.param), []);

const parseReplace = (rows: Row[], fnData: ReplaceData): Row[] =>
    rows.reduce(replaceString(fnData.alias, fnData.param.value, fnData.param.from, fnData.param.to), []);

const parseRepeat = (rows: Row[], fnData: RepeatData): Row[] =>
    rows.reduce(repeatString(fnData.alias, fnData.param.value, fnData.param.count), []);

const parseMid = (rows: Row[], fnData: MidData): Row[] =>
    rows.reduce(getSubstring(fnData.alias, fnData.param.value, fnData.param.start, fnData.param.length), []);

const parseTrim = (rows: Row[], fnData: TrimData): Row[] =>
    rows.reduce(trimString(fnData.alias, fnData.param, /^\s+|\s+$/g), []);

const parseRTrim = (rows: Row[], fnData: RTrimData): Row[] =>
    rows.reduce(trimString(fnData.alias, fnData.param, /\s*$/g), []);

const parseLTrim = (rows: Row[], fnData: LTrimData): Row[] =>
    rows.reduce(trimString(fnData.alias, fnData.param, /^\s*$/g), []);

const parseRPad = (rows: Row[], fnData: LPadData): Row[] =>
    rows.reduce(padString(fnData.alias, false, fnData.param), []);

const parseLPad = (rows: Row[], fnData: LPadData): Row[] =>
    rows.reduce(padString(fnData.alias, true, fnData.param), []);

const parsePosition = (rows: Row[], fnData: PositionData): Row[] =>
    rows.reduce(getSubstringIndex(fnData.alias, fnData.param.substring, fnData.param.value), []);

const parseLength = (rows: Row[], fnData: LengthData): Row[] =>
    rows.reduce(stringLength(fnData.alias, fnData.param), []);

const parseRight = (rows: Row[], fnData: LeftData): Row[] =>
    rows.reduce(sliceString(fnData.alias, fnData.param.value, -1, fnData.param.charsCount), []);

const parseLeft = (rows: Row[], fnData: LeftData): Row[] =>
    rows.reduce(sliceString(fnData.alias, fnData.param.value, 0, fnData.param.charsCount), []);

const parseUcase = (rows: Row[], fnData: UcaseData): Row[] =>
    rows.reduce(ucase(fnData.alias, fnData.param), []);

const parseLcase = (rows: Row[], fnData: LcaseData): Row[] =>
    rows.reduce(lcase(fnData.alias, fnData.param), []);

const parseInstr = (rows: Row[], fnData: InstrData): Row[] =>
    rows.reduce(getSubstringIndex(fnData.alias, fnData.param.search, fnData.param.value), []);

const parseInsertString = (rows: Row[], fnData: InsertData): Row[] =>
    rows.reduce(insertStrings(fnData.param, fnData.alias), []);

const parseField = (rows: Row[], fnData: FieldData): Row[] =>
    rows.reduce(getField(fnData.param.search, fnData.param.values, fnData.alias), []);

const parseFindInSet = (rows: Row[], fnData: FindInSetData): Row[] =>
    rows.reduce(findInSet(fnData.param.search, fnData.param.set, fnData.alias), []);

const parseFormat = (rows: Row[], fnData: FormatData): Row[] =>
    rows.reduce(useMathFn(formatNumber, fnData.alias, fnData.param.value, fnData.param.decimal), []);

const parseCos = (rows: Row[], fnData: CosData): Row[] =>
    rows.reduce(useMathFn(Math.cos, fnData.alias, fnData.param), []);

const parseCot = (rows: Row[], fnData: CotData): Row[] =>
    rows.reduce(useMathFn(ctg, fnData.alias, fnData.param), []);

const parseDegrees = (rows: Row[], fnData: DegreesData): Row[] =>
    rows.reduce(useMathFn(degrees, fnData.alias, fnData.param), []);

const parseDiv = (rows: Row[], fnData: DivData): Row[] =>
    rows.reduce(useMathFn(div, fnData.alias, fnData.param.x, fnData.param.y), []);

const parseExp = (rows: Row[], fnData: ExpData): Row[] =>
    rows.reduce(useMathFn(degrees, fnData.alias, fnData.param), []);

const parseFloor = (rows: Row[], fnData: FloorData): Row[] =>
    rows.reduce(useMathFn(Math.floor, fnData.alias, fnData.param), []);

const parseCeil = (rows: Row[], fnData: CeilData): Row[] =>
    rows.reduce(useMathFn(Math.ceil, fnData.alias, fnData.param), []);

const parseLn = (rows: Row[], fnData: LnData): Row[] =>
    rows.reduce(useMathFn(Math.log, fnData.alias, fnData.param), []);

const parseLog = (rows: Row[], fnData: LogData): Row[] =>
    rows.reduce(useMathFn(Math.log, fnData.alias, fnData.param), []);

const parseLog10 = (rows: Row[], fnData: Log10Data): Row[] =>
    rows.reduce(useMathFn(Math.log10, fnData.alias, fnData.param), []);

const parseLog2 = (rows: Row[], fnData: Log2Data): Row[] =>
    rows.reduce(useMathFn(Math.log2, fnData.alias, fnData.param), []);

const parseMod = (rows: Row[], fnData: ModData): Row[] =>
    rows.reduce(useMathFn(mod, fnData.alias, fnData.param.x, fnData.param.y), []);

const parsePi = (rows: Row[], fnData: PiData): Row[] =>
    rows.reduce(useMathFn(pi, fnData.alias, fnData.param), []);

const parseRadians = (rows: Row[], fnData: RadiansData): Row[] =>
    rows.reduce(useMathFn(radians, fnData.alias, fnData.param), []);

const parseRand = (rows: Row[], fnData: RandData): Row[] =>
    rows.reduce(useMathFn(Math.random, fnData.alias, fnData.param), []);

const parseSin = (rows: Row[], fnData: SinData): Row[] =>
    rows.reduce(useMathFn(Math.sin, fnData.alias, fnData.param), []);

const parseTan = (rows: Row[], fnData: TanData): Row[] =>
    rows.reduce(useMathFn(Math.tan, fnData.alias, fnData.param), []);

const parseRound = (rows: Row[], fnData: RoundData): Row[] =>
    rows.reduce(useMathFn(round, fnData.alias, fnData.param.value, fnData.param.decimal), []);

const parseTruncate = (rows: Row[], fnData: TruncateData): Row[] =>
    rows.reduce(useMathFn(Math.trunc, fnData.alias, fnData.param), []);

const parseSign = (rows: Row[], fnData: SignData): Row[] =>
    rows.reduce(useMathFn(Math.sign, fnData.alias, fnData.param), []);

const mysqlFunctions: Map<QuerySyntaxEnum, Fn<Row[]>> = new Map<QuerySyntaxEnum, Fn<Row[]>>([
    [QuerySyntaxEnum.Min, parseMin],
    [QuerySyntaxEnum.Max, parseMax],
    [QuerySyntaxEnum.Sum, parseSum],
    [QuerySyntaxEnum.Sqrt, parseSqrt],
    [QuerySyntaxEnum.Pow, parsePow],
    [QuerySyntaxEnum.Avg, parseAvg],
    [QuerySyntaxEnum.Least, parseLeast],
    [QuerySyntaxEnum.Greatest, parseGreatest],
    [QuerySyntaxEnum.Coalesce, parseCoalesce],
    [QuerySyntaxEnum.Count, parseCount],
    [QuerySyntaxEnum.Ascii, parseAscii],
    [QuerySyntaxEnum.Asin, parseAsin],
    [QuerySyntaxEnum.Atan, parseAtan],
    [QuerySyntaxEnum.Atan2, parseAtan2],
    [QuerySyntaxEnum.Bin, parseBin],
    [QuerySyntaxEnum.Binary, parseBinary],
    [QuerySyntaxEnum.CastAsBinary, parseCastAsBinary],
    [QuerySyntaxEnum.CastAsChar, parseCastAsChar],
    [QuerySyntaxEnum.CastAsDate, parseCastAsDate],
    [QuerySyntaxEnum.CastAsDatetime, parseCastAsDatetime],
    [QuerySyntaxEnum.CastAsTime, parseCastAsTime],
    [QuerySyntaxEnum.CastAsSigned, parseCastAsSigned],
    [QuerySyntaxEnum.CastAsUnsigned, parseCastAsUnsigned],
    [QuerySyntaxEnum.Ceil, parseCeil],
    [QuerySyntaxEnum.CharLength, parseCharLength],
    [QuerySyntaxEnum.Concat, parseConcat],
    [QuerySyntaxEnum.ConcatWs, parseConcatWs],
    [QuerySyntaxEnum.Cos, parseCos],
    [QuerySyntaxEnum.Cot, parseCot],
    [QuerySyntaxEnum.Degrees, parseDegrees],
    [QuerySyntaxEnum.Div, parseDiv],
    [QuerySyntaxEnum.Exp, parseExp],
    [QuerySyntaxEnum.Field, parseField],
    [QuerySyntaxEnum.FindInSet, parseFindInSet],
    [QuerySyntaxEnum.Floor, parseFloor],
    [QuerySyntaxEnum.Format, parseFormat],
    [QuerySyntaxEnum.InsertString, parseInsertString],
    [QuerySyntaxEnum.Instr, parseInstr],
    [QuerySyntaxEnum.Lcase, parseLcase],
    [QuerySyntaxEnum.Left, parseLeft],
    [QuerySyntaxEnum.Length, parseLength],
    [QuerySyntaxEnum.Ln, parseLn],
    [QuerySyntaxEnum.Locate, parsePosition],
    [QuerySyntaxEnum.Log, parseLog],
    [QuerySyntaxEnum.Log10, parseLog10],
    [QuerySyntaxEnum.Log2, parseLog2],
    [QuerySyntaxEnum.Lpad, parseLPad],
    [QuerySyntaxEnum.Ltrim, parseLTrim],
    [QuerySyntaxEnum.Mid, parseMid],
    [QuerySyntaxEnum.Mod, parseMod],
    [QuerySyntaxEnum.Pi, parsePi],
    [QuerySyntaxEnum.Position, parsePosition],
    [QuerySyntaxEnum.Pow, parsePow],
    [QuerySyntaxEnum.Radians, parseRadians],
    [QuerySyntaxEnum.Rand, parseRand],
    [QuerySyntaxEnum.Repeat, parseRepeat],
    [QuerySyntaxEnum.ReplaceString, parseReplace],
    [QuerySyntaxEnum.Reverse, parseReverse],
    [QuerySyntaxEnum.Right, parseRight],
    [QuerySyntaxEnum.Round, parseRound],
    [QuerySyntaxEnum.Rpad, parseRPad],
    [QuerySyntaxEnum.Rtrim, parseRTrim],
    [QuerySyntaxEnum.Sign, parseSign],
    [QuerySyntaxEnum.Sin, parseSin],
    [QuerySyntaxEnum.Space, parseSpace],
    [QuerySyntaxEnum.Strcmp, parseStrcmp],
    [QuerySyntaxEnum.Substr, parseSubstr],
    [QuerySyntaxEnum.SubstringIndex, parseSubstringIndex],
    [QuerySyntaxEnum.Tan, parseTan],
    [QuerySyntaxEnum.Trim, parseTrim],
    [QuerySyntaxEnum.Truncate, parseTruncate],
    [QuerySyntaxEnum.Ucase, parseUcase]
]);

const applyMySQLFunctions = (functions: FunctionData[]) => (rows: Row[], key: string, map: DataMap): void => {
    const parsedRows: Row[] = [];
    functions.forEach((fnData: FunctionData) => {
        const mySQLFn: Fn<Row[]> = mysqlFunctions.get(fnData.type);
        if (mySQLFn) {
            const fnResults:Row[] = mySQLFn(rows, fnData);
            parsedRows.push(...fnResults);
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