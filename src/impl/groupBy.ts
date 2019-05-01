import { basicSort } from './utils';
import { IQueryContext } from '../api/firebaseInterfaces';
import { OutputDataSnapshot } from '../api/firebaseTypes';
import { SortingData, AnyButFunction } from '@chego/chego-api';

const compareUsing = (sorters: SortingData[]) => (a: any, b: any): number => {
    const sortBy: SortingData = sorters[0];
    const by: string = sortBy.property.name;
    const valueA: AnyButFunction = (typeof a[by] === 'string') ? String(a[by]).toLowerCase() : a[by];
    const valueB: AnyButFunction = (typeof b[by] === 'string') ? String(b[by]).toLowerCase() : b[by];
    const sortResult: number = basicSort(valueA, valueB, sortBy.order);

    return sortResult === 0 && sorters.length > 1 ? compareUsing(sorters.slice(1))(a, b) : sortResult;
}

const pickRepresentative = (by: string) => (list: any[], current: any) => {
    const previous = list[list.length - 1];
    if (!previous || (previous && previous[by] !== current[by])) {
        list.push(current)
    }
    return list;
}

export const groupResultsIfRequired = (queryContext: IQueryContext) => (data: OutputDataSnapshot): any => {
    const groupByLength: number = queryContext.groupBy.length;
    if (groupByLength) {
        const result:OutputDataSnapshot = {};
        Object.keys(data).forEach((tableName: string) => {
            const by: string = queryContext.groupBy[groupByLength - 1].property.name;
            const groupedRows:any[] = data[tableName].sort(compareUsing(queryContext.groupBy)).reduce(pickRepresentative(by), []);
            Object.assign(result, {[tableName]:groupedRows});
        });
        return result;
    }
    return data;
}