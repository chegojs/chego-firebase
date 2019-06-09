import { SortingData, AnyButFunction } from '@chego/chego-api';
import { basicSort, IQueryContext } from '@chego/chego-database-boilerplate';

const compareUsing = (sorters:SortingData[]) => (a:any, b:any):number => {
    const sortBy:SortingData = sorters[0];
    const by:string = sortBy.property.name;
    const valueA:AnyButFunction = (typeof a[by] === 'string') ? String(a[by]).toLowerCase() : a[by];
    const valueB:AnyButFunction = (typeof b[by] === 'string') ? String(b[by]).toLowerCase() : b[by];
    const sortResult:number = basicSort(valueA, valueB, sortBy.order);

    return sortResult === 0 && sorters.length > 1 ? compareUsing(sorters.slice(1))(a,b) : sortResult;
}

export const orderResultsIfRequired = (queryContext: IQueryContext) => (data: any): any => {
    if (queryContext.orderBy.length) {
        Object.keys(data).forEach((tableName:string)=>{
            data[tableName].sort(compareUsing(queryContext.orderBy));
        });
    }
    return data;
}