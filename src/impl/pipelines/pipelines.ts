import { runSelectPipeline } from "./select";
import { runUpdatePipeline } from "./update";
import { runDeletePipeline } from "./delete";
import { runInsertPipeline } from "./insert";
import { QuerySyntaxEnum, Fn } from '@chego/chego-api';

export const pipelines: Map<QuerySyntaxEnum, Fn<Promise<any>>> = new Map<QuerySyntaxEnum, Fn<Promise<any>>>([
    [QuerySyntaxEnum.Select, runSelectPipeline],
    [QuerySyntaxEnum.Update, runUpdatePipeline],
    [QuerySyntaxEnum.Delete, runDeletePipeline],
    [QuerySyntaxEnum.Insert, runInsertPipeline]
]);