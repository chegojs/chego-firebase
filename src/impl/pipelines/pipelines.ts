import { runSelectPipeline } from "./select";
import { runUpdatePipeline } from "./update";
import { runDeletePipeline } from "./delete";
import { runInsertPipeline } from "./insert";
import { QuerySyntaxEnum } from '@chego/chego-api';
import { QueryPipeline } from '@chego/chego-database-boilerplate';

export const pipelines: Map<QuerySyntaxEnum, QueryPipeline> = new Map<QuerySyntaxEnum, QueryPipeline>([
    [QuerySyntaxEnum.Select, runSelectPipeline],
    [QuerySyntaxEnum.Update, runUpdatePipeline],
    [QuerySyntaxEnum.Delete, runDeletePipeline],
    [QuerySyntaxEnum.Insert, runInsertPipeline]
]);