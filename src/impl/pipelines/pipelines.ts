import { runSelectPipeline } from "./select";
import { runUpdatePipeline } from "./update";
import { runDeletePipeline } from "./delete";
import { runInsertPipeline } from "./insert";
import { CompileFunction } from '../../api/firebaseTypes';
import { QuerySyntaxEnum } from '@chego/chego-api';

export const pipelines: Map<QuerySyntaxEnum, CompileFunction> = new Map<QuerySyntaxEnum, CompileFunction>([
    [QuerySyntaxEnum.Select, runSelectPipeline],
    [QuerySyntaxEnum.Update, runUpdatePipeline],
    [QuerySyntaxEnum.Delete, runDeletePipeline],
    [QuerySyntaxEnum.Insert, runInsertPipeline]
]);