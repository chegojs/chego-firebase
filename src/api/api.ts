import { Row } from '@chego/chego-database-boilerplate';
import { Fn } from '@chego/chego-api';

export type ConditionFunction = Fn<number> | Fn<string>;
export type Condition = IConditionFunctionArray | ConditionFunction;

export interface IConditionFunctionArray extends Array<Condition> {}

export interface IConditions {
    test(row:Row):boolean;
}
