import { Row } from '@chego/chego-database-boilerplate';

export interface IConditions {
    test(row:Row):boolean;
}