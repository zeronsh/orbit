// Wire protocol types + AST, matching the Rust `orbit-protocol` / `oql` JSON
// (which is byte-compatible with Zero's). Keeping these in sync is what lets the
// existing Zero TypeScript client talk to an Orbit server unchanged.

export type Value = string | number | boolean | null;
export type Row = Record<string, Value | unknown>;

export type Direction = 'asc' | 'desc';
export type OrderPart = readonly [field: string, dir: Direction];

export type SimpleOperator =
  | '=' | '!=' | 'IS' | 'IS NOT'
  | '<' | '>' | '<=' | '>='
  | 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE'
  | 'IN' | 'NOT IN';

export type ValuePosition =
  | { type: 'literal'; value: Value | readonly (string | number | boolean)[] }
  | { type: 'column'; name: string }
  | { type: 'static'; anchor: 'authData' | 'preMutationRow'; field: string | string[] };

export type Condition =
  | { type: 'simple'; op: SimpleOperator; left: ValuePosition; right: ValuePosition }
  | { type: 'and'; conditions: Condition[] }
  | { type: 'or'; conditions: Condition[] }
  | { type: 'correlatedSubquery'; related: CorrelatedSubquery; op: 'EXISTS' | 'NOT EXISTS' };

export type Correlation = { parentField: string[]; childField: string[] };
export type CorrelatedSubquery = {
  correlation: Correlation;
  subquery: AST;
  hidden?: boolean;
  /** A `.one()` relationship — the client unwraps it to a single row (or undefined). */
  singular?: boolean;
};

export type AST = {
  table: string;
  alias?: string;
  where?: Condition;
  related?: CorrelatedSubquery[];
  start?: { row: Row; exclusive: boolean };
  limit?: number;
  orderBy?: OrderPart[];
};

// --- patches & messages -----------------------------------------------------

export type RowPatchOp =
  | { op: 'put'; tableName: string; value: Row }
  | { op: 'update'; tableName: string; id: Row; merge?: Row; constrain?: string[] }
  | { op: 'del'; tableName: string; id: Row }
  | { op: 'clear' };

export type QueriesPatchOp =
  | { op: 'put'; hash: string; ttl?: number; ast?: AST; name?: string; args?: unknown[] }
  | { op: 'del'; hash: string }
  | { op: 'clear' };

export type Downstream =
  | ['connected', { wsid: string; timestamp?: number }]
  | ['pokeStart', { pokeID: string; baseCookie: string | null }]
  | ['pokePart', {
      pokeID: string;
      lastMutationIDChanges?: Record<string, number>;
      gotQueriesPatch?: QueriesPatchOp[];
      rowsPatch?: RowPatchOp[];
    }]
  | ['pokeEnd', { pokeID: string; cookie: string; cancel?: boolean }]
  | ['pong', Record<string, never>]
  | ['error', { kind: string; message: string }];

export type CrudOp =
  | { op: 'insert'; tableName: string; primaryKey: string[]; value: Row }
  | { op: 'upsert'; tableName: string; primaryKey: string[]; value: Row }
  | { op: 'update'; tableName: string; primaryKey: string[]; value: Row }
  | { op: 'delete'; tableName: string; primaryKey: string[]; value: Row };

export type Mutation =
  | { type: 'crud'; id: number; clientID: string; name: '_zero_crud'; args: [{ ops: CrudOp[] }]; timestamp: number }
  | { type: 'custom'; id: number; clientID: string; name: string; args: unknown[]; timestamp: number };

export type Upstream =
  | ['initConnection', { desiredQueriesPatch: QueriesPatchOp[] }]
  | ['changeDesiredQueries', { desiredQueriesPatch: QueriesPatchOp[] }]
  | ['push', {
      clientGroupID: string;
      mutations: Mutation[];
      pushVersion: number;
      timestamp: number;
      requestID: string;
    }]
  | ['ping', Record<string, never>];

export const PROTOCOL_VERSION = 51;

/** A simple stable string hash; the server treats the result as an opaque key. */
export function hashString(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(36);
}

/** Stable hash of a query AST (matches the server's expectation of a string key). */
export function hashAST(ast: AST): string {
  return hashString(JSON.stringify(ast));
}
