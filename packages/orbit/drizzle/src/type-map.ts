// Drizzle column type → Orbit value type. Ported from drizzle-zero's
// `drizzle-to-zero.ts` (Orbit's value types are the same set Zero uses:
// string / number / boolean / json). Resolution order, per column:
//   1. by Drizzle `columnType` (e.g. `PgText`)         — most specific
//   2. by Drizzle `dataType`   (e.g. `number`)
//   3. by raw Postgres SQL type (`getSQLType()`)        — broad fallback

import type { OrbitValueType } from '../../orm-core/src/index.ts';

/** Drizzle `dataType` → Orbit type. */
export const dataTypeToOrbit = {
  number: 'number',
  bigint: 'number',
  boolean: 'boolean',
  date: 'number',
} as const satisfies Record<string, OrbitValueType>;

/** Drizzle `columnType` (dialect-specific) → Orbit type. */
export const columnTypeToOrbit = {
  PgText: 'string',
  PgChar: 'string',
  PgVarchar: 'string',
  PgUUID: 'string',
  PgEnumColumn: 'string',
  PgJsonb: 'json',
  PgJson: 'json',
  PgNumeric: 'number',
  PgDateString: 'number',
  PgTime: 'number',
  PgTimestampString: 'number',
  PgArray: 'json',
  // MySQL / SQLite analogues (best-effort, so non-pg dialects map too)
  MySqlText: 'string',
  MySqlVarChar: 'string',
  MySqlChar: 'string',
  MySqlJson: 'json',
  MySqlInt: 'number',
  MySqlBigInt53: 'number',
  MySqlBoolean: 'boolean',
  SQLiteText: 'string',
  SQLiteInteger: 'number',
  SQLiteReal: 'number',
  SQLiteBlobJson: 'json',
  SQLiteBoolean: 'boolean',
} as const satisfies Record<string, OrbitValueType>;

/** Raw Postgres SQL type name → Orbit type. */
export const sqlTypeToOrbit: Record<string, OrbitValueType> = {
  text: 'string',
  char: 'string',
  character: 'string',
  varchar: 'string',
  'character varying': 'string',
  uuid: 'string',
  enum: 'string',
  jsonb: 'json',
  json: 'json',
  numeric: 'number',
  decimal: 'number',
  int: 'number',
  integer: 'number',
  smallint: 'number',
  bigint: 'number',
  int2: 'number',
  int4: 'number',
  int8: 'number',
  real: 'number',
  float4: 'number',
  float8: 'number',
  'double precision': 'number',
  serial: 'number',
  bigserial: 'number',
  date: 'number',
  time: 'number',
  'time without time zone': 'number',
  'time with time zone': 'number',
  timetz: 'number',
  timestamp: 'number',
  'timestamp without time zone': 'number',
  'timestamp with time zone': 'number',
  timestamptz: 'number',
  boolean: 'boolean',
  bool: 'boolean',
};

/** Minimal shape of a Drizzle column we read for type resolution. */
export interface DrizzleColumnLike {
  readonly columnType?: string;
  readonly dataType?: string;
  getSQLType?(): string;
}

/** Resolve a Drizzle column to an Orbit value type, or `null` if unsupported. */
export function orbitTypeOf(column: DrizzleColumnLike): OrbitValueType | null {
  const byColumnType = column.columnType && (columnTypeToOrbit as Record<string, OrbitValueType>)[column.columnType];
  if (byColumnType) return byColumnType;
  const byDataType = column.dataType && (dataTypeToOrbit as Record<string, OrbitValueType>)[column.dataType];
  if (byDataType) return byDataType;
  const sql = column.getSQLType?.();
  if (sql) {
    const base = sql.replace(/\(.*$/, '').replace(/\[\]$/, '').trim().toLowerCase();
    if (sql.endsWith('[]')) return 'json';
    if (sqlTypeToOrbit[base]) return sqlTypeToOrbit[base];
  }
  return null;
}
