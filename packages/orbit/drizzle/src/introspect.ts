// Read a Drizzle schema into Orbit's normalized {@link SchemaIR}.
//
// Relationships come from (in priority order):
//   1. Drizzle Relations v2 — `defineRelations(...)` (the v1.0 RC API), including
//      many-to-many via `.through(...)` (emitted as a junction chain).
//   2. Foreign keys (`.references(...)`) — used to synthesize `one`/`many`
//      relationships when no Relations object is supplied.
//
// Column names use the **database** column name (Orbit syncs raw Postgres rows),
// so Orbit field names line up with what replication delivers.

import { getTableColumns, getTableName, is, Many, One, Table } from 'drizzle-orm';
import { getTableConfig } from 'drizzle-orm/pg-core';
import type { IRColumn, IRConnection, IRRelationship, IRTable, SchemaIR } from '../../orm-core/src/index.ts';
import { orbitTypeOf, type DrizzleColumnLike } from './type-map.ts';

export interface DrizzleAdapterConfig {
  /**
   * The Relations v2 object (`defineRelations(...)`), if it isn't one of the
   * exports in the schema you passed. When omitted, it is auto-detected among the
   * schema's exports; failing that, relationships are derived from foreign keys.
   */
  readonly relations?: unknown;
  /**
   * Per-table selection. Omit for "all tables, all columns". `false` excludes a
   * table; an object selects columns (`{ id: true, secret: false }`). Primary-key
   * columns are always included.
   */
  readonly tables?: Record<string, boolean | Record<string, boolean>>;
  /**
   * Custom TypeScript types per `table.column` (database names), supplied by the
   * CLI's type resolver so `$type<>()` survives into codegen. Runtime callers
   * can't read `$type` (it is type-level only), so this is normally CLI-only.
   */
  readonly customTypes?: Record<string, Record<string, string>>;
  /** Synthesize relationships from foreign keys when no Relations object exists. Default true. */
  readonly fkRelationships?: boolean;
  /** Print what was skipped / inferred. */
  readonly debug?: boolean;
}

type AnyRecord = Record<string, unknown>;

function debugLog(on: boolean | undefined, ...args: unknown[]): void {
  if (on) console.error('[orbit-drizzle]', ...args);
}

/** pg `getTableConfig`, with a fallback for non-pg tables. */
function tableConfig(table: Table): { name: string; primaryKeyColumns: string[][]; foreignKeys: { columns: string[]; foreignTable: string; foreignColumns: string[] }[] } {
  try {
    const cfg = getTableConfig(table as Parameters<typeof getTableConfig>[0]);
    return {
      name: cfg.name,
      primaryKeyColumns: cfg.primaryKeys.map((pk) => pk.columns.map((c) => c.name)),
      foreignKeys: cfg.foreignKeys.map((fk) => {
        const ref = fk.reference();
        return {
          columns: ref.columns.map((c) => c.name),
          foreignTable: getTableName(ref.foreignTable),
          foreignColumns: ref.foreignColumns.map((c) => c.name),
        };
      }),
    };
  } catch {
    return { name: getTableName(table), primaryKeyColumns: [], foreignKeys: [] };
  }
}

function isTableIncluded(tableName: string, config: DrizzleAdapterConfig): boolean {
  if (!config.tables) return true;
  const entry = config.tables[tableName];
  return entry !== false && entry !== undefined;
}

function isColumnIncluded(tableName: string, columnName: string, isPk: boolean, config: DrizzleAdapterConfig): boolean {
  if (isPk) return true; // PKs are always needed
  if (!config.tables) return true;
  const entry = config.tables[tableName];
  if (entry === undefined || typeof entry === 'boolean') return true;
  return entry[columnName] !== false; // included unless explicitly excluded
}

function buildColumn(
  tableName: string,
  dbName: string,
  column: DrizzleColumnLike & { notNull?: boolean; primary?: boolean; hasDefault?: boolean; defaultFn?: unknown; enumValues?: readonly string[] },
  isPk: boolean,
  config: DrizzleAdapterConfig,
): IRColumn | null {
  const type = orbitTypeOf(column);
  if (!type) {
    debugLog(config.debug, `skipping ${tableName}.${dbName}: unsupported type ${column.columnType ?? column.dataType ?? column.getSQLType?.()}`);
    return null;
  }
  const hasDefault = Boolean(column.hasDefault) || typeof column.defaultFn !== 'undefined';
  const optional = isPk ? false : hasDefault ? true : !column.notNull;

  // custom type: CLI-resolved $type<>() wins; otherwise enums become a string union.
  const cliCustom = config.customTypes?.[tableName]?.[dbName];
  const enumCustom = column.enumValues && column.enumValues.length
    ? column.enumValues.map((v) => JSON.stringify(v)).join(' | ')
    : undefined;

  return {
    name: dbName,
    type,
    optional,
    customType: cliCustom ?? enumCustom,
    sourceType: column.columnType ?? column.dataType,
  };
}

/** Find a Relations v2 (`defineRelations`) object: keyed by table → `{ table, relations }`. */
function findRelationsObject(input: AnyRecord, config: DrizzleAdapterConfig): AnyRecord | undefined {
  if (config.relations) return config.relations as AnyRecord;
  for (const value of Object.values(input)) {
    if (value && typeof value === 'object' && !is(value, Table)) {
      const vals = Object.values(value as AnyRecord);
      if (vals.length > 0 && vals.every((v) => v && typeof v === 'object' && is((v as AnyRecord).table, Table) && 'relations' in (v as AnyRecord))) {
        return value as AnyRecord;
      }
    }
  }
  return undefined;
}

function throughColumnNames(side: unknown): string[] {
  // `through.source` / `through.target`: array of RelationsBuilderColumn wrappers
  // `{ _: { tableName, column, key } }` where `column` is the junction Drizzle column.
  if (!Array.isArray(side)) return [];
  return side.map((w) => (w as AnyRecord)?._ as AnyRecord).map((inner) => (inner?.column as AnyRecord)?.name as string).filter(Boolean);
}

function relationToIR(sourceTableName: string, relName: string, rel: AnyRecord, included: Set<string>): IRRelationship | null {
  const cardinality = is(rel, One) ? 'one' : 'many';
  const sourceFields = (rel.sourceColumns as { name: string }[] | undefined)?.map((c) => c.name) ?? [];
  const targetFields = (rel.targetColumns as { name: string }[] | undefined)?.map((c) => c.name) ?? [];
  const destName = rel.targetTable ? getTableName(rel.targetTable as Table) : undefined;
  if (!destName || !sourceFields.length || !targetFields.length) return null;

  // junction (many-to-many) via `.through(...)`
  if (rel.throughTable) {
    const junctionName = getTableName(rel.throughTable as Table);
    const through = rel.through as AnyRecord | undefined;
    const throughSrc = throughColumnNames(through?.source);
    const throughTgt = throughColumnNames(through?.target);
    if (!throughSrc.length || !throughTgt.length) return null;
    if (!included.has(destName) || !included.has(junctionName)) return null;
    const chain: IRConnection[] = [
      { sourceField: sourceFields, destField: throughSrc, destSchema: junctionName, cardinality: 'many' },
      { sourceField: throughTgt, destField: targetFields, destSchema: destName, cardinality: 'many' },
    ];
    return { table: sourceTableName, name: relName, chain };
  }

  if (!included.has(destName)) return null;
  return {
    table: sourceTableName,
    name: relName,
    chain: [{ sourceField: sourceFields, destField: targetFields, destSchema: destName, cardinality }],
  };
}

/** Heuristic relationship name from an FK column (drops a trailing `Id`/`_id`). */
function fkRelName(fkColumn: string, fallback: string): string {
  const m = fkColumn.replace(/(_id|Id)$/, '');
  return m && m !== fkColumn ? m : fallback;
}

/** Convert a Drizzle schema (`import * as schema`) into Orbit's {@link SchemaIR}. */
export function drizzleToIR(input: AnyRecord, config: DrizzleAdapterConfig = {}): SchemaIR {
  // 1. Tables + columns.
  const tables: IRTable[] = [];
  const tableInstances = new Map<string, Table>();
  const included = new Set<string>();

  for (const value of Object.values(input)) {
    if (!is(value, Table)) continue;
    const table = value as Table;
    const tableName = getTableName(table);
    if (!isTableIncluded(tableName, config)) {
      debugLog(config.debug, `skipping table ${tableName} (excluded by config)`);
      continue;
    }
    tableInstances.set(tableName, table);
    included.add(tableName);
  }

  for (const [tableName, table] of tableInstances) {
    const cfg = tableConfig(table);
    const columns = getTableColumns(table) as Record<string, DrizzleColumnLike & { name: string; primary?: boolean }>;

    const pk = new Set<string>();
    for (const col of Object.values(columns)) if ((col as { primary?: boolean }).primary) pk.add(col.name);
    for (const pkCols of cfg.primaryKeyColumns) for (const c of pkCols) pk.add(c);

    const irColumns: IRColumn[] = [];
    for (const col of Object.values(columns)) {
      const isPk = pk.has(col.name);
      if (!isColumnIncluded(tableName, col.name, isPk, config)) continue;
      const irCol = buildColumn(tableName, col.name, col as never, isPk, config);
      if (irCol) irColumns.push(irCol);
    }

    if (pk.size === 0) {
      throw new Error(`@orbit/drizzle: table "${tableName}" has no primary key — Orbit requires one. Add .primaryKey() or primaryKey({columns}).`);
    }
    tables.push({ name: tableName, columns: irColumns, primaryKey: [...pk] });
  }

  // 2. Relationships — Relations v2 first, else foreign keys.
  const relationships: IRRelationship[] = [];
  const relationsObj = findRelationsObject(input, config);
  const seen = new Set<string>(); // `${table}.${name}` dedupe

  if (relationsObj) {
    for (const node of Object.values(relationsObj)) {
      const n = node as AnyRecord;
      if (!is(n.table, Table)) continue;
      const sourceTableName = getTableName(n.table as Table);
      if (!included.has(sourceTableName)) continue;
      const rels = n.relations as AnyRecord;
      for (const [relName, rel] of Object.entries(rels)) {
        const ir = relationToIR(sourceTableName, relName, rel as AnyRecord, included);
        if (ir) {
          relationships.push(ir);
          seen.add(`${sourceTableName}.${relName}`);
        }
      }
    }
  } else if (config.fkRelationships !== false) {
    // Derive from foreign keys: a `one` on the child + a `many` back on the parent.
    for (const [tableName, table] of tableInstances) {
      const cfg = tableConfig(table);
      for (const fk of cfg.foreignKeys) {
        const parent = fk.foreignTable;
        if (!included.has(parent)) continue;
        const oneName = uniqueName(fkRelName(fk.columns[0] ?? parent, parent), tableName, seen);
        relationships.push({
          table: tableName,
          name: oneName,
          chain: [{ sourceField: fk.columns, destField: fk.foreignColumns, destSchema: parent, cardinality: 'one' }],
        });
        const manyName = uniqueName(`${tableName}s`, parent, seen);
        relationships.push({
          table: parent,
          name: manyName,
          chain: [{ sourceField: fk.foreignColumns, destField: fk.columns, destSchema: tableName, cardinality: 'many' }],
        });
      }
    }
  }

  return { tables, relationships };
}

function uniqueName(base: string, table: string, seen: Set<string>): string {
  let name = base;
  let i = 2;
  while (seen.has(`${table}.${name}`)) name = `${base}${i++}`;
  seen.add(`${table}.${name}`);
  return name;
}
