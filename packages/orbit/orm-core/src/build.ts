// Runtime: turn a {@link SchemaIR} into a live Orbit `SchemaDef` (the same object
// `createSchema(...)` would produce). Used by adapters' `defineOrbitSchema(...)`
// runtime entry point. Custom `$type<>()` types are type-level only and do not
// exist at runtime, so this produces the plain column types; for full custom-type
// fidelity, use the codegen emitter (`emit.ts`) instead.

import {
  boolean as boolCol,
  createSchema,
  json as jsonCol,
  number as numCol,
  optional,
  relationships,
  string as strCol,
  table,
  type Column,
  type RelationshipsDef,
  type SchemaDef,
  type TableDef,
} from '../../client/src/index.ts';
import type { IRColumn, IRRelationship, SchemaIR } from './ir.ts';

function makeColumn(c: IRColumn): Column {
  const base =
    c.type === 'number' ? numCol() : c.type === 'boolean' ? boolCol() : c.type === 'json' ? jsonCol() : strCol();
  return c.optional ? optional(base as Column) : (base as Column);
}

/** Build a live Orbit schema object from the IR. */
export function buildOrbitSchema(ir: SchemaIR): SchemaDef {
  const tableDefs = new Map<string, TableDef>();
  const tables: TableDef[] = [];

  for (const t of ir.tables) {
    const columns: Record<string, Column> = {};
    for (const c of t.columns) columns[c.name] = makeColumn(c);
    const pk = (t.primaryKey.length ? t.primaryKey : ['id']) as [string, ...string[]];
    const def = table(t.name)
      .columns(columns as never)
      .primaryKey(...pk) as unknown as TableDef;
    tableDefs.set(t.name, def);
    tables.push(def);
  }

  // `relationships()` is per source table, and `createSchema` rejects a table
  // whose relationships are declared twice — so group all of a table's
  // relationships into one call.
  const byTable = new Map<string, IRRelationship[]>();
  for (const r of ir.relationships) {
    const list = byTable.get(r.table) ?? [];
    list.push(r);
    byTable.set(r.table, list);
  }

  const rels: RelationshipsDef[] = [];
  for (const [tableName, list] of byTable) {
    const src = tableDefs.get(tableName);
    if (!src) continue;
    rels.push(
      relationships(src, ({ one, many }) => {
        const out: Record<string, unknown> = {};
        for (const r of list) {
          const last = r.chain[r.chain.length - 1];
          const connect = last.cardinality === 'one' ? one : many;
          const args = r.chain.map((c) => {
            const dest = tableDefs.get(c.destSchema);
            if (!dest) throw new Error(`@orbit/orm-core: relationship "${tableName}.${r.name}" references unknown table "${c.destSchema}"`);
            return { sourceField: [...c.sourceField], destField: [...c.destField], destSchema: dest };
          });
          // `one`/`many` accept 1 (direct) or 2 (junction) hops.
          out[r.name] = (connect as (...a: unknown[]) => unknown)(...args);
        }
        return out as never;
      }),
    );
  }

  return createSchema({ tables: tables as never, relationships: rels as never });
}
