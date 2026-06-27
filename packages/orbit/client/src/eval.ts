// Client-side query evaluator — the local IVM that gives Zero-like reads:
// `where`/`orderBy`/`limit`/`start`/`related` (nested results) + `EXISTS`,
// evaluated over the local row store (synced rows + optimistic overlay). Run on
// every store change (non-incremental, but correct; client datasets are small).
//
// Mirrors the value semantics of the Rust engine (`compareValues` /
// `valuesEqual`), so client results match what the server materializes.

import type { AST, Condition, CorrelatedSubquery, Correlation, OrderPart, Row, SimpleOperator, Value, ValuePosition } from './protocol.ts';

/** A nested query result row: columns plus any `related` arrays under their alias. */
export type ResultRow = Record<string, unknown>;

function isNull(v: unknown): v is null | undefined {
  return v === null || v === undefined;
}

function typeRank(v: Value): number {
  if (isNull(v)) return 0;
  switch (typeof v) {
    case 'boolean':
      return 1;
    case 'number':
      return 2;
    case 'string':
      return 3;
    default:
      return 4;
  }
}

/** Total order with `null == null` (sorting semantics; mirrors `compare_values`). */
export function compareValues(a: Value, b: Value): number {
  if (isNull(a) && isNull(b)) return 0;
  if (isNull(a)) return -1;
  if (isNull(b)) return 1;
  if (typeof a === typeof b) {
    if (typeof a === 'number') return a < (b as number) ? -1 : a > (b as number) ? 1 : 0;
    if (typeof a === 'boolean') return a === b ? 0 : a ? 1 : -1;
    if (typeof a === 'string') return a < (b as string) ? -1 : a > (b as string) ? 1 : 0;
    return 0;
  }
  return typeRank(a) - typeRank(b);
}

/** Equality with `null != null` (join / `=` semantics; mirrors `values_equal`). */
export function valuesEqual(a: Value, b: Value): boolean {
  if (isNull(a) || isNull(b)) return false;
  return a === b;
}

function likeToRegExp(pattern: string, ci: boolean): RegExp {
  let re = '';
  for (const ch of pattern) {
    if (ch === '%') re += '.*';
    else if (ch === '_') re += '.';
    else re += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`^${re}$`, ci ? 'is' : 's');
}

function resolve(row: Row, pos: ValuePosition): Value | readonly Value[] | undefined {
  if (pos.type === 'column') return row[pos.name] as Value;
  if (pos.type === 'literal') return pos.value as Value | readonly Value[];
  return undefined; // 'static' (authData/preMutationRow) — server-side only
}

function evalSimple(row: Row, op: SimpleOperator, left: ValuePosition, right: ValuePosition): boolean {
  const l = resolve(row, left);
  const r = resolve(row, right);
  if (l === undefined || r === undefined) return true; // unsupported position → don't filter

  if (op === 'IS') return isNull(l as Value) === isNull(r as Value) && (isNull(l as Value) || l === r);
  if (op === 'IS NOT') return !(isNull(l as Value) === isNull(r as Value) && (isNull(l as Value) || l === r));

  if (op === 'IN' || op === 'NOT IN') {
    // SQL three-valued logic: `NULL IN (…)` and `NULL NOT IN (…)` are both unknown,
    // so the row is excluded either way (matches Zero / Postgres).
    if (isNull(l as Value)) return false;
    const arr = (Array.isArray(r) ? r : [r]) as Value[];
    const found = arr.some((x) => valuesEqual(l as Value, x));
    return op === 'IN' ? found : !found;
  }

  const a = l as Value;
  const b = r as Value;
  switch (op) {
    case '=':
      return valuesEqual(a, b);
    case '!=':
      return !isNull(a) && !isNull(b) && a !== b;
    case '<':
      return !isNull(a) && !isNull(b) && compareValues(a, b) < 0;
    case '>':
      return !isNull(a) && !isNull(b) && compareValues(a, b) > 0;
    case '<=':
      return !isNull(a) && !isNull(b) && compareValues(a, b) <= 0;
    case '>=':
      return !isNull(a) && !isNull(b) && compareValues(a, b) >= 0;
    case 'LIKE':
      return !isNull(a) && likeToRegExp(String(b), false).test(String(a));
    case 'NOT LIKE':
      return !isNull(a) && !likeToRegExp(String(b), false).test(String(a));
    case 'ILIKE':
      return !isNull(a) && likeToRegExp(String(b), true).test(String(a));
    case 'NOT ILIKE':
      return !isNull(a) && !likeToRegExp(String(b), true).test(String(a));
    default:
      return true;
  }
}

/** Evaluate a single `simple` condition against a row (reused by the IVM engine). */
export function simpleMatches(
  row: Row,
  cond: { op: SimpleOperator; left: ValuePosition; right: ValuePosition },
): boolean {
  return evalSimple(row, cond.op, cond.left, cond.right);
}

function compareByOrder(a: Row, b: Row, orderBy: readonly OrderPart[]): number {
  for (const [field, dir] of orderBy) {
    const c = compareValues(a[field] as Value, b[field] as Value);
    if (c !== 0) return dir === 'asc' ? c : -c;
  }
  return 0;
}

/**
 * Append any primary-key columns missing from `orderBy` (ascending), so the
 * order is total — mirrors the Rust engine's `complete_ordering` / Zero's
 * `completeOrdering`. With no `orderBy` this yields pure primary-key order,
 * matching how the server materializes (source rows are pk-ordered).
 */
function completeOrder(orderBy: readonly OrderPart[] | undefined, pk: readonly string[]): OrderPart[] {
  const order: OrderPart[] = [...(orderBy ?? [])];
  for (const k of pk) if (!order.some(([f]) => f === k)) order.push([k, 'asc']);
  return order;
}

function correlated(parent: Row, child: Row, c: Correlation): boolean {
  return c.parentField.every((pf, i) => valuesEqual(parent[pf] as Value, child[c.childField[i]] as Value));
}

/**
 * Visible relationships contributed by `EXISTS`/`NOT EXISTS` conditions: a
 * correlated subquery with an explicit alias is materialized into the output
 * (the matched children for EXISTS, `[]` for NOT EXISTS) in addition to acting
 * as a filter — exactly like Zero. Auto-generated `zsubq_*` ones stay hidden.
 */
export function existsRelationships(cond: Condition | undefined): CorrelatedSubquery[] {
  const out: CorrelatedSubquery[] = [];
  const walk = (c: Condition | undefined): void => {
    if (!c) return;
    if (c.type === 'correlatedSubquery') {
      const alias = c.related.subquery.alias;
      if (alias && !alias.startsWith('zsubq_')) out.push(c.related);
    } else if (c.type === 'and' || c.type === 'or') {
      for (const inner of c.conditions) walk(inner);
    }
  };
  walk(cond);
  return out;
}

/**
 * Lift the destination rows out of a junction (`hidden`) relationship layer.
 * `junctionRows` are the rows of the junction table (already carrying their
 * nested destination rows under the inner alias); we concatenate those nested
 * rows and drop the junction layer entirely — mirroring Zero's junction view.
 */
function liftHidden(junctionRows: ResultRow[], junctionAst: AST): ResultRow[] {
  const inner = junctionAst.related?.[0];
  if (!inner) return [];
  const innerAlias = inner.subquery.alias ?? inner.subquery.table;
  return junctionRows.flatMap((jr) => (jr[innerAlias] as ResultRow[]) ?? []);
}

/**
 * Evaluate `ast` against `getRows` (a function returning all current rows of a
 * table). Returns nested result rows. Set `applyWhere: false` for server-resolved
 * (named) queries whose `where` was already applied server-side.
 */
export function evaluate(
  getRows: (table: string) => Row[],
  ast: AST,
  opts: { applyWhere?: boolean; pkOf?: (table: string) => readonly string[] } = {},
): ResultRow[] {
  const applyWhere = opts.applyWhere ?? true;
  const pkOf = opts.pkOf ?? (() => ['id']);

  const run = (node: AST, parent: Row | null, corr: Correlation | null): ResultRow[] => {
    let rows = getRows(node.table);
    if (parent && corr) rows = rows.filter((r) => correlated(parent, r, corr));

    const evalCond = (r: Row, cond: Condition): boolean => {
      switch (cond.type) {
        case 'simple':
          return evalSimple(r, cond.op, cond.left, cond.right);
        case 'and':
          return cond.conditions.every((c) => evalCond(r, c));
        case 'or':
          return cond.conditions.some((c) => evalCond(r, c));
        case 'correlatedSubquery': {
          const has = run(cond.related.subquery, r, cond.related.correlation).length > 0;
          return cond.op === 'EXISTS' ? has : !has;
        }
      }
    };

    if (applyWhere && node.where) rows = rows.filter((r) => evalCond(r, node.where!));
    rows = [...rows];
    const order = completeOrder(node.orderBy, pkOf(node.table));
    rows.sort((a, b) => compareByOrder(a, b, order));

    if (node.start) {
      const { row: cursor, exclusive } = node.start;
      const i = rows.findIndex((r) => {
        const c = compareByOrder(r, cursor, order);
        return exclusive ? c > 0 : c >= 0;
      });
      rows = i < 0 ? [] : rows.slice(i);
    }
    if (node.limit != null) rows = rows.slice(0, node.limit);

    const related = node.related ?? [];
    const existsRels = existsRelationships(node.where);
    if (related.length === 0 && existsRels.length === 0) return rows as ResultRow[];
    return rows.map((r) => {
      const out: ResultRow = { ...r };
      for (const rel of [...related, ...existsRels]) {
        const alias = rel.subquery.alias ?? rel.subquery.table;
        const children = run(rel.subquery, r, rel.correlation);
        // A `hidden` relationship is a junction (many-to-many) layer: drop the
        // junction rows and lift the nested destination rows up under this alias.
        out[alias] = rel.hidden ? liftHidden(children, rel.subquery) : children;
      }
      return out;
    });
  };

  return run(ast, null, null);
}

/**
 * Unwrap `.one()` relationships in a result tree to a single row (or `undefined`),
 * recursively. The raw materialized shape keeps relationships as arrays (matching
 * the server/IVM snapshot); this is the client-facing presentation transform.
 */
export function unwrapSingular(rows: ResultRow[], ast: AST): ResultRow[] {
  const related = ast.related;
  if (!related || related.length === 0) return rows;
  return rows.map((r) => {
    const out: ResultRow = { ...r };
    for (const rel of related) {
      const alias = rel.subquery.alias ?? rel.subquery.table;
      if (rel.hidden) {
        // `out[alias]` was already flattened to destination rows by `liftHidden`;
        // unwrap/recurse using the inner (destination) subquery + its cardinality.
        const inner = rel.subquery.related![0];
        const dest = unwrapSingular((out[alias] as ResultRow[]) ?? [], inner.subquery);
        out[alias] = inner.singular ? dest[0] : dest;
      } else {
        const children = unwrapSingular((out[alias] as ResultRow[]) ?? [], rel.subquery);
        out[alias] = rel.singular ? children[0] : children;
      }
    }
    return out;
  });
}
