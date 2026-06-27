// Bridges the client Store to the IVM operator graph: seeds a MemorySource per
// table from the store's effective rows, and translates each changed effective-
// row key into an add/remove/edit push so the pipeline updates incrementally.

import { MemorySource } from './source.ts';
import { rowEq, type Op } from './data.ts';
import type { SourceProvider } from './build.ts';
import type { AST, CorrelatedSubquery, OrderPart, Row } from '../protocol.ts';
import { existsRelationships } from '../eval.ts';
import type { Store } from '../store.ts';

export class StoreProvider implements SourceProvider {
  #store: Store;
  #sources = new Map<string, MemorySource>();

  constructor(store: Store) {
    this.#store = store;
  }

  pkOf(table: string): string[] {
    return this.#store.pkOf(table);
  }

  connect(table: string, order: OrderPart[]): Op {
    return this.#ensure(table).connect(order);
  }

  #ensure(table: string): MemorySource {
    let s = this.#sources.get(table);
    if (!s) {
      s = new MemorySource(table, this.#store.pkOf(table));
      for (const row of this.#store.effectiveRows(table)) s.insertInitial(row);
      this.#sources.set(table, s);
    }
    return s;
  }

  /** Push the change for one touched effective-row key into its table's source. */
  applyChange(table: string, key: string): void {
    const src = this.#sources.get(table);
    if (!src) return;
    const next = this.#store.effectiveRow(table, key);
    const prev = src.get(key);
    if (prev && next) {
      if (!rowEq(prev, next)) src.push({ type: 'edit', row: next, oldRow: prev });
    } else if (next) {
      src.push({ type: 'add', row: next });
    } else if (prev) {
      src.push({ type: 'remove', row: prev });
    }
  }
}

/** Tables referenced anywhere in an AST (top + related + EXISTS subqueries). */
export function tablesOf(ast: AST, out = new Set<string>()): Set<string> {
  out.add(ast.table);
  for (const rel of ast.related ?? []) tablesOf(rel.subquery, out);
  for (const rel of existsRelationships(ast.where)) tablesOf(rel.subquery, out);
  collectExistsTables(ast.where, out);
  return out;
}

/** EXISTS subqueries (incl. hidden `zsubq_*`) referenced by a condition. */
function collectExistsTables(cond: AST['where'], out: Set<string>): void {
  if (!cond) return;
  if (cond.type === 'correlatedSubquery') tablesOf((cond.related as CorrelatedSubquery).subquery, out);
  else if (cond.type === 'and' || cond.type === 'or') for (const c of cond.conditions) collectExistsTables(c, out);
}

/** Convert an IVM result node into a client row (nested arrays, singular unwrap). */
export function nodeToRow(node: { row: Row; relationships: Record<string, { row: Row; relationships: Record<string, unknown> }[]> }, ast: AST): Record<string, unknown> {
  const out: Record<string, unknown> = { ...node.row };
  for (const rel of ast.related ?? []) {
    const alias = rel.subquery.alias ?? rel.subquery.table;
    if (rel.hidden) {
      // Junction (many-to-many): drop the junction nodes and lift the nested
      // destination nodes up under this alias (matches Zero's hidden junction).
      const inner = rel.subquery.related![0];
      const innerAlias = inner.subquery.alias ?? inner.subquery.table;
      const junctionNodes = node.relationships[alias] ?? [];
      const dest = junctionNodes.flatMap((jn) => (jn.relationships[innerAlias] as typeof junctionNodes ?? []).map((c) => nodeToRow(c as never, inner.subquery)));
      out[alias] = inner.singular ? dest[0] : dest;
    } else {
      const children = (node.relationships[alias] ?? []).map((c) => nodeToRow(c as never, rel.subquery));
      out[alias] = rel.singular ? children[0] : children;
    }
  }
  for (const rel of existsRelationships(ast.where)) {
    const alias = rel.subquery.alias!;
    out[alias] = (node.relationships[alias] ?? []).map((c) => nodeToRow(c as never, rel.subquery));
  }
  return out;
}
