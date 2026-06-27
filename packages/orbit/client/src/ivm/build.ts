// Pipeline builder: turn an AST into a live IVM operator graph. Port of the Rust
// `build_pipeline` (crates/oql/src/builder.rs). Shape:
//   source → (start cursor) → (filter | hidden-joins + cond-filter for WHERE)
//          → (joins for each related) → (take for limit)
// The predicate semantics reuse `eval.ts` (simpleMatches) so the IVM and the
// re-evaluation engine agree exactly.

import type { AST, Condition, CorrelatedSubquery, OrderPart } from '../protocol.ts';
import { simpleMatches } from '../eval.ts';
import { completeOrder, makeComparator, type Node, type Op } from './data.ts';
import { CondFilter, Filter, Join, skip, Take, type NodePredicate, type RowPredicate } from './operators.ts';

const SUBQ = 'zsubq_';

/** Supplies a source connection + primary key for each table in an AST. */
export interface SourceProvider {
  pkOf(table: string): string[];
  connect(table: string, order: OrderPart[]): Op;
}

export function buildPipeline(
  ast: AST,
  provider: SourceProvider,
  partitionKey: string[] | null = null,
): Op {
  const pk = provider.pkOf(ast.table);
  const order = completeOrder(ast.orderBy, pk);
  let current = provider.connect(ast.table, order);

  if (ast.start) current = skip(current, ast.start.row, ast.start.exclusive, makeComparator(order));

  if (ast.where) {
    if (conditionHasExists(ast.where)) {
      const joins: { relName: string; related: CorrelatedSubquery }[] = [];
      const counter = { n: 0 };
      const resolved = resolveCondition(ast.where, joins, counter);
      for (const { relName, related } of joins) {
        const child = buildPipeline(related.subquery, provider);
        current = new Join(
          current,
          child,
          related.correlation.parentField,
          related.correlation.childField,
          relName,
        );
      }
      current = new CondFilter(current, nodePredicate(resolved), pk);
    } else {
      current = new Filter(current, rowPredicate(ast.where));
    }
  }

  for (const sub of ast.related ?? []) {
    const child = buildPipeline(sub.subquery, provider, sub.correlation.childField);
    current = new Join(
      current,
      child,
      sub.correlation.parentField,
      sub.correlation.childField,
      relationshipName(sub),
    );
  }

  if (ast.limit != null) current = new Take(current, ast.limit, pk, partitionKey, makeComparator(order));

  return current;
}

/** Output relationship name for a related subquery (alias minus `zsubq_`). */
function relationshipName(sub: CorrelatedSubquery): string {
  const a = sub.subquery.alias;
  if (a) return a.startsWith(SUBQ) ? a.slice(SUBQ.length) : a;
  return sub.subquery.table;
}

// --- WHERE compilation ------------------------------------------------------

type Resolved =
  | { kind: 'simple'; cond: Extract<Condition, { type: 'simple' }> }
  | { kind: 'and' | 'or'; items: Resolved[] }
  | { kind: 'exists'; relName: string; negated: boolean };

/** Replace each EXISTS with a reference to the hidden relationship that backs it. */
function resolveCondition(
  cond: Condition,
  joins: { relName: string; related: CorrelatedSubquery }[],
  counter: { n: number },
): Resolved {
  switch (cond.type) {
    case 'simple':
      return { kind: 'simple', cond };
    case 'and':
      return { kind: 'and', items: cond.conditions.map((c) => resolveCondition(c, joins, counter)) };
    case 'or':
      return { kind: 'or', items: cond.conditions.map((c) => resolveCondition(c, joins, counter)) };
    case 'correlatedSubquery': {
      const relName = cond.related.subquery.alias ?? `${SUBQ}exists_${counter.n++}`;
      joins.push({ relName, related: cond.related });
      return { kind: 'exists', relName, negated: cond.op === 'NOT EXISTS' };
    }
  }
}

function nodePredicate(resolved: Resolved): NodePredicate {
  const evalResolved = (r: Resolved, node: Node): boolean => {
    switch (r.kind) {
      case 'simple':
        return simpleMatches(node.row, r.cond);
      case 'and':
        return r.items.every((x) => evalResolved(x, node));
      case 'or':
        return r.items.some((x) => evalResolved(x, node));
      case 'exists': {
        const present = (node.relationships[r.relName]?.length ?? 0) > 0;
        return present !== r.negated;
      }
    }
  };
  return (node) => evalResolved(resolved, node);
}

function rowPredicate(cond: Condition): RowPredicate {
  const evalRow = (c: Condition, row: Record<string, unknown>): boolean => {
    switch (c.type) {
      case 'simple':
        return simpleMatches(row, c);
      case 'and':
        return c.conditions.every((x) => evalRow(x, row));
      case 'or':
        return c.conditions.some((x) => evalRow(x, row));
      case 'correlatedSubquery':
        throw new Error('EXISTS must be handled by CondFilter');
    }
  };
  return (row) => evalRow(cond, row);
}

function conditionHasExists(cond: Condition): boolean {
  switch (cond.type) {
    case 'correlatedSubquery':
      return true;
    case 'and':
    case 'or':
      return cond.conditions.some(conditionHasExists);
    case 'simple':
      return false;
  }
}
