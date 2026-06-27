// Core types for the incremental view-maintenance (IVM) operator graph — a
// TypeScript port of the Rust `oql` engine (crates/oql/src/ivm). Operators form
// a pipeline: a Source emits Changes on each mutation, which propagate downstream
// (filter/join/take), and a terminal view applies them to a materialized result.
// Mirrors Zero's `zql/src/ivm`.

import type { OrderPart, Row, Value } from '../protocol.ts';
import { compareValues, valuesEqual } from '../eval.ts';

/** A row flowing through the pipeline plus its (eagerly materialized) children. */
export type Node = { row: Row; relationships: Record<string, Node[]> };

/** An incremental change propagated downstream through `push`. */
export type Change =
  | { type: 'add'; node: Node }
  | { type: 'remove'; node: Node }
  | { type: 'edit'; node: Node; oldNode: Node }
  | { type: 'child'; node: Node; relationshipName: string; change: Change };

/** A change applied to a Source (bare rows — sources are leaves). */
export type SourceChange =
  | { type: 'add'; row: Row }
  | { type: 'remove'; row: Row }
  | { type: 'edit'; row: Row; oldRow: Row };

export function changeNode(c: Change): Node {
  return c.node;
}

/** Parameters for a fetch (pull). */
export type FetchRequest = {
  constraint?: Record<string, Value>;
  start?: { row: Row; basis: 'at' | 'after' };
  reverse?: boolean;
};

/** An operator: both the fetch (pull) and push sides, plus a downstream link. */
export interface Op {
  fetch(req: FetchRequest): Node[];
  push(change: Change): Change[];
  output: Op | null;
  setOutput(o: Op): void;
}

/**
 * Propagate `change` into `op`, then deliver each emitted change to `op`'s
 * output — after `op.push` returns, mirroring the Rust `deliver` driver (so
 * operators may `fetch` from their inputs mid-push).
 */
export function deliver(op: Op, change: Change): void {
  const results = op.push(change);
  const out = op.output;
  if (out) for (const r of results) deliver(out, r);
}

export type Comparator = (a: Row, b: Row) => number;

export function makeComparator(order: readonly OrderPart[], reverse = false): Comparator {
  return (a, b) => {
    for (const [field, dir] of order) {
      const c = compareValues(a[field] as Value, b[field] as Value);
      if (c !== 0) {
        const r = dir === 'asc' ? c : -c;
        return reverse ? -r : r;
      }
    }
    return 0;
  };
}

/** Append missing primary-key columns (ascending) — Zero's `completeOrdering`. */
export function completeOrder(orderBy: readonly OrderPart[] | undefined, pk: readonly string[]): OrderPart[] {
  const order: OrderPart[] = [...(orderBy ?? [])];
  for (const k of pk) if (!order.some(([f]) => f === k)) order.push([k, 'asc']);
  return order;
}

export function pkKey(pk: readonly string[], row: Row): string {
  return JSON.stringify(pk.map((k) => (row as Record<string, unknown>)[k] ?? null));
}

/** Does `row` satisfy every column-equality in `constraint` (join semantics)? */
export function constraintMatches(constraint: Record<string, Value>, row: Row): boolean {
  for (const k of Object.keys(constraint)) {
    if (!valuesEqual((row as Record<string, Value>)[k] ?? null, constraint[k])) return false;
  }
  return true;
}

/** `to[i] = from[fromKeys[i]]`, or null if any key value is null (no join). */
export function buildJoinConstraint(
  from: Row,
  fromKeys: readonly string[],
  toKeys: readonly string[],
): Record<string, Value> | null {
  const c: Record<string, Value> = {};
  for (let i = 0; i < fromKeys.length; i++) {
    const v = (from as Record<string, Value>)[fromKeys[i]] ?? null;
    if (v === null) return null;
    c[toKeys[i]] = v;
  }
  return c;
}

/** Shallow value equality of two rows (for change diffing). */
export function rowEq(a: Row, b: Row): boolean {
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  for (const k of ak) if ((a as Record<string, unknown>)[k] !== (b as Record<string, unknown>)[k]) return false;
  return true;
}

/** Deep equality of two nodes (row + relationships) for Take's diffing. */
export function nodeEq(a: Node, b: Node): boolean {
  if (!rowEq(a.row, b.row)) return false;
  const ak = Object.keys(a.relationships);
  const bk = Object.keys(b.relationships);
  if (ak.length !== bk.length) return false;
  for (const k of ak) {
    const ca = a.relationships[k];
    const cb = b.relationships[k];
    if (!cb || ca.length !== cb.length) return false;
    for (let i = 0; i < ca.length; i++) if (!nodeEq(ca[i], cb[i])) return false;
  }
  return true;
}
