// IVM operators — ports of the Rust operators (crates/oql/src/ivm). Filter and
// Skip are stateless change transforms; CondFilter (WHERE incl. EXISTS) and Take
// (LIMIT) recompute their passing set/window on push and diff against the prior
// state; Join attaches a child relationship and propagates parent/child changes.

import {
  buildJoinConstraint,
  nodeEq,
  pkKey,
  type Change,
  type Comparator,
  type FetchRequest,
  type Node,
  type Op,
} from './data.ts';
import type { Row } from '../protocol.ts';

// --- Filter (stateless row predicate) --------------------------------------

export type RowPredicate = (row: Row) => boolean;

export class Filter implements Op {
  output: Op | null = null;
  #input: Op;
  #predicate: RowPredicate;

  constructor(input: Op, predicate: RowPredicate) {
    this.#input = input;
    this.#predicate = predicate;
    input.setOutput(this);
  }
  fetch(req: FetchRequest): Node[] {
    return this.#input.fetch(req).filter((n) => this.#predicate(n.row));
  }
  push(change: Change): Change[] {
    return filterPush(change, this.#predicate);
  }
  setOutput(o: Op): void {
    this.output = o;
  }
}

function filterPush(change: Change, predicate: RowPredicate): Change[] {
  switch (change.type) {
    case 'add':
    case 'remove':
    case 'child':
      return predicate(change.node.row) ? [change] : [];
    case 'edit': {
      const o = predicate(change.oldNode.row);
      const n = predicate(change.node.row);
      if (o && n) return [change];
      if (o && !n) return [{ type: 'remove', node: change.oldNode }];
      if (!o && n) return [{ type: 'add', node: change.node }];
      return [];
    }
  }
}

/** Skip (`start` cursor): keep rows at/after `bound` in the input's sort order. */
export function skip(input: Op, bound: Row, exclusive: boolean, cmp: Comparator): Filter {
  return new Filter(input, (row) => {
    const o = cmp(row, bound);
    return exclusive ? o > 0 : o >= 0;
  });
}

// --- CondFilter (WHERE, incl. EXISTS via hidden relationships) --------------

export type NodePredicate = (node: Node) => boolean;

export class CondFilter implements Op {
  output: Op | null = null;
  #input: Op;
  #predicate: NodePredicate;
  #pk: string[];
  #passing: Set<string> | null = null;

  constructor(input: Op, predicate: NodePredicate, pk: string[]) {
    this.#input = input;
    this.#predicate = predicate;
    this.#pk = pk;
    input.setOutput(this);
  }

  /** Lazily build the set of currently-passing primary keys from the input. */
  #ensure(): Set<string> {
    if (!this.#passing) {
      this.#passing = new Set<string>();
      for (const n of this.#input.fetch({})) if (this.#predicate(n)) this.#passing.add(pkKey(this.#pk, n.row));
    }
    return this.#passing;
  }

  fetch(req: FetchRequest): Node[] {
    const nodes = this.#input.fetch(req).filter((n) => this.#predicate(n));
    this.#ensure();
    return nodes;
  }

  /**
   * Incremental: only the changed node's membership can flip, so process just
   * this change instead of recomputing the whole passing set.
   */
  push(change: Change): Change[] {
    const passing = this.#ensure();
    const key = pkKey(this.#pk, change.node.row);
    switch (change.type) {
      case 'add':
        if (this.#predicate(change.node)) {
          passing.add(key);
          return [change];
        }
        return [];
      case 'remove':
        if (passing.has(key)) {
          passing.delete(key);
          return [change];
        }
        return [];
      case 'edit': {
        const was = passing.has(key);
        const now = this.#predicate(change.node);
        if (was && now) return [change];
        if (was && !now) {
          passing.delete(key);
          return [{ type: 'remove', node: change.oldNode }];
        }
        if (!was && now) {
          passing.add(key);
          return [{ type: 'add', node: change.node }];
        }
        return [];
      }
      case 'child': {
        // The row is unchanged; a (hidden EXISTS) relationship changed, which can
        // flip membership.
        const was = passing.has(key);
        const now = this.#predicate(change.node);
        if (was && now) return [change]; // still passing — propagate the child change
        if (was && !now) {
          passing.delete(key);
          return [{ type: 'remove', node: change.node }];
        }
        if (!was && now) {
          passing.add(key);
          return [{ type: 'add', node: change.node }];
        }
        return [];
      }
    }
  }
  setOutput(o: Op): void {
    this.output = o;
  }
}

// --- Take (LIMIT, optionally partitioned per parent) -----------------------

export class Take implements Op {
  output: Op | null = null;
  #input: Op;
  #limit: number;
  #pk: string[];
  #partitionKey: string[] | null;
  #cmp: Comparator;
  /** All rows per partition, kept in sort order (incrementally maintained). */
  #partitions: Map<string, Node[]> | null = null;
  /** The last-emitted top-`limit` window per partition (for diffing). */
  #windows = new Map<string, Node[]>();

  constructor(input: Op, limit: number, pk: string[], partitionKey: string[] | null, cmp: Comparator) {
    this.#input = input;
    this.#limit = limit;
    this.#pk = pk;
    this.#partitionKey = partitionKey;
    this.#cmp = cmp;
    input.setOutput(this);
  }

  #partition(row: Row): string {
    return this.#partitionKey
      ? JSON.stringify(this.#partitionKey.map((k) => (row as Record<string, unknown>)[k] ?? null))
      : '';
  }

  #ensure(): Map<string, Node[]> {
    if (!this.#partitions) {
      this.#partitions = new Map();
      // input.fetch is already in sort order, so per-partition order is preserved.
      for (const n of this.#input.fetch({})) {
        const p = this.#partition(n.row);
        const arr = this.#partitions.get(p) ?? [];
        arr.push(n);
        this.#partitions.set(p, arr);
      }
      for (const [p, arr] of this.#partitions) this.#windows.set(p, arr.slice(0, this.#limit));
    }
    return this.#partitions;
  }

  fetch(req: FetchRequest): Node[] {
    this.#ensure();
    if (req.constraint || !this.#partitionKey) {
      return this.#input.fetch(req).slice(0, this.#limit);
    }
    const out: Node[] = [];
    for (const arr of this.#partitions!.values()) out.push(...arr.slice(0, this.#limit));
    return out;
  }

  /** Incremental: touch only the affected partition(s) and diff their windows. */
  push(change: Change): Change[] {
    this.#ensure();
    const out: Change[] = [];
    switch (change.type) {
      case 'add':
        this.#updatePartition(this.#partition(change.node.row), (arr) => insertSorted(arr, change.node, this.#cmp), out);
        break;
      case 'remove':
        this.#updatePartition(this.#partition(change.node.row), (arr) => removeByPk(arr, change.node.row, this.#pk), out);
        break;
      case 'child':
        this.#updatePartition(this.#partition(change.node.row), (arr) => replaceByPk(arr, change.node, this.#pk), out);
        break;
      case 'edit': {
        const po = this.#partition(change.oldNode.row);
        const pn = this.#partition(change.node.row);
        if (po === pn) {
          this.#updatePartition(po, (arr) => {
            removeByPk(arr, change.oldNode.row, this.#pk);
            insertSorted(arr, change.node, this.#cmp);
          }, out);
        } else {
          this.#updatePartition(po, (arr) => removeByPk(arr, change.oldNode.row, this.#pk), out);
          this.#updatePartition(pn, (arr) => insertSorted(arr, change.node, this.#cmp), out);
        }
        break;
      }
    }
    return out;
  }

  #updatePartition(p: string, mutate: (arr: Node[]) => void, out: Change[]): void {
    const arr = this.#partitions!.get(p) ?? [];
    mutate(arr);
    this.#partitions!.set(p, arr);
    const newWin = arr.slice(0, this.#limit);
    const oldWin = this.#windows.get(p) ?? [];
    diffWindow(oldWin, newWin, this.#pk, out);
    this.#windows.set(p, newWin);
  }
  setOutput(o: Op): void {
    this.output = o;
  }
}

/** Insert `node` into a sorted array at the position given by `cmp` (total order). */
function insertSorted(arr: Node[], node: Node, cmp: Comparator): void {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cmp(arr[mid].row, node.row) < 0) lo = mid + 1;
    else hi = mid;
  }
  arr.splice(lo, 0, node);
}

function removeByPk(arr: Node[], row: Row, pk: string[]): void {
  const k = pkKey(pk, row);
  const i = arr.findIndex((n) => pkKey(pk, n.row) === k);
  if (i >= 0) arr.splice(i, 1);
}

function replaceByPk(arr: Node[], node: Node, pk: string[]): void {
  const k = pkKey(pk, node.row);
  const i = arr.findIndex((n) => pkKey(pk, n.row) === k);
  if (i >= 0) arr[i] = node;
}

/** Diff two windows (pk + full-node equality) into add/remove changes. */
function diffWindow(oldWin: Node[], newWin: Node[], pk: string[], out: Change[]): void {
  const oldMap = new Map(oldWin.map((n) => [pkKey(pk, n.row), n]));
  const newMap = new Map(newWin.map((n) => [pkKey(pk, n.row), n]));
  for (const [k, o] of oldMap) {
    const n = newMap.get(k);
    if (!n || !nodeEq(o, n)) out.push({ type: 'remove', node: o });
  }
  for (const [k, n] of newMap) {
    const o = oldMap.get(k);
    if (!o || !nodeEq(o, n)) out.push({ type: 'add', node: n });
  }
}

// --- Join (hierarchical, non-flattening) -----------------------------------

export class Join implements Op {
  output: Op | null = null;
  #parent: Op;
  #child: Op;
  #parentKey: string[];
  #childKey: string[];
  #rel: string;

  constructor(parent: Op, child: Op, parentKey: string[], childKey: string[], relationshipName: string) {
    this.#parent = parent;
    this.#child = child;
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#rel = relationshipName;
    parent.setOutput(new JoinParentPort(this));
    child.setOutput(new JoinChildPort(this));
  }

  #process(parent: Node): Node {
    const constraint = buildJoinConstraint(parent.row, this.#parentKey, this.#childKey);
    const children = constraint ? this.#child.fetch({ constraint }) : [];
    return { row: parent.row, relationships: { ...parent.relationships, [this.#rel]: children } };
  }

  fetch(req: FetchRequest): Node[] {
    return this.#parent.fetch(req).map((n) => this.#process(n));
  }
  push(): Change[] {
    throw new Error('Join is pushed via its parent/child ports');
  }
  setOutput(o: Op): void {
    this.output = o;
  }

  pushParent(change: Change): Change[] {
    switch (change.type) {
      case 'add':
        return [{ type: 'add', node: this.#process(change.node) }];
      case 'remove':
        return [{ type: 'remove', node: this.#process(change.node) }];
      case 'child':
        return [{
          type: 'child',
          node: this.#process(change.node),
          relationshipName: change.relationshipName,
          change: change.change,
        }];
      case 'edit':
        // If the edit moves the parent to a different join key its child set
        // changes — split into remove (old) + add (new). Otherwise edit in place.
        if (sameKey(change.oldNode.row, change.node.row, this.#parentKey)) {
          return [{ type: 'edit', node: this.#process(change.node), oldNode: this.#process(change.oldNode) }];
        }
        return [
          { type: 'remove', node: this.#process(change.oldNode) },
          { type: 'add', node: this.#process(change.node) },
        ];
    }
  }

  pushChild(change: Change): Change[] {
    // For a key-changing edit, re-materialize BOTH old parents (lose the child)
    // and new parents (gain it); the two key values are disjoint.
    const childRows: Row[] = [change.node.row];
    if (change.type === 'edit' && !sameKey(change.oldNode.row, change.node.row, this.#childKey)) {
      childRows[0] = change.node.row;
      childRows.push(change.oldNode.row);
    }
    const out: Change[] = [];
    for (const childRow of childRows) {
      const constraint = buildJoinConstraint(childRow, this.#childKey, this.#parentKey);
      if (!constraint) continue;
      for (const p of this.#parent.fetch({ constraint })) {
        out.push({ type: 'child', node: this.#process(p), relationshipName: this.#rel, change });
      }
    }
    return out;
  }
}

/** Two rows share the same compound key (used to detect join-key-changing edits). */
function sameKey(a: Row, b: Row, keys: string[]): boolean {
  return keys.every((k) => (a as Record<string, unknown>)[k] === (b as Record<string, unknown>)[k]);
}

class JoinParentPort implements Op {
  #join: Join;
  constructor(join: Join) {
    this.#join = join;
  }
  get output(): Op | null {
    return this.#join.output;
  }
  set output(_o: Op | null) {
    /* a join port's output follows the join's output */
  }
  fetch(): Node[] {
    throw new Error('join port has no fetch');
  }
  push(c: Change): Change[] {
    return this.#join.pushParent(c);
  }
  setOutput(): void {
    /* no-op */
  }
}

class JoinChildPort implements Op {
  #join: Join;
  constructor(join: Join) {
    this.#join = join;
  }
  get output(): Op | null {
    return this.#join.output;
  }
  set output(_o: Op | null) {
    /* follows the join's output */
  }
  fetch(): Node[] {
    throw new Error('join port has no fetch');
  }
  push(c: Change): Change[] {
    return this.#join.pushChild(c);
  }
  setOutput(): void {
    /* no-op */
  }
}
