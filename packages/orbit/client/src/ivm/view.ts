// Terminal of an IVM pipeline: maintains the materialized result (`nodes`)
// incrementally as changes arrive, applying add/remove/edit/child to a sorted
// array. Because each Change carries the node's full (re-materialized)
// relationships, a `child` change just replaces the parent subtree in place.

import {
  makeComparator,
  pkKey,
  type Change,
  type Comparator,
  type Node,
  type Op,
} from './data.ts';
import type { OrderPart, Row } from '../protocol.ts';

export class MaterializedView {
  /** The current top-level result, in sort order. */
  nodes: Node[];
  #cmp: Comparator;
  #pk: string[];

  constructor(top: Op, order: OrderPart[], pk: string[]) {
    this.#cmp = makeComparator(order);
    this.#pk = pk;
    this.nodes = top.fetch({});
    const terminal: Op = {
      output: null,
      fetch: () => [],
      setOutput: () => {},
      push: (c: Change) => {
        this.#apply(c);
        return [];
      },
    };
    top.setOutput(terminal);
  }

  #apply(change: Change): void {
    switch (change.type) {
      case 'add':
        this.#insert(change.node);
        break;
      case 'remove':
        this.#removeByPk(change.node.row);
        break;
      case 'edit':
        this.#removeByPk(change.oldNode.row);
        this.#insert(change.node);
        break;
      case 'child': {
        const i = this.#indexByPk(change.node.row);
        if (i >= 0) this.nodes[i] = change.node; // row unchanged → position unchanged
        break;
      }
    }
  }

  #insert(node: Node): void {
    let lo = 0;
    let hi = this.nodes.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.#cmp(this.nodes[mid].row, node.row) < 0) lo = mid + 1;
      else hi = mid;
    }
    this.nodes.splice(lo, 0, node);
  }

  #indexByPk(row: Row): number {
    const k = pkKey(this.#pk, row);
    return this.nodes.findIndex((n) => pkKey(this.#pk, n.row) === k);
  }

  #removeByPk(row: Row): void {
    const i = this.#indexByPk(row);
    if (i >= 0) this.nodes.splice(i, 1);
  }

  /** The result as Zero's `{row, rels}` snapshot shape (hidden `zsubq_*` excluded). */
  snapshot(): { row: Row; rels: Record<string, unknown> }[] {
    return this.nodes.map(normNode);
  }
}

function normNode(node: Node): { row: Row; rels: Record<string, unknown> } {
  const rels: Record<string, unknown> = {};
  for (const k of Object.keys(node.relationships).sort()) {
    if (k.startsWith('zsubq_')) continue;
    rels[k] = node.relationships[k].map(normNode);
  }
  return { row: node.row, rels };
}
