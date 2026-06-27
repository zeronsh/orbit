// MemorySource: the in-memory root data source of a pipeline. Port of the Rust
// `MemorySource` (crates/oql/src/ivm/source.rs), simplified to **commit-first**:
// a push commits the change to storage, then delivers it to each connection's
// output. Downstream fetches therefore observe the post-change state directly —
// equivalent to Zero's overlay mechanism for the cross-table case, without the
// epoch bookkeeping. A source may have multiple connections (one per consumer,
// each with its own sort order).

import {
  constraintMatches,
  deliver,
  makeComparator,
  pkKey,
  type Change,
  type Comparator,
  type FetchRequest,
  type Node,
  type Op,
  type SourceChange,
} from './data.ts';
import type { OrderPart, Row } from '../protocol.ts';

type Connection = { output: Op | null; order: OrderPart[]; cmp: Comparator };

export class MemorySource {
  readonly table: string;
  readonly pk: string[];
  #rows = new Map<string, Row>();
  #connections: Connection[] = [];

  constructor(table: string, pk: string[]) {
    this.table = table;
    this.pk = pk;
  }

  insertInitial(row: Row): void {
    this.#rows.set(pkKey(this.pk, row), row);
  }

  rows(): Row[] {
    return [...this.#rows.values()];
  }

  get(key: string): Row | undefined {
    return this.#rows.get(key);
  }

  connect(order: OrderPart[]): SourceConnection {
    const conn: Connection = { output: null, order, cmp: makeComparator(order) };
    this.#connections.push(conn);
    return new SourceConnection(this, conn);
  }

  fetchConn(req: FetchRequest, conn: Connection): Node[] {
    let rows = [...this.#rows.values()];
    if (req.constraint) rows = rows.filter((r) => constraintMatches(req.constraint!, r));
    const cmp = req.reverse ? makeComparator(conn.order, true) : conn.cmp;
    rows = [...rows].sort(cmp);
    if (req.start) {
      const { row, basis } = req.start;
      const i = rows.findIndex((r) => {
        const c = cmp(r, row);
        return basis === 'after' ? c > 0 : c >= 0;
      });
      rows = i < 0 ? [] : rows.slice(i);
    }
    return rows.map((r) => ({ row: r, relationships: {} }));
  }

  /** Apply a source change (commit-first) and propagate to all connections. */
  push(change: SourceChange): void {
    // Commit to storage first so downstream fetches see the post-change view.
    if (change.type === 'add') {
      this.#rows.set(pkKey(this.pk, change.row), change.row);
    } else if (change.type === 'remove') {
      this.#rows.delete(pkKey(this.pk, change.row));
    } else {
      this.#rows.delete(pkKey(this.pk, change.oldRow));
      this.#rows.set(pkKey(this.pk, change.row), change.row);
    }
    for (const conn of this.#connections) {
      if (conn.output) deliver(conn.output, toChange(change));
    }
  }
}

/** A set of MemorySources, one per table — a `SourceProvider` for the builder. */
export class MemorySourceProvider {
  #sources = new Map<string, MemorySource>();

  add(table: string, pk: string[], rows: Row[]): MemorySource {
    const s = new MemorySource(table, pk);
    for (const r of rows) s.insertInitial(r);
    this.#sources.set(table, s);
    return s;
  }
  source(table: string): MemorySource {
    const s = this.#sources.get(table);
    if (!s) throw new Error(`no source for table ${table}`);
    return s;
  }
  pkOf(table: string): string[] {
    return this.source(table).pk;
  }
  connect(table: string, order: OrderPart[]): Op {
    return this.source(table).connect(order);
  }
  push(table: string, change: SourceChange): void {
    this.source(table).push(change);
  }
}

function toChange(change: SourceChange): Change {
  if (change.type === 'add') return { type: 'add', node: { row: change.row, relationships: {} } };
  if (change.type === 'remove') return { type: 'remove', node: { row: change.row, relationships: {} } };
  return {
    type: 'edit',
    node: { row: change.row, relationships: {} },
    oldNode: { row: change.oldRow, relationships: {} },
  };
}

/** One connection (consumer) of a MemorySource — an Input + push target. */
export class SourceConnection implements Op {
  output: Op | null = null;
  #src: MemorySource;
  #conn: Connection;

  constructor(src: MemorySource, conn: Connection) {
    this.#src = src;
    this.#conn = conn;
  }

  fetch(req: FetchRequest): Node[] {
    return this.#src.fetchConn(req, this.#conn);
  }
  push(): Change[] {
    throw new Error('a source connection never receives an upstream push');
  }
  setOutput(o: Op): void {
    this.output = o;
    this.#conn.output = o;
  }
}
