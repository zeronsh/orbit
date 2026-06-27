// Query lifetime management: dedup, TTL, and garbage collection — mirrors Zero's
// `query-manager.ts` + `zql/src/query/ttl.ts`, simplified. Multiple subscribers
// to the same query share one upstream subscription (refcount); when the last
// view is destroyed the query is kept for its TTL, then unsubscribed (a `del` in
// `changeDesiredQueries`). Without this, queries would leak forever.

import type { QueriesPatchOp } from './protocol.ts';

/** The `put` variant of a desired-query patch op (always has a `hash`). */
export type QueryPut = Extract<QueriesPatchOp, { op: 'put' }>;

export type TTL = number | `${number}${'s' | 'm' | 'h' | 'd'}` | 'forever' | 'none';

const UNIT_MS = { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 } as const;
const MAX_TTL_MS = 600_000; // 10 minutes — matches Zero's clamp
export const DEFAULT_TTL: TTL = '5m';

/** Parse a TTL into milliseconds. `'forever'` → Infinity, `'none'`/0 → 0. */
export function parseTTL(ttl: TTL): number {
  if (ttl === 'forever') return Infinity;
  if (ttl === 'none') return 0;
  if (typeof ttl === 'number') return Math.max(0, ttl);
  const m = /^(\d+(?:\.\d+)?)(s|m|h|d)$/.exec(ttl);
  return m ? Number(m[1]) * UNIT_MS[m[2] as keyof typeof UNIT_MS] : parseTTL(DEFAULT_TTL);
}

function clampTTL(ms: number): number {
  return ms === Infinity ? Infinity : Math.min(ms, MAX_TTL_MS);
}

export type Scheduler = {
  setTimeout: (fn: () => void, ms: number) => unknown;
  clearTimeout: (handle: unknown) => void;
};

const defaultScheduler: Scheduler = {
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
};

type Entry = { put: QueryPut; count: number; ttlMs: number; timer?: unknown };

export class QueryManager {
  #entries = new Map<string, Entry>();
  #onSubscribe: (put: QueryPut) => void;
  #onUnsubscribe: (hash: string) => void;
  #sched: Scheduler;

  constructor(opts: {
    onSubscribe: (put: QueryPut) => void;
    onUnsubscribe: (hash: string) => void;
    scheduler?: Scheduler;
  }) {
    this.#onSubscribe = opts.onSubscribe;
    this.#onUnsubscribe = opts.onUnsubscribe;
    this.#sched = opts.scheduler ?? defaultScheduler;
  }

  /**
   * Register interest in a query. Returns a release function (idempotent). The
   * first registration subscribes upstream; identical queries dedupe to one.
   */
  add(hash: string, put: QueryPut, ttl: TTL = DEFAULT_TTL): () => void {
    const ttlMs = clampTTL(parseTTL(ttl));
    let entry = this.#entries.get(hash);
    if (entry) {
      entry.count++;
      entry.ttlMs = Math.max(entry.ttlMs, ttlMs);
      this.#cancelGc(entry);
    } else {
      const stamped: QueryPut = Number.isFinite(ttlMs) ? { ...put, ttl: ttlMs } : put;
      entry = { put: stamped, count: 1, ttlMs };
      this.#entries.set(hash, entry);
      this.#onSubscribe(stamped);
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#release(hash);
    };
  }

  #release(hash: string): void {
    const entry = this.#entries.get(hash);
    if (!entry || --entry.count > 0) return;
    if (entry.ttlMs === Infinity) return; // forever — stay subscribed
    if (entry.ttlMs <= 0) {
      this.#gc(hash);
      return;
    }
    entry.timer = this.#sched.setTimeout(() => this.#gc(hash), entry.ttlMs);
  }

  #cancelGc(entry: Entry): void {
    if (entry.timer !== undefined) {
      this.#sched.clearTimeout(entry.timer);
      entry.timer = undefined;
    }
  }

  #gc(hash: string): void {
    const entry = this.#entries.get(hash);
    if (!entry || entry.count > 0) return;
    this.#entries.delete(hash);
    this.#onUnsubscribe(hash);
  }

  /** All currently-subscribed query `put` ops (for reconnect resume). */
  active(): QueryPut[] {
    return [...this.#entries.values()].map((e) => e.put);
  }

  /** Number of live (subscribed, incl. within-TTL) queries — for tests/introspection. */
  size(): number {
    return this.#entries.size;
  }
}
