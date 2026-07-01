// The Orbit client: connects to an Orbit server over WebSocket, subscribes to
// queries, applies pokes, and sends mutations. Mirrors Zero's `Zero` class
// surface (`.query`, `.mutate`, materialized views) and behaviour:
//
//  - **Reactive local reads**: views run the query (`where`/`orderBy`/`limit`/
//    `related`) over the local store on every change (see `eval.ts`), so results
//    are correctly ordered/limited and nested — not just flat synced rows.
//  - **Optimistic mutations**: a mutation is applied to a local overlay
//    immediately and rebased away once the server confirms its lastMutationID.
//  - **Resilient connection**: auto-reconnect with backoff, resubscribing active
//    queries and resending unconfirmed mutations on reconnect.

import {
  hashAST,
  hashString,
  type AST,
  type CrudOp,
  type Downstream,
  type Mutation,
  type QueriesPatchOp,
  type Row,
  type Upstream,
} from './protocol.ts';
import { Query, SchemaQuery, type SchemaQueries, type QueryHost } from './query.ts';
import { type AnySchema, type PkOf, type RowOf, type SchemaDef } from './schema.ts';
import {
  collectOps,
  type CtxOf,
  type MutateAPI,
  type MutationDef,
  type MutationDefs,
  type QueriesAPI,
  type QueryDef,
  type QueryDefs,
} from './custom.ts';
import { QueryManager, type TTL } from './query-manager.ts';
import { Store, type ChangedKey } from './store.ts';
import type { KV } from './persist.ts';
import { buildPipeline } from './ivm/build.ts';
import { MaterializedView } from './ivm/view.ts';
import { completeOrder } from './ivm/data.ts';
import { StoreProvider, tablesOf, nodeToRow } from './ivm/store-provider.ts';

/** Encode an auth token into a `Sec-WebSocket-Protocol` value (Zero-compatible). */
function encodeSecProtocol(authToken: string): string {
  const payload = JSON.stringify({ initConnectionMessage: null, authToken });
  const b64 =
    typeof btoa === 'function' ? btoa(payload) : Buffer.from(payload, 'utf8').toString('base64');
  return encodeURIComponent(b64);
}

export type OrbitOptions<S extends SchemaDef = AnySchema> = {
  server: string; // ws://host:port
  /** The schema — supplies end-to-end types for `query`/`mutate`. Optional. */
  schema?: S;
  /**
   * Auth token (or async getter) sent to the server, which forwards it as a
   * `Bearer` token to your push/query endpoints so they can authenticate.
   */
  auth?: string | (() => string | Promise<string>);
  clientID?: string;
  clientGroupID?: string;
  /** Max reconnect backoff in ms (default 30s). Set 0 to disable reconnect. */
  maxReconnectMs?: number;
  /**
   * How long a query stays subscribed after its last view is destroyed, before
   * it's garbage-collected (a `del` is sent upstream). Default `'5m'`.
   */
  queryTTL?: TTL;
  /**
   * Persist synced rows + pending mutations to a `KV` (e.g. `new IDBKV()` in the
   * browser) so data is available offline and survives reloads. Hydrated before
   * the first connection.
   */
  persist?: KV;
  /**
   * Called when the server sends a terminal `error` message (e.g. auth/version
   * failure). Without a handler the error is logged; the socket still reconnects
   * with backoff, so handle fatal errors here (e.g. refresh auth or `close()`).
   */
  onError?: (e: { kind: string; message: string }) => void;
};

/** Per-table CRUD mutators, mirroring Zero's `z.mutate.<table>.<op>(row)`. */
export type TableMutator<T extends Row = Row, PK extends keyof T = keyof T> = {
  insert(value: T): void;
  upsert(value: T): void;
  update(value: Pick<T, PK> & Partial<T>): void;
  delete(value: Pick<T, PK>): void;
};

/** `orbit.query.<table>` — one schema-aware query per table (relationships by name). */
export type QueryAccess<S extends SchemaDef> = SchemaQueries<S>;

/** `orbit.mutate.<table>` — one typed mutator per schema table. */
export type MutateAccess<S extends SchemaDef> = {
  [K in keyof S['tables']]: TableMutator<RowOf<S['tables'][K]>, PkOf<S['tables'][K]>>;
};

/**
 * A live view over a query's results, maintained **incrementally** by the IVM
 * operator graph (see `ivm/`). The query is compiled to a pipeline of operators
 * fed by the local store; each store change is pushed through as add/remove/edit
 * and the materialized result is updated in place — not re-evaluated from
 * scratch. `applyWhere` defaults to true (the client store is shared across
 * subscriptions, so each view must filter to its own rows); pass false only when
 * the caller guarantees the store already holds exactly this query's rows.
 */
export class View<T extends Row = Row> {
  #ast: AST;
  #unsub: () => void;
  #onDestroy?: () => void;
  #provider: StoreProvider;
  #mview: MaterializedView;
  #tables: Set<string>;
  data: T[] = [];
  #listeners = new Set<() => void>();

  constructor(store: Store, ast: AST, applyWhere = true, onDestroy?: () => void) {
    this.#ast = applyWhere ? ast : { ...ast, where: undefined };
    this.#onDestroy = onDestroy;
    this.#tables = tablesOf(this.#ast);
    this.#provider = new StoreProvider(store);
    const pk = store.pkOf(this.#ast.table);
    const top = buildPipeline(this.#ast, this.#provider);
    this.#mview = new MaterializedView(top, completeOrder(this.#ast.orderBy, pk), pk);
    this.#refresh();
    this.#unsub = store.subscribe((changed) => this.#onChange(changed));
  }

  #onChange(changed: ReadonlyArray<ChangedKey>): void {
    let touched = false;
    for (const { table, key } of changed) {
      if (this.#tables.has(table)) {
        this.#provider.applyChange(table, key);
        touched = true;
      }
    }
    if (touched) this.#refresh();
  }

  #refresh(): void {
    this.data = this.#mview.nodes.map((n) => nodeToRow(n, this.#ast)) as unknown as T[];
    for (const fn of this.#listeners) fn();
  }

  subscribe(fn: () => void): () => void {
    this.#listeners.add(fn);
    return () => this.#listeners.delete(fn);
  }

  /** Stop reacting to store changes + release the query subscription (Zero's `view.destroy()`). */
  destroy(): void {
    this.#unsub();
    this.#listeners.clear();
    this.#onDestroy?.();
  }
}

/** A context value or a (sync) getter for it. */
type CtxInput<C> = C | (() => C);
/** The Ctx the handlers expect, inferred from the mutator/query defs. */
type ResolveCtx<MD, QD> = MD extends MutationDefs
  ? CtxOf<MD>
  : QD extends QueryDefs
    ? CtxOf<QD>
    : unknown;

export class Orbit<
  S extends SchemaDef = AnySchema,
  MD extends MutationDefs | undefined = undefined,
  QD extends QueryDefs | undefined = undefined,
> implements QueryHost {
  // Not `readonly`: when `persist` is enabled and no explicit id was given, these
  // are restored from the KV in `#init` so a reload keeps the same identity (and
  // thus resumes the same server CVR as a fast delta instead of a full resync).
  clientID: string;
  clientGroupID: string;
  /** Per-table typed query builder (ad-hoc queries): `orbit.query.todo.where(...)`. */
  readonly query: QueryAccess<S>;
  /** Custom (named) queries from the `queries` option: `orbit.queries.allTodos()`. */
  readonly queries: QD extends QueryDefs ? QueriesAPI<QD> : Record<string, never>;
  /**
   * Mutators. With a `mutators` option these are your custom mutators
   * (`orbit.mutate.createTodo(args)`); otherwise per-table CRUD
   * (`orbit.mutate.todo.insert(...)`).
   */
  readonly mutate: MD extends MutationDefs ? MutateAPI<MD> : MutateAccess<S>;

  #ws: WebSocket | undefined;
  #store: Store;
  #opts: OrbitOptions<S>;
  #pkByTable: Record<string, string[]>;
  #schema?: S;
  #mutatorDefs?: Record<string, MutationDef>;
  /** Context for optimistic mutators + local query derivation (a value or getter).
   * The server independently derives the authoritative ctx from the auth token. */
  #context?: unknown | (() => unknown);
  #nextMutationID = 1;
  #closed = false;
  #connecting = false;
  #reconnectMs = 500;
  #maxReconnectMs: number;
  #queryTTL: TTL;
  #kv?: KV;
  /** Last server cookie we've fully applied; sent as `baseCookie` on reconnect so
   * the server can prove a delta resume is safe (else it full-resyncs). */
  #cookie?: string;

  /** Query lifetime: dedup + TTL + GC. Its `active()` set is resent on reconnect. */
  #queries: QueryManager;
  /** Unconfirmed mutations by id — resent on (re)connect, dropped on confirm. */
  #unconfirmedPushes = new Map<number, Upstream>();
  /** In-flight poke buffer: rows + lastMutationID changes accumulate across
   * `pokePart`s and are applied atomically on `pokeEnd`. A mid-poke disconnect (or
   * `pokeEnd.cancel`) discards it, so the store never holds a torn partial poke. */
  #poke: { rows: import('./protocol.ts').RowPatchOp[]; lmids: Record<string, number> } | null = null;
  /** Surfaced when the server sends a terminal `error` message. */
  #onError?: (e: { kind: string; message: string }) => void;
  /** Whether the id was supplied explicitly (then we never override it from the KV). */
  #idFromOpts: boolean;
  /** Releases the Web-Locks persistence leadership (held by the first tab). */
  #releasePersistLock?: () => void;

  constructor(
    opts: OrbitOptions<S> & { mutators?: MD; queries?: QD; context?: CtxInput<ResolveCtx<MD, QD>> },
  ) {
    this.#opts = opts;
    this.#schema = opts.schema;
    this.#context = (opts as { context?: unknown | (() => unknown) }).context;
    this.#maxReconnectMs = opts.maxReconnectMs ?? 30_000;
    this.#queryTTL = opts.queryTTL ?? '5m';
    this.#kv = opts.persist;
    this.#onError = opts.onError;
    this.#queries = new QueryManager({
      onSubscribe: (put) => this.#send(['changeDesiredQueries', { desiredQueriesPatch: [put] }]),
      onUnsubscribe: (hash) =>
        this.#send(['changeDesiredQueries', { desiredQueriesPatch: [{ op: 'del', hash }] }]),
    });
    this.#idFromOpts = opts.clientID != null;
    this.clientID = opts.clientID ?? Math.random().toString(36).slice(2);
    this.clientGroupID = opts.clientGroupID ?? this.clientID;

    // Primary keys per table from the schema (defaults to ['id']).
    this.#pkByTable = {};
    for (const t of Object.values(opts.schema?.tables ?? {})) {
      this.#pkByTable[t.name] = [...t.primaryKey];
    }
    this.#store = new Store(this.#pkByTable);

    const schemaForQueries = (this.#schema ?? { tables: {}, relationships: {} }) as S;
    this.query = new Proxy({}, {
      get: (_t, table: string) => new SchemaQuery(this, schemaForQueries, table, Query.from(table)),
    }) as QueryAccess<S>;

    // Custom queries: `orbit.queries.<name>(args)` -> a typed Subscribable that
    // subscribes by NAME (resolved authoritatively by the server's query
    // endpoint). The def runs locally to derive the AST used for local
    // ordering/nesting/optimistic reads (its `where` is applied server-side).
    const queryDefs = opts.queries as Record<string, QueryDef> | undefined;
    this.queries = (queryDefs
      ? new Proxy({}, {
          get: (_t, name: string) => (args?: unknown) => ({
            materialize: () => {
              const ast = queryDefs[name].handler({ args, ctx: this.#resolveContext() } as never).ast();
              const argList = args === undefined ? [] : [args];
              return this.materializeNamed(name, argList, ast);
            },
          }),
        })
      : ({} as Record<string, never>)) as this['queries'];

    // Mutators: custom (sent to the server by NAME — the server forwards them to
    // the app's push endpoint, which runs them with context) or per-table CRUD.
    this.#mutatorDefs = opts.mutators as Record<string, MutationDef> | undefined;
    this.mutate = (this.#mutatorDefs
      ? new Proxy({}, {
          get: (_t, name: string) => (args?: unknown) => this.mutateCustom(name, args),
        })
      : new Proxy({}, {
          get: (_t, table: string): TableMutator => {
            const pk = this.#pkByTable[table] ?? ['id'];
            const crud = (op: CrudOp['op'], value: Row) =>
              this.mutateCrud({ op, tableName: table, primaryKey: pk, value } as CrudOp);
            return {
              insert: (value) => crud('insert', value),
              upsert: (value) => crud('upsert', value),
              update: (value) => crud('update', value),
              delete: (value) => crud('delete', value),
            };
          },
        })) as this['mutate'];

    void this.#init();
  }

  /** Build a raw (untyped) query against `table` — escape hatch. */
  queryRaw(table: string): Query {
    return Query.from(table);
  }

  /** Subscribe to an ad-hoc query and return a live [`View`]. */
  materialize(query: Query, ttl: TTL = this.#queryTTL): View {
    const ast = query.ast();
    const hash = hashAST(ast);
    const release = this.#queries.add(hash, { op: 'put', hash, ast }, ttl);
    return new View(this.#store, ast, true, release);
  }

  /**
   * Subscribe to a custom (named) query by name + args. The server's query
   * endpoint resolves/authorizes it (with the connection's auth) into the actual
   * query; the client uses the def's `ast` for local ordering/nesting/optimism.
   *
   * The local view DOES apply the def's `where` (applyWhere=true): the client
   * store is shared across all subscriptions, so without it a filtered query like
   * `issue({id})` would match every row another query synced (e.g. the issues
   * list) and `.one()` would return the wrong row. This assumes the def's filter
   * matches the server's resolution, which holds for ordinary parameterized
   * queries; the store only ever holds server-authorized rows.
   */
  materializeNamed(name: string, args: unknown[], ast: AST, ttl: TTL = this.#queryTTL): View {
    const hash = hashString(JSON.stringify([name, args]));
    const release = this.#queries.add(hash, { op: 'put', hash, name, args }, ttl);
    return new View(this.#store, ast, true, release);
  }

  /** Apply a single CRUD op (mirrors `z.mutate.<table>.insert(...)`). */
  mutateCrud(op: CrudOp): void {
    const id = this.#nextMutationID++;
    const mutation: Mutation = {
      type: 'crud',
      id,
      clientID: this.clientID,
      name: '_zero_crud',
      args: [{ ops: [op] }],
      timestamp: Date.now(),
    };
    this.#store.addPending(id, [op], mutation); // optimistic + persisted
    this.#pushMutation(id, mutation);
  }

  /** Run a custom mutator by name (optimistically, then on the server). */
  mutateCustom(name: string, args?: unknown): void {
    const id = this.#nextMutationID++;
    const mutation: Mutation = {
      type: 'custom',
      id,
      clientID: this.clientID,
      name,
      args: args === undefined ? [] : [args],
      timestamp: Date.now(),
    };
    // Optimistic: run the mutator locally (with the client `context`) to overlay
    // its effect immediately. The server re-runs it with the authoritative ctx and
    // the confirmed rows replace this overlay on rebase.
    const def = this.#mutatorDefs?.[name];
    let ops: CrudOp[] = [];
    if (def && this.#schema) {
      try {
        ops = collectOps(this.#schema, def, args, this.#resolveContext());
      } catch {
        /* a mutator that needs real ctx locally just skips the optimistic step */
      }
    }
    this.#store.addPending(id, ops, mutation);
    this.#pushMutation(id, mutation);
  }

  close(): void {
    this.#closed = true;
    this.#ws?.close();
    this.#releasePersistLock?.();
  }

  // --- internals ----------------------------------------------------------

  /** Resolve the client context (a value or sync getter); `{}` when unset. */
  #resolveContext(): unknown {
    const c = this.#context;
    return typeof c === 'function' ? (c as () => unknown)() : (c ?? {});
  }

  /**
   * Web-Locks single-writer election for the persisted cache. Two tabs sharing one
   * IndexedDB (and the restored clientID) would each flush their own dirty rows,
   * pending mutations, and cookie into the same flat keyspace with no coordination —
   * last-writer-wins can persist a cookie covering rows only the *other* tab wrote,
   * and both tabs would drive the same server-side CVR. Only the first tab (the
   * lock holder) gets persistence; later tabs run memory-only with their own fresh
   * clientID (a full server sync — correct, just uncached). The lock auto-releases
   * when the tab dies, so the next reload elects a new leader. Environments without
   * Web Locks (Node, tests, old browsers) keep today's behavior.
   */
  async #acquirePersistLock(): Promise<boolean> {
    const locks = (
      globalThis as {
        navigator?: {
          locks?: {
            request(
              name: string,
              opts: { ifAvailable: boolean },
              cb: (lock: unknown) => unknown,
            ): Promise<unknown>;
          };
        };
      }
    ).navigator?.locks;
    if (!locks) return true;
    return new Promise((resolve) => {
      const req = locks.request('zeronsh-orbit-persist', { ifAvailable: true }, (lock) => {
        if (lock === null) {
          resolve(false); // another tab holds the cache
          return;
        }
        resolve(true);
        // Hold the lock until close() (or tab death, which releases it implicitly).
        return new Promise<void>((release) => {
          this.#releasePersistLock = release;
        });
      });
      // A LockManager error (e.g. permissions) must not block startup.
      void Promise.resolve(req).catch(() => resolve(true));
    });
  }

  /** Hydrate persisted state (if any), restore unconfirmed mutations, then connect. */
  async #init(): Promise<void> {
    if (this.#kv && !(await this.#acquirePersistLock())) {
      this.#kv = undefined; // follower tab: memory-only, fresh identity
    }
    if (this.#kv) {
      try {
        await this.#store.hydrate(this.#kv);
        // Restore a STABLE identity so a reload resumes the same server-side CVR as
        // a fast delta instead of looking like a brand-new client (full resync).
        // Only when no explicit id was given; sign-out (resetOrbit) deletes the KV,
        // so a different user gets a fresh identity.
        if (!this.#idFromOpts) {
          const savedID = await this.#kv.get('clientID');
          if (typeof savedID === 'string') {
            this.clientID = savedID;
            const savedGroup = await this.#kv.get('clientGroupID');
            this.clientGroupID = typeof savedGroup === 'string' ? savedGroup : savedID;
          } else {
            void this.#kv.set('clientID', this.clientID);
            void this.#kv.set('clientGroupID', this.clientGroupID);
          }
        }
        // Re-queue restored pending mutations so they're resent on connect, and
        // continue the mutation-id sequence past them.
        let maxId = 0;
        for (const m of this.#store.pendingMutations()) {
          maxId = Math.max(maxId, m.id);
          this.#unconfirmedPushes.set(m.id, ['push', {
            clientGroupID: this.clientGroupID,
            mutations: [m],
            pushVersion: 1,
            timestamp: m.timestamp,
            requestID: Math.random().toString(36).slice(2),
          }]);
        }
        if (maxId >= this.#nextMutationID) this.#nextMutationID = maxId + 1;
        // Continue the mutation-id sequence past the high-water mark persisted
        // across reloads. Otherwise ids restart at 1 and the server (which tracks a
        // per-client lastMutationID) silently drops them as already-processed.
        const savedNextID = await this.#kv.get('nextMutationID');
        if (typeof savedNextID === 'number' && savedNextID > this.#nextMutationID) {
          this.#nextMutationID = savedNextID;
        }
        // Restore the last applied cookie (persisted by the store, after its rows, so
        // it's never ahead of them) so a reload resumes as a delta safely.
        this.#cookie = this.#store.cookie();
      } catch {
        /* a persistence failure must not block connecting */
      }
    }
    await this.#connect();
  }

  #pushMutation(id: number, mutation: Mutation): void {
    const msg: Upstream = ['push', {
      clientGroupID: this.clientGroupID,
      mutations: [mutation],
      pushVersion: 1,
      timestamp: Date.now(),
      requestID: Math.random().toString(36).slice(2),
    }];
    this.#unconfirmedPushes.set(id, msg);
    // Persist the id high-water mark and send only once it's DURABLE. The server
    // records the id on receipt — if the send won the race and the tab died before
    // this write committed, a reload would reuse the id for a *different* mutation,
    // which the server silently drops as already-processed (divergence). KV writes
    // to one key commit in issue order, so rapid mutations still send in order.
    // (The store persists the pending mutation itself, debounced; that pairing is
    // safe either way: if `p/id` is lost too, the mutation is retried wholesale.)
    const kv = this.#kv;
    if (kv) {
      void kv
        .set('nextMutationID', this.#nextMutationID)
        .catch(() => {}) // a persistence failure must not block mutating
        .then(() => this.#send(msg));
    } else {
      this.#send(msg);
    }
  }

  async #connect(): Promise<void> {
    if (this.#closed || this.#connecting) return;
    if (typeof WebSocket === 'undefined') return; // non-browser/test env
    this.#connecting = true;

    // Auth token is sent in the `Sec-WebSocket-Protocol` header (the only way to
    // pass auth on a browser WebSocket handshake). The server forwards it as a
    // Bearer token to the app's push/query endpoints.
    const token = typeof this.#opts.auth === 'function' ? await this.#opts.auth() : this.#opts.auth;
    if (this.#closed) {
      this.#connecting = false;
      return;
    }
    // clientID rides in the connect URL (Zero-style) so a view-syncer can load
    // this client's persisted view and, on reconnect to ANY node, resume as a
    // delta instead of re-sending the whole result. baseCookie is the last cookie
    // we applied — the server fast-resumes only if it matches the stored version.
    let url = `${this.#opts.server}/sync/v51/connect?clientID=${encodeURIComponent(this.clientID)}`;
    if (this.#cookie != null) url += `&baseCookie=${encodeURIComponent(this.#cookie)}`;
    const ws = token ? new WebSocket(url, [encodeSecProtocol(token)]) : new WebSocket(url);
    this.#ws = ws;

    ws.addEventListener('open', () => {
      this.#connecting = false;
      // Backoff resets on the first completed poke (a real health signal), not on
      // mere socket open — an open-then-immediately-close server can't reconnect-storm.
      this.#resume(ws);
    });
    ws.addEventListener('message', (ev: MessageEvent) => {
      this.#onMessage(JSON.parse(ev.data as string) as Downstream);
    });
    ws.addEventListener('close', (ev: CloseEvent) => {
      this.#connecting = false;
      if (this.#ws === ws) this.#ws = undefined;
      this.#poke = null; // discard any partially-received poke (no torn state)
      // WS 1009 (Message Too Big): a frame we sent exceeded the server's limit — an
      // oversized mutation the server can never accept. It's persisted and re-sent on
      // every reconnect, so it would loop forever and block the whole mutation queue.
      // Drop the offending (largest pending) mutation and surface an error instead of
      // storming. Mirrors Zero's poison-mutation handling (#5982).
      if (ev.code === 1009 && this.#unconfirmedPushes.size > 0) {
        let poisonId: number | undefined;
        let maxSize = -1;
        for (const [id, msg] of this.#unconfirmedPushes) {
          const size = JSON.stringify(msg).length;
          if (size > maxSize) {
            maxSize = size;
            poisonId = id;
          }
        }
        if (poisonId !== undefined) {
          this.#unconfirmedPushes.delete(poisonId);
          this.#store.dropPending(poisonId);
          this.#onError?.({
            kind: 'mutation-too-large',
            message: `dropped mutation ${poisonId} (~${maxSize} bytes): server closed with code 1009 (message too big)`,
          });
        }
      }
      this.#scheduleReconnect();
    });
    ws.addEventListener('error', () => ws.close());
  }

  /** Resubscribe active queries and resend unconfirmed mutations. */
  #resume(ws: WebSocket): void {
    const queries = this.#queries.active();
    if (queries.length) {
      ws.send(JSON.stringify(['changeDesiredQueries', { desiredQueriesPatch: queries }]));
    }
    for (const msg of this.#unconfirmedPushes.values()) ws.send(JSON.stringify(msg));
  }

  #scheduleReconnect(): void {
    if (this.#closed || this.#maxReconnectMs === 0) return;
    const delay = Math.min(this.#reconnectMs, this.#maxReconnectMs);
    this.#reconnectMs = Math.min(this.#reconnectMs * 2, this.#maxReconnectMs);
    setTimeout(() => void this.#connect(), delay);
  }

  #send(msg: Upstream): void {
    if (this.#ws && this.#ws.readyState === WebSocket.OPEN) {
      this.#ws.send(JSON.stringify(msg));
    }
    // Otherwise it's captured in #activeQueries / #unconfirmedPushes and will be
    // (re)sent by #resume on the next open.
  }

  #onMessage(msg: Downstream): void {
    const [tag, body] = msg;
    switch (tag) {
      case 'pokeStart':
        // Begin buffering a poke. (The server only sends a delta when our reported
        // baseCookie matches its stored version, else it full-resyncs, so we trust
        // the framing here.)
        this.#poke = { rows: [], lmids: {} };
        return;
      case 'pokePart': {
        const poke = (this.#poke ??= { rows: [], lmids: {} });
        if (body.rowsPatch) poke.rows.push(...body.rowsPatch);
        if (body.lastMutationIDChanges) Object.assign(poke.lmids, body.lastMutationIDChanges);
        return;
      }
      case 'pokeEnd': {
        const poke = this.#poke;
        this.#poke = null;
        if (!poke || body.cancel) return; // canceled → discard the whole poke
        // Apply atomically on pokeEnd: synced rows FIRST, then confirmations, so a
        // confirmed mutation's optimistic overlay only drops once its authoritative
        // row is in the store (no flicker), and a mid-poke disconnect (no pokeEnd)
        // leaves nothing partially applied.
        if (poke.rows.length) this.#store.applyAll(poke.rows);
        const confirmed = poke.lmids[this.clientID];
        if (confirmed != null) {
          for (const id of this.#unconfirmedPushes.keys()) {
            if (id <= confirmed) this.#unconfirmedPushes.delete(id);
          }
          this.#store.confirmThrough(confirmed);
        }
        this.#cookie = body.cookie; // in-memory: the live reconnect baseCookie this session
        // Persist the cookie through the store so it lands AFTER the rows it covers
        // (never ahead of them) — otherwise a reload resumes with a cookie that makes
        // the server suppress rows this client never durably stored.
        this.#store.setCookie(body.cookie);
        this.#reconnectMs = 500; // a completed poke proves the connection is healthy
        return;
      }
      case 'error':
        // Surface terminal server errors instead of silently looping.
        if (this.#onError) this.#onError(body);
        else console.error(`orbit: server error: ${body.kind}: ${body.message}`);
        return;
      default:
        return; // 'connected', 'pong' — nothing to apply
    }
  }
}
