// Pluggable client persistence — a small async key/value abstraction (mirroring
// the role of Zero/Replicache's `kv/store.ts`, without the dag/btree). The Store
// persists synced rows under `e/…` keys and pending mutations under `p/…`, so
// data is available offline and survives reloads. `IDBKV` backs it with
// IndexedDB in the browser; `MemoryKV` is for tests / non-browser environments.

/** One write in an atomic {@link KV.batch}. */
export type BatchOp = { type: 'set'; key: string; value: unknown } | { type: 'del'; key: string };

export interface KV {
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown): Promise<void>;
  del(key: string): Promise<void>;
  /** All `[key, value]` pairs whose key starts with `prefix`. */
  entries(prefix: string): Promise<[string, unknown][]>;
  /**
   * Optional: apply `ops` in order as ONE atomic unit (a single IndexedDB
   * transaction) — all-or-nothing under a crash, and far cheaper than a
   * transaction per key. When absent, callers fall back to sequential
   * `set`/`del` with careful ordering (see `Store.flush`).
   */
  batch?(ops: BatchOp[]): Promise<void>;
}

/** In-memory KV — used by tests and as a no-IndexedDB fallback. */
export class MemoryKV implements KV {
  #m = new Map<string, unknown>();
  async get(key: string): Promise<unknown> {
    return this.#m.get(key);
  }
  async set(key: string, value: unknown): Promise<void> {
    this.#m.set(key, value);
  }
  async del(key: string): Promise<void> {
    this.#m.delete(key);
  }
  async entries(prefix: string): Promise<[string, unknown][]> {
    return [...this.#m].filter(([k]) => k.startsWith(prefix));
  }
  async batch(ops: BatchOp[]): Promise<void> {
    for (const op of ops) {
      if (op.type === 'set') this.#m.set(op.key, op.value);
      else this.#m.delete(op.key);
    }
  }
}

function wrap<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

/** IndexedDB-backed KV (browser). One object store, keyed by string. */
export class IDBKV implements KV {
  #dbp: Promise<IDBDatabase>;

  constructor(name = 'orbit') {
    this.#dbp = new Promise((resolve, reject) => {
      const r = indexedDB.open(name, 1);
      r.onupgradeneeded = () => r.result.createObjectStore('kv');
      r.onsuccess = () => resolve(r.result);
      r.onerror = () => reject(r.error);
    });
  }

  async #store(mode: IDBTransactionMode): Promise<IDBObjectStore> {
    const db = await this.#dbp;
    return db.transaction('kv', mode).objectStore('kv');
  }

  async get(key: string): Promise<unknown> {
    return wrap((await this.#store('readonly')).get(key));
  }
  async set(key: string, value: unknown): Promise<void> {
    await wrap((await this.#store('readwrite')).put(value as unknown as never, key));
  }
  async del(key: string): Promise<void> {
    await wrap((await this.#store('readwrite')).delete(key));
  }
  async entries(prefix: string): Promise<[string, unknown][]> {
    const s = await this.#store('readonly');
    // Scan only the prefix's key range: [prefix, successor) — the successor is the
    // prefix with its last char incremented, so the range is exactly the keys that
    // start with `prefix` (IDB compares string keys by UTF-16 code unit). Avoids
    // materializing the whole store to read one namespace.
    const successor =
      prefix.slice(0, -1) + String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);
    const range = IDBKeyRange.bound(prefix, successor, false, true);
    const [keys, vals] = await Promise.all([wrap(s.getAllKeys(range)), wrap(s.getAll(range))]);
    return keys.map((k, i) => [String(k), vals[i]] as [string, unknown]);
  }
  /**
   * All ops in ONE readwrite transaction: atomic under a crash (IndexedDB
   * transactions are all-or-nothing) and one commit instead of one per key —
   * the flush of a large poke goes from N transactions to 1.
   */
  async batch(ops: BatchOp[]): Promise<void> {
    if (ops.length === 0) return;
    const db = await this.#dbp;
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction('kv', 'readwrite');
      const s = tx.objectStore('kv');
      for (const op of ops) {
        if (op.type === 'set') s.put(op.value as unknown as never, op.key);
        else s.delete(op.key);
      }
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
  }
}
