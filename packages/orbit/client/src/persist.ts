// Pluggable client persistence — a small async key/value abstraction (mirroring
// the role of Zero/Replicache's `kv/store.ts`, without the dag/btree). The Store
// persists synced rows under `e/…` keys and pending mutations under `p/…`, so
// data is available offline and survives reloads. `IDBKV` backs it with
// IndexedDB in the browser; `MemoryKV` is for tests / non-browser environments.

export interface KV {
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown): Promise<void>;
  del(key: string): Promise<void>;
  /** All `[key, value]` pairs whose key starts with `prefix`. */
  entries(prefix: string): Promise<[string, unknown][]>;
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
    const [keys, vals] = await Promise.all([wrap(s.getAllKeys()), wrap(s.getAll())]);
    const out: [string, unknown][] = [];
    for (let i = 0; i < keys.length; i++) {
      const k = String(keys[i]);
      if (k.startsWith(prefix)) out.push([k, vals[i]]);
    }
    return out;
  }
}
