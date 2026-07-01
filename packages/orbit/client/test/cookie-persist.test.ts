// Regression: the persisted resume cookie must never be ahead of the persisted
// rows. If it is, a reload sends a `baseCookie` that makes the server's delta-resume
// SUPPRESS rows the client never durably received — permanent, asymmetric divergence
// (device A never sees device B's writes, even across refresh). The store persists
// the cookie only in `flush`, AFTER the row writes.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store } from '../src/store.ts';
import { MemoryKV } from '../src/persist.ts';
import type { KV } from '../src/persist.ts';
import type { RowPatchOp } from '../src/protocol.ts';

/** A KV that records the order of `set` keys, to assert rows-before-cookie. */
class RecordingKV implements KV {
  #m = new Map<string, unknown>();
  order: string[] = [];
  async get(k: string): Promise<unknown> {
    return this.#m.get(k);
  }
  async set(k: string, v: unknown): Promise<void> {
    this.order.push(k);
    this.#m.set(k, v);
  }
  async del(k: string): Promise<void> {
    this.#m.delete(k);
  }
  async entries(prefix: string): Promise<[string, unknown][]> {
    return [...this.#m.entries()].filter(([k]) => k.startsWith(prefix));
  }
}

const put = (id: string): RowPatchOp => ({ op: 'put', tableName: 'todo', value: { id } });

test('flush persists the cookie AFTER the rows it covers', async () => {
  const kv = new RecordingKV();
  const store = new Store({ todo: ['id'] });
  await store.hydrate(kv);

  store.applyAll([put('a'), put('b')]);
  store.setCookie('00000001');
  await store.flush();

  const firstRow = kv.order.findIndex((k) => k.startsWith('e/'));
  const cookieAt = kv.order.indexOf('cookie');
  assert.ok(firstRow >= 0, 'rows were persisted');
  assert.ok(cookieAt >= 0, 'cookie was persisted');
  assert.ok(cookieAt > firstRow, `cookie must be written after rows: ${kv.order.join(',')}`);
});

test('a reload never restores a cookie ahead of the cached rows', async () => {
  const kv = new MemoryKV();

  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  // Poke 1: row {a} at cookie C1 — flushed (durable).
  a.applyAll([put('a')]);
  a.setCookie('00000001');
  await a.flush();
  // Poke 2: row {b} at cookie C2 — applied + cookie recorded, but the tab reloads
  // BEFORE the debounced flush runs (the exact race the bug hit during live drawing).
  a.applyAll([put('b')]);
  a.setCookie('00000002');
  // (no flush)

  // Reload: a fresh store hydrates from the same KV.
  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);

  // The durable cookie is only C1 (C2 was never flushed) — NOT ahead of the rows.
  assert.equal(b.cookie(), '00000001');
  // And it holds exactly what C1 covers: {a}. It must NOT claim C2 while missing {b}.
  assert.deepEqual(
    b.effectiveRows('todo').map((r) => r.id).sort(),
    ['a'],
  );
});

test('after a full flush the cookie and its rows survive a reload together', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  a.applyAll([put('a'), put('b')]);
  a.setCookie('00000005');
  await a.flush();

  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);
  assert.equal(b.cookie(), '00000005');
  assert.deepEqual(
    b.effectiveRows('todo').map((r) => r.id).sort(),
    ['a', 'b'],
  );
});
