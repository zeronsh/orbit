// Exercises the client's WebSocket reconnect/resume path with a mock WebSocket:
// on reconnect it must resubscribe active queries and resend unconfirmed
// mutations, and must NOT resend a mutation the server already confirmed.

import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { Orbit, createSchema, table, string, boolean, MemoryKV } from '../src/index.ts';

const schema = createSchema({
  tables: [table('todo').columns({ id: string(), text: string(), done: boolean() }).primaryKey('id')],
});

class MockWS {
  static instances: MockWS[] = [];
  static OPEN = 1;
  readyState = 0;
  sent: string[] = [];
  #listeners: Record<string, ((ev: unknown) => void)[]> = {};
  url: string;
  protocols?: string[];
  constructor(url: string, protocols?: string[]) {
    this.url = url;
    this.protocols = protocols;
    MockWS.instances.push(this);
  }
  addEventListener(type: string, fn: (ev: unknown) => void) {
    (this.#listeners[type] ??= []).push(fn);
  }
  send(data: string) {
    this.sent.push(data);
  }
  close() {
    this.readyState = 3;
    this.#emit('close', {});
  }
  open() {
    this.readyState = MockWS.OPEN;
    this.#emit('open', {});
  }
  message(obj: unknown) {
    this.#emit('message', { data: JSON.stringify(obj) });
  }
  #emit(type: string, ev: unknown) {
    for (const fn of this.#listeners[type] ?? []) fn(ev);
  }
}

/** All upstream message tags sent on a socket. */
const tags = (ws: MockWS) => ws.sent.map((s) => (JSON.parse(s) as [string])[0]);
const parsed = (ws: MockWS) => ws.sent.map((s) => JSON.parse(s) as [string, Record<string, unknown>]);

test('reconnect resubscribes active queries and resends unconfirmed mutations', async () => {
  mock.timers.enable({ apis: ['setTimeout'] });
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  try {
    const orbit = new Orbit({ server: 'ws://x', schema, clientID: 'c1', maxReconnectMs: 1000 });
    await Promise.resolve();
    const ws1 = MockWS.instances[0]!;
    assert.ok(ws1, 'first socket created');
    ws1.open();

    // Subscribe a query + run a mutation while connected.
    const view = orbit.query.todo.where('done', '=', false).materialize();
    orbit.mutate.todo.insert({ id: 't1', text: 'hi', done: false });
    assert.deepEqual(tags(ws1).sort(), ['changeDesiredQueries', 'push']);

    // Drop the connection → schedule reconnect → new socket.
    ws1.close();
    mock.timers.tick(1000);
    await Promise.resolve();
    const ws2 = MockWS.instances[1]!;
    assert.ok(ws2, 'reconnected socket created');
    assert.equal(ws2.sent.length, 0, 'nothing sent before reopen');

    // On reopen it must resume: resubscribe the query AND resend the mutation.
    ws2.open();
    assert.deepEqual(tags(ws2).sort(), ['changeDesiredQueries', 'push']);
    const resentPush = parsed(ws2).find((m) => m[0] === 'push')!;
    assert.equal((resentPush[1].mutations as { id: number }[])[0].id, 1);

    view.destroy?.();
  } finally {
    mock.timers.reset();
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('a confirmed mutation is not resent on reconnect', async () => {
  mock.timers.enable({ apis: ['setTimeout'] });
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  try {
    const orbit = new Orbit({ server: 'ws://x', schema, clientID: 'c1', maxReconnectMs: 1000 });
    await Promise.resolve();
    const ws1 = MockWS.instances[0]!;
    ws1.open();
    orbit.mutate.todo.insert({ id: 't1', text: 'hi', done: false });

    // Server confirms mutation id 1 via a complete poke (start/part/end — the
    // client applies a poke atomically on pokeEnd).
    ws1.message(['pokeStart', { pokeID: 'p', baseCookie: null }]);
    ws1.message(['pokePart', { pokeID: 'p', lastMutationIDChanges: { c1: 1 } }]);
    ws1.message(['pokeEnd', { pokeID: 'p', cookie: '1' }]);

    ws1.close();
    mock.timers.tick(1000);
    await Promise.resolve();
    const ws2 = MockWS.instances[1]!;
    ws2.open();
    assert.ok(!tags(ws2).includes('push'), 'confirmed mutation must not be resent');
  } finally {
    mock.timers.reset();
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

const putTodo = (id: string) => ({
  op: 'put' as const,
  tableName: 'todo',
  value: { id, text: id, done: false },
});
const settle = () => new Promise((r) => setTimeout(r, 0));

test('persist: clientID + mutation-id survive a reload (fast resume, ids keep climbing)', async () => {
  const kv = new MemoryKV();
  // No WebSocket in node → #connect is a no-op; maxReconnectMs:0 → no timers.
  const make = () => new Orbit({ server: 'ws://x', schema, persist: kv, maxReconnectMs: 0 });

  const a = make();
  await settle(); // #init hydrates + persists a fresh identity
  a.mutate.todo.insert({ id: 't1', text: 'a', done: false });
  a.mutate.todo.insert({ id: 't2', text: 'b', done: false });
  await settle();
  const id = a.clientID;
  assert.equal(await kv.get('nextMutationID'), 3, 'sent ids 1,2 → next is 3');

  // Simulate a reload: a brand-new client over the SAME persisted KV.
  const b = make();
  await settle();
  assert.equal(b.clientID, id, 'clientID restored → resumes the same server CVR');
  b.mutate.todo.insert({ id: 't3', text: 'c', done: false });
  await settle();
  assert.equal(await kv.get('nextMutationID'), 4, 'mutation ids continue past reload (not reset to 1)');
});

test('persist: an explicit clientID is never overridden by the KV', async () => {
  const kv = new MemoryKV();
  await kv.set('clientID', 'persisted-other');
  const a = new Orbit({ server: 'ws://x', schema, persist: kv, clientID: 'explicit', maxReconnectMs: 0 });
  await settle();
  assert.equal(a.clientID, 'explicit');
});
const todoIds = (v: { data: readonly { id: string }[] }) => v.data.map((r) => r.id);

test('a poke is applied atomically on pokeEnd; a mid-poke disconnect discards the partial', async () => {
  mock.timers.enable({ apis: ['setTimeout'] });
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  try {
    const orbit = new Orbit({ server: 'ws://x', schema, clientID: 'c1', maxReconnectMs: 1000 });
    await Promise.resolve();
    const ws1 = MockWS.instances[0]!;
    ws1.open();
    const view = orbit.query.todo.materialize() as unknown as { data: { id: string }[]; destroy?: () => void };

    // Partial poke: start + part (put t1) but NO pokeEnd, then disconnect.
    ws1.message(['pokeStart', { pokeID: 'p1', baseCookie: null }]);
    ws1.message(['pokePart', { pokeID: 'p1', rowsPatch: [putTodo('t1')] }]);
    assert.equal(view.data.length, 0, 'rows are not applied until pokeEnd');
    ws1.close(); // mid-poke disconnect
    assert.equal(view.data.length, 0, 'the partial poke is discarded on disconnect (no torn state)');

    // Reconnect; a COMPLETE poke applies atomically.
    mock.timers.tick(1000);
    await Promise.resolve();
    const ws2 = MockWS.instances[1]!;
    ws2.open();
    ws2.message(['pokeStart', { pokeID: 'p2', baseCookie: null }]);
    ws2.message(['pokePart', { pokeID: 'p2', rowsPatch: [putTodo('t1')] }]);
    ws2.message(['pokeEnd', { pokeID: 'p2', cookie: '1' }]);
    assert.deepEqual(todoIds(view), ['t1'], 'a complete poke is applied');
    view.destroy?.();
  } finally {
    mock.timers.reset();
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('pokeEnd.cancel discards the buffered poke', async () => {
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  try {
    const orbit = new Orbit({ server: 'ws://x', schema, clientID: 'c1' });
    await Promise.resolve();
    const ws = MockWS.instances[0]!;
    ws.open();
    const view = orbit.query.todo.materialize() as unknown as { data: { id: string }[]; destroy?: () => void };
    ws.message(['pokeStart', { pokeID: 'p', baseCookie: null }]);
    ws.message(['pokePart', { pokeID: 'p', rowsPatch: [putTodo('t2')] }]);
    ws.message(['pokeEnd', { pokeID: 'p', cookie: '1', cancel: true }]);
    assert.equal(view.data.length, 0, 'a canceled poke is not applied');
    view.destroy?.();
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('a terminal server `error` is surfaced to onError (not silently dropped)', async () => {
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  const errors: { kind: string; message: string }[] = [];
  try {
    const orbit = new Orbit({ server: 'ws://x', schema, clientID: 'c1', onError: (e) => errors.push(e) });
    await Promise.resolve();
    const ws = MockWS.instances[0]!;
    ws.open();
    ws.message(['error', { kind: 'Unauthorized', message: 'bad token' }]);
    assert.deepEqual(errors, [{ kind: 'Unauthorized', message: 'bad token' }]);
    orbit.close();
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});
