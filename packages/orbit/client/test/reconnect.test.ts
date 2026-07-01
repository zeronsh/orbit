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
  closeWith(code: number) {
    this.readyState = 3;
    this.#emit('close', { code });
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

// These two only exercise persistence, not the socket. Force `#connect` to be a
// no-op by removing the global WebSocket: Node 21+ exposes one, so otherwise the
// client opens a real (failing) connection to ws://x whose async cleanup lands
// after the test ends (undici → "Maximum call stack size exceeded").
test('persist: clientID + mutation-id survive a reload (fast resume, ids keep climbing)', async () => {
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = undefined;
  const kv = new MemoryKV();
  const make = () => new Orbit({ server: 'ws://x', schema, persist: kv, maxReconnectMs: 0 });
  try {
    const a = make();
    await settle(); // #init hydrates + persists a fresh identity
    a.mutate.todo.insert({ id: 't1', text: 'a', done: false });
    a.mutate.todo.insert({ id: 't2', text: 'b', done: false });
    await settle();
    const id = a.clientID;
    assert.equal(await kv.get('nextMutationID'), 3, 'sent ids 1,2 → next is 3');

    // Simulate a reload: the old tab dies (releasing its Web-Locks persistence
    // leadership — a real unload does this implicitly), then a brand-new client
    // starts over the SAME persisted KV.
    a.close();
    await settle();
    const b = make();
    await settle();
    assert.equal(b.clientID, id, 'clientID restored → resumes the same server CVR');
    b.mutate.todo.insert({ id: 't3', text: 'c', done: false });
    await settle();
    assert.equal(await kv.get('nextMutationID'), 4, 'mutation ids continue past reload (not reset to 1)');
    b.close(); // release the persistence lock for later tests
    await settle();
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('persist: an explicit clientID is never overridden by the KV', async () => {
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = undefined;
  const kv = new MemoryKV();
  await kv.set('clientID', 'persisted-other');
  try {
    const a = new Orbit({ server: 'ws://x', schema, persist: kv, clientID: 'explicit', maxReconnectMs: 0 });
    await settle();
    assert.equal(a.clientID, 'explicit');
    a.close();
    await settle();
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('persist: a second tab (same KV) becomes a memory-only follower — no shared-cache writes', async () => {
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = undefined;
  // Deterministic Web-Locks mock: CI Node (22) has no navigator.locks, and the
  // client's graceful no-locks fallback would make BOTH instances leaders. The mock
  // implements the same contract the client relies on: ifAvailable grants the first
  // requester, returns null while held, and releases when the holder's callback
  // promise settles (our release fn / tab death).
  const prevNavDesc = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
  const held = new Set<string>();
  const locks = {
    request(name: string, _opts: { ifAvailable: boolean }, cb: (lock: unknown) => unknown): Promise<unknown> {
      if (held.has(name)) return Promise.resolve(cb(null));
      held.add(name);
      const done = Promise.resolve(cb({ name }));
      void done.finally(() => held.delete(name));
      return done;
    },
  };
  Object.defineProperty(globalThis, 'navigator', { value: { locks }, configurable: true });
  const kv = new MemoryKV();
  const make = () => new Orbit({ server: 'ws://x', schema, persist: kv, maxReconnectMs: 0 });
  try {
    const leader = make();
    await settle();
    const leaderID = leader.clientID;
    assert.equal(await kv.get('clientID'), leaderID, 'leader persisted its identity');

    // "Second tab": same KV while the leader is still alive → follower.
    const follower = make();
    await settle();
    assert.notEqual(follower.clientID, leaderID, 'follower gets its own fresh clientID');
    assert.equal(await kv.get('clientID'), leaderID, 'follower must not overwrite the shared identity');

    // Follower mutations stay in memory: no pending entries or id marks in the KV
    // beyond what the leader wrote (nextMutationID would be from the leader only).
    follower.mutate.todo.insert({ id: 'f1', text: 'x', done: false });
    await settle();
    assert.equal(await kv.get('nextMutationID'), undefined, 'follower writes nothing to the shared KV');
    assert.deepEqual(await kv.entries('p/'), [], 'follower pending mutations are not persisted');

    // Leader death (tab closed) releases the lock; the NEXT instance leads again.
    leader.close();
    follower.close();
    await settle();
    const c = make();
    await settle();
    assert.equal(c.clientID, leaderID, 'a new instance after leader death restores the identity');
    c.close();
    await settle();
  } finally {
    if (prevNavDesc) Object.defineProperty(globalThis, 'navigator', prevNavDesc);
    else delete (globalThis as { navigator?: unknown }).navigator;
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('a mutation push is sent only after the id high-water mark is durable', async () => {
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  // A KV whose nextMutationID write is held until we release it.
  let releaseSet: (() => void) | undefined;
  class SlowKV extends MemoryKV {
    override set(key: string, value: unknown): Promise<void> {
      if (key === 'nextMutationID') {
        return new Promise<void>((resolve) => {
          releaseSet = () => {
            void super.set(key, value).then(resolve);
          };
        });
      }
      return super.set(key, value);
    }
  }
  const kv = new SlowKV();
  try {
    const orbit = new Orbit({ server: 'ws://x', schema, persist: kv, maxReconnectMs: 0 });
    await settle();
    const ws = MockWS.instances[0]!;
    ws.open();

    orbit.mutate.todo.insert({ id: 't1', text: 'hi', done: false });
    await settle();
    assert.ok(!tags(ws).includes('push'), 'push NOT sent while the id write is pending');

    releaseSet!();
    await settle();
    assert.ok(tags(ws).includes('push'), 'push sent once the id high-water mark is durable');
    orbit.close();
    await settle();
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
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

test('an oversized (poison) mutation is dropped on WS 1009, not looped forever', async () => {
  mock.timers.enable({ apis: ['setTimeout'] });
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = MockWS as unknown;
  MockWS.instances.length = 0;
  const errors: { kind: string; message: string }[] = [];
  try {
    const orbit = new Orbit({
      server: 'ws://x',
      schema,
      clientID: 'c1',
      maxReconnectMs: 1000,
      onError: (e) => errors.push(e),
    });
    await Promise.resolve();
    const ws1 = MockWS.instances[0]!;
    ws1.open();

    // A small mutation (id 1) and an oversized "poison" mutation (id 2).
    orbit.mutate.todo.insert({ id: 't1', text: 'hi', done: false });
    orbit.mutate.todo.insert({ id: 't2', text: 'x'.repeat(5000), done: false });

    // The server rejects the oversized frame by closing with WS code 1009.
    ws1.closeWith(1009);
    mock.timers.tick(1000);
    await Promise.resolve();

    const ws2 = MockWS.instances[1]!;
    assert.ok(ws2, 'reconnected');
    ws2.open();

    // Only the small mutation is resent; the poison one was dropped (loop broken).
    const pushIds = parsed(ws2)
      .filter((m) => m[0] === 'push')
      .flatMap((p) => (p[1].mutations as { id: number }[]).map((m) => m.id));
    assert.deepEqual(pushIds, [1], 'only mutation 1 resent; oversized mutation 2 dropped');
    assert.equal(errors.length, 1, 'onError fired once');
    assert.equal(errors[0]!.kind, 'mutation-too-large');
  } finally {
    mock.timers.reset();
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});
