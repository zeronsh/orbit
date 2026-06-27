import { test } from 'node:test';
import assert from 'node:assert/strict';
import { QueryManager, parseTTL, type QueryPut } from '../src/query-manager.ts';

function fakeClock() {
  let now = 0;
  let id = 0;
  const timers = new Map<number, { fn: () => void; at: number }>();
  return {
    scheduler: {
      setTimeout: (fn: () => void, ms: number) => {
        const t = ++id;
        timers.set(t, { fn, at: now + ms });
        return t;
      },
      clearTimeout: (h: unknown) => {
        timers.delete(h as number);
      },
    },
    tick(ms: number) {
      now += ms;
      for (const [t, e] of [...timers]) if (e.at <= now) {
        timers.delete(t);
        e.fn();
      }
    },
  };
}

const put = (hash: string): QueryPut => ({ op: 'put', hash, ast: { table: 't' } });

function mk(scheduler: ReturnType<typeof fakeClock>['scheduler']) {
  const subs: string[] = [];
  const unsubs: string[] = [];
  const qm = new QueryManager({
    onSubscribe: (p) => subs.push(p.hash),
    onUnsubscribe: (h) => unsubs.push(h),
    scheduler,
  });
  return { qm, subs, unsubs };
}

test('parseTTL parses units, numbers, forever/none', () => {
  assert.equal(parseTTL('5m'), 300_000);
  assert.equal(parseTTL('30s'), 30_000);
  assert.equal(parseTTL('2h'), 7_200_000);
  assert.equal(parseTTL(1234), 1234);
  assert.equal(parseTTL('forever'), Infinity);
  assert.equal(parseTTL('none'), 0);
});

test('dedups identical queries to one subscription', () => {
  const { qm, subs } = mk(fakeClock().scheduler);
  qm.add('h1', put('h1'), '5m');
  qm.add('h1', put('h1'), '5m');
  assert.deepEqual(subs, ['h1']);
  assert.equal(qm.size(), 1);
});

test('GCs only after TTL once all views are released', () => {
  const clock = fakeClock();
  const { qm, unsubs } = mk(clock.scheduler);
  const r1 = qm.add('h1', put('h1'), '5m');
  const r2 = qm.add('h1', put('h1'), '5m');
  r1();
  clock.tick(10 * 60_000);
  assert.deepEqual(unsubs, []); // r2 still holds a reference
  r2();
  clock.tick(4 * 60_000);
  assert.deepEqual(unsubs, []); // within the 5m TTL
  clock.tick(2 * 60_000);
  assert.deepEqual(unsubs, ['h1']); // GC'd after TTL
  assert.equal(qm.size(), 0);
});

test('re-add before TTL cancels GC and does not re-subscribe', () => {
  const clock = fakeClock();
  const { qm, subs, unsubs } = mk(clock.scheduler);
  qm.add('h1', put('h1'), '5m')();
  clock.tick(60_000);
  qm.add('h1', put('h1'), '5m'); // revived within TTL
  clock.tick(10 * 60_000);
  assert.deepEqual(unsubs, []);
  assert.deepEqual(subs, ['h1']); // subscribed once, never re-sent
});

test('forever never GCs; none GCs immediately', () => {
  const clock = fakeClock();
  const { qm, unsubs } = mk(clock.scheduler);
  qm.add('f', put('f'), 'forever')();
  clock.tick(1e9);
  assert.deepEqual(unsubs, []);
  qm.add('n', put('n'), 'none')();
  assert.deepEqual(unsubs, ['n']);
});

test('active() returns live puts with ttl stamped', () => {
  const { qm } = mk(fakeClock().scheduler);
  qm.add('h1', put('h1'), '5m');
  const a = qm.active();
  assert.equal(a.length, 1);
  assert.equal(a[0].op === 'put' ? a[0].ttl : undefined, 300_000);
});

test('release is idempotent', () => {
  const { qm, unsubs } = mk(fakeClock().scheduler);
  const r1 = qm.add('h1', put('h1'), 'none');
  qm.add('h1', put('h1'), 'none');
  r1();
  r1(); // double release must not drop the second reference
  assert.equal(qm.size(), 1);
});
