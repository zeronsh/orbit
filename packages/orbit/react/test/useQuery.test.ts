// Exhaustive behavior tests for `useQuery`. Uses react-test-renderer (no DOM) so it
// runs under `node --test --experimental-strip-types`; components are built with
// `createElement` (type-stripping doesn't transform JSX).
//
// Covers: initial data, reactive updates, query-change re-subscription (the bug
// where a route param changed in place and the view went stale), no subscription
// leak on change/unmount, stable-query no-op re-renders, and StrictMode.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { act, createElement as h, StrictMode } from 'react';
import TestRenderer from 'react-test-renderer';
import { useQuery } from '../src/index.ts';

type Row = { id: string };

class FakeView {
  query: FakeQuery;
  data: Row[];
  destroyed = false;
  subs = new Set<() => void>();
  constructor(query: FakeQuery, data: Row[]) {
    this.query = query;
    this.data = data;
  }
  subscribe(fn: () => void) {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }
  destroy() {
    this.destroyed = true;
    this.subs.clear();
  }
  emit(data: Row[]) {
    this.data = data;
    for (const f of [...this.subs]) f();
  }
}

class FakeQuery {
  id: string;
  initial: Row[];
  materializeCount = 0;
  views: FakeView[] = [];
  constructor(id: string, initial: Row[]) {
    this.id = id;
    this.initial = initial;
  }
  materialize(): FakeView {
    this.materializeCount++;
    const v = new FakeView(this, this.initial);
    this.views.push(v);
    return v;
  }
  get live(): FakeView {
    return this.views[this.views.length - 1]!;
  }
  get aliveViews(): FakeView[] {
    return this.views.filter((v) => !v.destroyed);
  }
}

// Captures the hook's output each render so tests can assert on it.
let lastRows: Row[] = [];
// oxlint-disable-next-line no-explicit-any
function Probe({ query }: { query: any }) {
  lastRows = useQuery(query);
  return null;
}
const ids = (rows: Row[]) => rows.map((r) => r.id);

// oxlint-disable-next-line no-explicit-any
function render(query: any): any {
  let r: any; // eslint-disable-line @typescript-eslint/no-explicit-any
  act(() => {
    r = TestRenderer.create(h(Probe, { query }));
  });
  return r;
}

test('returns the query data on the first render + materializes once', () => {
  const q = new FakeQuery('a', [{ id: 'a' }]);
  const r = render(q);
  assert.deepEqual(lastRows, [{ id: 'a' }]);
  assert.equal(q.materializeCount, 1);
  act(() => r.unmount());
});

test('re-renders when the underlying view emits a change', () => {
  const q = new FakeQuery('a', [{ id: 'a' }]);
  const r = render(q);
  act(() => q.live.emit([{ id: 'a' }, { id: 'a2' }]));
  assert.deepEqual(ids(lastRows), ['a', 'a2']);
  act(() => r.unmount());
});

test('re-materializes + shows the new data when the query changes WITHOUT a remount', () => {
  const qA = new FakeQuery('A', [{ id: 'A' }]);
  const qB = new FakeQuery('B', [{ id: 'B' }]);
  const r = render(qA);
  assert.deepEqual(ids(lastRows), ['A']);
  // Same component instance, new query prop (e.g. a route param flipping).
  act(() => r.update(h(Probe, { query: qB })));
  assert.deepEqual(ids(lastRows), ['B'], 'shows B immediately after the query changes');
  assert.equal(qB.materializeCount, 1);
  act(() => r.unmount());
});

test('destroys the previous view + releases its subscription when the query changes', () => {
  const qA = new FakeQuery('A', [{ id: 'A' }]);
  const qB = new FakeQuery('B', [{ id: 'B' }]);
  const r = render(qA);
  const viewA = qA.live;
  act(() => r.update(h(Probe, { query: qB })));
  assert.ok(viewA.destroyed, 'old view destroyed');
  assert.equal(viewA.subs.size, 0, 'old subscription released');
  assert.equal(qA.aliveViews.length, 0);
  assert.equal(qB.aliveViews.length, 1, 'exactly one live view (the new one)');
  act(() => r.unmount());
});

test('after a query change, the new view drives updates and stale-view emits are ignored', () => {
  const qA = new FakeQuery('A', [{ id: 'A' }]);
  const qB = new FakeQuery('B', [{ id: 'B' }]);
  const r = render(qA);
  const viewA = qA.live;
  act(() => r.update(h(Probe, { query: qB })));
  act(() => qB.live.emit([{ id: 'B' }, { id: 'B2' }]));
  assert.deepEqual(ids(lastRows), ['B', 'B2']);
  act(() => viewA.emit([{ id: 'STALE' }])); // destroyed view → no subscribers
  assert.deepEqual(ids(lastRows), ['B', 'B2'], 'stale emit ignored');
  act(() => r.unmount());
});

test('a stable query is materialized once across unrelated re-renders', () => {
  const q = new FakeQuery('a', [{ id: 'a' }]);
  const r = render(q);
  act(() => r.update(h(Probe, { query: q })));
  act(() => r.update(h(Probe, { query: q })));
  assert.equal(q.materializeCount, 1, 'not re-materialized for the same query');
  assert.equal(q.aliveViews.length, 1);
  act(() => r.unmount());
});

test('destroys the view + releases the subscription on unmount', () => {
  const q = new FakeQuery('a', [{ id: 'a' }]);
  const r = render(q);
  const view = q.live;
  act(() => r.unmount());
  assert.ok(view.destroyed, 'view destroyed on unmount');
  assert.equal(view.subs.size, 0);
  assert.equal(q.aliveViews.length, 0);
});

test('StrictMode: exactly one live subscription after mount, all released on unmount', () => {
  const q = new FakeQuery('a', [{ id: 'a' }]);
  let r: any; // eslint-disable-line @typescript-eslint/no-explicit-any
  act(() => {
    r = TestRenderer.create(h(StrictMode, null, h(Probe, { query: q })));
  });
  assert.deepEqual(ids(lastRows), ['a']);
  assert.equal(q.aliveViews.length, 1, 'no leaked view under StrictMode double-invoke');
  act(() => q.aliveViews[0]!.emit([{ id: 'a' }, { id: 'z' }]));
  assert.deepEqual(ids(lastRows), ['a', 'z'], 'the live view still drives updates');
  act(() => r.unmount());
  assert.equal(q.aliveViews.length, 0, 'all views destroyed on unmount');
});
