// Benchmark: incremental IVM vs. full re-evaluation, over a stream of mutations.
// Run: node --experimental-strip-types bench/ivm-bench.ts

import { performance } from 'node:perf_hooks';
import { evaluate } from '../src/eval.ts';
import { buildPipeline } from '../src/ivm/build.ts';
import { MaterializedView } from '../src/ivm/view.ts';
import { MemorySourceProvider } from '../src/ivm/source.ts';
import { completeOrder } from '../src/ivm/data.ts';
import type { AST, Row } from '../src/protocol.ts';

const N = 10_000;
const MUTATIONS = 5_000;
const pk = ['id'];

// Query: WHERE active = true ORDER BY created LIMIT 50 — exercises Filter + Take.
const ast: AST = {
  table: 't',
  where: { type: 'simple', op: '=', left: { type: 'column', name: 'active' }, right: { type: 'literal', value: true } },
  orderBy: [['created', 'asc']],
  limit: 50,
};

function makeRow(i: number): Row {
  return { id: `r${i}`, active: i % 2 === 0, created: (i * 2654435761) % 1_000_000 };
}

const initial: Row[] = [];
for (let i = 0; i < N; i++) initial.push(makeRow(i));

// Deterministic mutation stream (update the `active` flag of an existing row).
const mutations = Array.from({ length: MUTATIONS }, (_, k) => {
  const i = (k * 7919) % N;
  return { ...makeRow(i), active: (k & 1) === 0 ? true : false } as Row;
});

// --- full re-evaluation (eval.ts) ------------------------------------------
function benchEval(): number {
  const map = new Map<string, Row>();
  for (const r of initial) map.set(r.id as string, r);
  const getRows = (t: string) => (t === 't' ? [...map.values()] : []);
  const start = performance.now();
  for (const m of mutations) {
    map.set(m.id as string, m); // update
    evaluate(getRows, ast, { pkOf: () => pk }); // re-evaluate the whole query
  }
  return performance.now() - start;
}

// --- incremental IVM --------------------------------------------------------
function benchIvm(): { ms: number; reuseRatio: number } {
  const provider = new MemorySourceProvider();
  provider.add('t', pk, initial.map((r) => ({ ...r })));
  const top = buildPipeline(ast, provider);
  const view = new MaterializedView(top, completeOrder(ast.orderBy, pk), pk);

  const start = performance.now();
  let reused = 0;
  let total = 0;
  for (const m of mutations) {
    const before = view.nodes;
    const beforeSet = new Set(before.map((n) => n.row));
    provider.push('t', { type: 'edit', row: m, oldRow: { ...m, active: !m.active } });
    // Count how many result rows kept their identity (no re-render needed).
    for (const n of view.nodes) if (beforeSet.has(n.row)) reused++;
    total += view.nodes.length;
  }
  const ms = performance.now() - start;
  return { ms, reuseRatio: reused / total };
}

const evalMs = benchEval();
const ivm = benchIvm();
process.stdout.write(
  `N=${N} rows, ${MUTATIONS} mutations, query: WHERE active=true ORDER BY created LIMIT 50\n` +
    `  re-eval (eval.ts):   ${evalMs.toFixed(0)} ms  (${(evalMs / MUTATIONS).toFixed(3)} ms/mutation)\n` +
    `  incremental (IVM):   ${ivm.ms.toFixed(0)} ms  (${(ivm.ms / MUTATIONS).toFixed(3)} ms/mutation)\n` +
    `  speedup:             ${(evalMs / ivm.ms).toFixed(1)}x\n` +
    `  result-row identity reused across mutations: ${(ivm.reuseRatio * 100).toFixed(1)}%\n`,
);
