// Differential parity against Zero's **real** zql engine — the same certification
// the Rust engine passes, now applied to the TypeScript client evaluator.
//
// `crates/oql/tests/golden/*.json` were produced by running Zero's own zql (via
// `mono/orbit-golden/gen.ts`) over query scenarios, recording the materialized
// result after the initial load and after each mutation. This replays the
// identical scenarios through `@orbit/client`'s `evaluate()` + `Store` and
// asserts byte-identical snapshots. If these pass, the client engine produces
// the same query results as Zero.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Store } from '../src/store.ts';
import { evaluate } from '../src/eval.ts';
import type { AST, Condition, CorrelatedSubquery, Row } from '../src/protocol.ts';

type GTable = { columns: Record<string, { type: string }>; pk: string[]; rows: Row[] };
type GPush = { table: string; op: 'add' | 'remove' | 'edit'; row: Row; oldRow?: Row };
type Snap = { row: Row; rels: Record<string, Snap[]> };
type Golden = { name: string; tables: Record<string, GTable>; ast: AST; pushes: GPush[]; snapshots: Snap[][] };

const load = (rel: string): Golden[] =>
  JSON.parse(readFileSync(fileURLToPath(new URL(rel, import.meta.url)), 'utf8'));

function buildStore(tables: Record<string, GTable>): Store {
  const pk: Record<string, string[]> = {};
  for (const [name, t] of Object.entries(tables)) pk[name] = t.pk;
  const store = new Store(pk);
  const ops = [];
  for (const [name, t] of Object.entries(tables)) {
    for (const r of t.rows) ops.push({ op: 'put' as const, tableName: name, value: r });
  }
  store.applyAll(ops);
  return store;
}

function applyPush(store: Store, p: GPush): void {
  if (p.op === 'add') store.applyAll([{ op: 'put', tableName: p.table, value: p.row }]);
  else if (p.op === 'remove') store.applyAll([{ op: 'del', tableName: p.table, id: p.row }]);
  else store.applyAll([
    { op: 'del', tableName: p.table, id: p.oldRow! },
    { op: 'put', tableName: p.table, value: p.row },
  ]);
}

/** All visible relationships of a node: explicit `related` + visibly-aliased EXISTS. */
function visibleRels(ast: AST): CorrelatedSubquery[] {
  const rels = [...(ast.related ?? [])];
  const walk = (c: Condition | undefined): void => {
    if (!c) return;
    if (c.type === 'correlatedSubquery') {
      const a = c.related.subquery.alias;
      if (a && !a.startsWith('zsubq_')) rels.push(c.related);
    } else if (c.type === 'and' || c.type === 'or') c.conditions.forEach(walk);
  };
  walk(ast.where);
  return rels;
}

/** Convert a nested evaluate() row into Zero's `{row, rels}` snapshot shape. */
function toSnap(resultRow: Record<string, unknown>, ast: AST): Snap {
  const related = visibleRels(ast);
  const aliases = new Set(related.map((r) => r.subquery.alias ?? r.subquery.table));
  const row: Row = {};
  for (const k of Object.keys(resultRow)) if (!aliases.has(k)) row[k] = resultRow[k] as Row[string];
  const rels: Record<string, Snap[]> = {};
  for (const rel of related) {
    const alias = rel.subquery.alias ?? rel.subquery.table;
    const children = (resultRow[alias] as Record<string, unknown>[]) ?? [];
    rels[alias] = children.map((c) => toSnap(c, rel.subquery));
  }
  return { row, rels };
}

function snapshot(store: Store, ast: AST): Snap[] {
  return evaluate((t) => store.effectiveRows(t), ast, { pkOf: (t) => store.pkOf(t) }).map((r) =>
    toSnap(r, ast),
  );
}

function runScenario(g: Golden): string | null {
  const store = buildStore(g.tables);
  const snaps: Snap[][] = [snapshot(store, g.ast)];
  for (const p of g.pushes) {
    applyPush(store, p);
    snaps.push(snapshot(store, g.ast));
  }
  if (snaps.length !== g.snapshots.length) {
    return `${g.name}: snapshot count ${snaps.length} != ${g.snapshots.length}`;
  }
  for (let i = 0; i < snaps.length; i++) {
    try {
      assert.deepEqual(snaps[i], g.snapshots[i]);
    } catch {
      return `${g.name}: snapshot ${i} differs\n  orbit: ${JSON.stringify(snaps[i])}\n  zero:  ${JSON.stringify(g.snapshots[i])}`;
    }
  }
  return null;
}

function runCorpus(label: string, file: string): void {
  test(`${label} matches Zero's zql engine`, () => {
    const goldens = load(file);
    const failures: string[] = [];
    for (const g of goldens) {
      const f = runScenario(g);
      if (f) failures.push(f);
    }
    const passed = goldens.length - failures.length;
    console.log(`  ${label}: ${passed}/${goldens.length} scenarios matched Zero`);
    assert.equal(failures.length, 0, `\n${failures.slice(0, 5).join('\n\n')}`);
  });
}

runCorpus('hand-written', '../../../../crates/oql/tests/golden/zql_golden.json');
runCorpus('fuzz', '../../../../crates/oql/tests/golden/zql_fuzz_golden.json');
runCorpus('related/exists', '../../../../crates/oql/tests/golden/zql_related_golden.json');
