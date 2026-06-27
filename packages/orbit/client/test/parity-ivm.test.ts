// Incremental parity: replays Zero's differential corpora through the TS IVM
// operator graph (ivm/*), applying each mutation as a push and comparing the
// materialized result to Zero's snapshot after EVERY change. This validates the
// incremental engine (not just a from-scratch evaluation): each push exercises
// the operators' add/remove/edit propagation.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { buildPipeline } from '../src/ivm/build.ts';
import { MaterializedView } from '../src/ivm/view.ts';
import { MemorySourceProvider } from '../src/ivm/source.ts';
import { completeOrder } from '../src/ivm/data.ts';
import type { SourceChange } from '../src/ivm/data.ts';
import type { AST, Row } from '../src/protocol.ts';

type GTable = { pk: string[]; rows: Row[] };
type GPush = { table: string; op: 'add' | 'remove' | 'edit'; row: Row; oldRow?: Row };
type Snap = { row: Row; rels: Record<string, Snap[]> };
type Golden = { name: string; tables: Record<string, GTable>; ast: AST; pushes: GPush[]; snapshots: Snap[][] };

const load = (rel: string): Golden[] =>
  JSON.parse(readFileSync(fileURLToPath(new URL(rel, import.meta.url)), 'utf8'));

function pushToChange(p: GPush): SourceChange {
  if (p.op === 'add') return { type: 'add', row: p.row };
  if (p.op === 'remove') return { type: 'remove', row: p.row };
  return { type: 'edit', row: p.row, oldRow: p.oldRow! };
}

function runScenario(g: Golden): string | null {
  const provider = new MemorySourceProvider();
  for (const [name, t] of Object.entries(g.tables)) provider.add(name, t.pk, t.rows);

  const pk = provider.pkOf(g.ast.table);
  const top = buildPipeline(g.ast, provider);
  const view = new MaterializedView(top, completeOrder(g.ast.orderBy, pk), pk);

  const snaps: unknown[] = [view.snapshot()];
  for (const p of g.pushes) {
    provider.push(p.table, pushToChange(p));
    snaps.push(view.snapshot());
  }
  if (snaps.length !== g.snapshots.length) {
    return `${g.name}: snapshot count ${snaps.length} != ${g.snapshots.length}`;
  }
  for (let i = 0; i < snaps.length; i++) {
    try {
      assert.deepEqual(snaps[i], g.snapshots[i]);
    } catch {
      return `${g.name}: snapshot ${i} differs\n  orbit-ivm: ${JSON.stringify(snaps[i])}\n  zero:      ${JSON.stringify(g.snapshots[i])}`;
    }
  }
  return null;
}

function runCorpus(label: string, file: string): void {
  test(`IVM ${label} matches Zero (incremental, with pushes)`, () => {
    const goldens = load(file);
    const failures: string[] = [];
    for (const g of goldens) {
      let f: string | null;
      try {
        f = runScenario(g);
      } catch (e) {
        f = `${g.name}: THREW ${String((e as Error)?.message ?? e)}`;
      }
      if (f) failures.push(f);
    }
    console.log(`  IVM ${label}: ${goldens.length - failures.length}/${goldens.length} matched Zero`);
    assert.equal(failures.length, 0, `\n${failures.slice(0, 5).join('\n\n')}`);
  });
}

runCorpus('hand-written', '../../../../crates/oql/tests/golden/zql_golden.json');
runCorpus('fuzz', '../../../../crates/oql/tests/golden/zql_fuzz_golden.json');
runCorpus('related/exists', '../../../../crates/oql/tests/golden/zql_related_golden.json');
