#!/usr/bin/env node
// Single source of truth for the Orbit version: `@zeronsh/orbit`'s package.json
// (managed by changesets). This propagates that version to the Rust workspace so
// the npm package, the `orbit-server` binary, and the `ghcr.io/zeronsh/orbit-server`
// image are always released in lockstep at the same version.
//
//   node scripts/sync-versions.mjs           # write the npm version into Rust
//   node scripts/sync-versions.mjs --check   # fail if they have drifted (CI guard)
//
// Run automatically by `pnpm version-packages` (after `changeset version`).

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const check = process.argv.includes('--check');

// Rust crates that inherit `version.workspace = true` (kept in sync via Cargo.lock).
const CRATES = ['oql', 'orbit-protocol', 'orbit-schema', 'orbit-cache'];

const version = JSON.parse(readFileSync(join(root, 'packages/orbit/package.json'), 'utf8')).version;

const cargoTomlPath = join(root, 'Cargo.toml');
let cargoToml = readFileSync(cargoTomlPath, 'utf8');
const current = cargoToml.match(/\[workspace\.package\][\s\S]*?\nversion = "([^"]+)"/)?.[1];

if (check) {
  if (current !== version) {
    console.error(`✗ version drift: packages/orbit = ${version}, Cargo.toml [workspace.package] = ${current}`);
    console.error('  run `node scripts/sync-versions.mjs` (or `pnpm version-packages`).');
    process.exit(1);
  }
  console.log(`✓ versions aligned at ${version}`);
  process.exit(0);
}

// 1. Cargo.toml [workspace.package].version
cargoToml = cargoToml.replace(/(\[workspace\.package\][\s\S]*?\nversion = ")[^"]+(")/, `$1${version}$2`);
writeFileSync(cargoTomlPath, cargoToml);

// 2. Cargo.lock entries for the workspace crates
const cargoLockPath = join(root, 'Cargo.lock');
let cargoLock = readFileSync(cargoLockPath, 'utf8');
for (const name of CRATES) {
  cargoLock = cargoLock.replace(new RegExp(`(name = "${name}"\\nversion = ")[^"]+(")`), `$1${version}$2`);
}
writeFileSync(cargoLockPath, cargoLock);

console.log(`✓ synced Rust workspace (Cargo.toml + Cargo.lock) to ${version}`);

// 3. Keep apps/demo pinned to the LAST-PUBLISHED @zeronsh/orbit. It consumes the
// package from the npm registry (not the workspace link) so Railway can build it
// without compiling from source. `changeset version` bumps this dependent to the
// NEW, not-yet-published version — which would make `pnpm install --frozen-lockfile`
// (CI, the release job, and the Railway demo build) fail with NO_MATCHING_VERSION
// until the release publishes: a deadlock. The lockfile still resolves the last
// published version, so pin the manifest back to match it. Bump the demo
// deliberately (a normal commit + lockfile refresh) once a release is live.
const lock = readFileSync(join(root, 'pnpm-lock.yaml'), 'utf8');
const lockedSpec = lock.match(/\n {2}apps\/demo:[\s\S]*?'@zeronsh\/orbit':\s*\n\s*specifier:\s*(\S+)/)?.[1];
const demoPkgPath = join(root, 'apps/demo/package.json');
const demoPkg = readFileSync(demoPkgPath, 'utf8');
const fixedDemo = lockedSpec
  ? demoPkg.replace(/("@zeronsh\/orbit":\s*")[^"]+(")/, `$1${lockedSpec}$2`)
  : demoPkg;
if (fixedDemo !== demoPkg) {
  writeFileSync(demoPkgPath, fixedDemo);
  console.log(`✓ kept apps/demo on @zeronsh/orbit ${lockedSpec} (last published; avoids the release deadlock)`);
}
