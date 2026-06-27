import { defineConfig } from 'tsup';

// One package, many entry points. `splitting` pulls shared code (e.g. the client
// imported by react/drizzle) into shared chunks so each subpath import stays
// tree-shakeable. Peer deps (react, drizzle-orm, pg, ts-morph, prettier) and the
// `commander` dependency are auto-externalized by tsup from package.json.
export default defineConfig({
  entry: {
    client: 'client/src/index.ts',
    react: 'react/src/index.ts',
    server: 'server/src/index.ts',
    'server/pg': 'server/src/pg.ts',
    'orm-core': 'orm-core/src/index.ts',
    drizzle: 'drizzle/src/index.ts',
    'drizzle/cli': 'drizzle/src/cli/index.ts',
    'drizzle/cli/bin': 'drizzle/src/cli/bin.ts',
  },
  format: ['esm'],
  target: 'es2022',
  dts: true,
  splitting: true,
  treeshake: true,
  clean: true,
  sourcemap: true,
});
