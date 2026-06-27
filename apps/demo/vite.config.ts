import { defineConfig } from 'vite';
import { tanstackStart } from '@tanstack/react-start/plugin/vite';
import viteReact from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';

// Resolve the Orbit packages straight from their TypeScript source — no library
// build step. Vite transpiles them on the fly (client + SSR). The app's API
// routes live in src/routes/api/ (push.ts, query.ts).
export default defineConfig({
  plugins: [
    tanstackStart(), // must come before react()
    viteReact(),
  ],
  resolve: {
    alias: [
      // Resolve @zeronsh/orbit subpaths to source (most-specific first).
      { find: '@zeronsh/orbit/server/pg', replacement: fileURLToPath(new URL('../../packages/orbit/server/src/pg.ts', import.meta.url)) },
      { find: '@zeronsh/orbit/server', replacement: fileURLToPath(new URL('../../packages/orbit/server/src/index.ts', import.meta.url)) },
      { find: '@zeronsh/orbit/client', replacement: fileURLToPath(new URL('../../packages/orbit/client/src/index.ts', import.meta.url)) },
      { find: '@zeronsh/orbit/react', replacement: fileURLToPath(new URL('../../packages/orbit/react/src/index.ts', import.meta.url)) },
    ],
  },
});
