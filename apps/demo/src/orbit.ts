import { Orbit, IDBKV } from '@zeronsh/orbit/client';
import { schema, mutators, queries } from './shared.ts';
import { bearerToken } from './auth-client.ts';

export type { Pixel, Cursor } from './shared.ts';

const IDB_NAME = 'orbit-pixels';

// Lazily create the client on the browser only (it opens a WebSocket). `auth`
// reads the current better-auth bearer token, which Orbit forwards to the app's
// /api/push and /api/query endpoints as `Authorization: Bearer …`. `userID` is the
// signed-in user; it becomes the client `context` so optimistic writes use the same
// id the server derives from the session (the server stays authoritative).
let client: Orbit<typeof schema, typeof mutators, typeof queries> | null = null;
export function getOrbit(userID: string) {
  if (!client) {
    client = new Orbit({
      server: import.meta.env.VITE_ORBIT_SERVER ?? 'ws://127.0.0.1:4848',
      schema,
      mutators,
      queries,
      context: () => ({ userID }),
      auth: () => bearerToken(),
      // Persist synced rows + pending mutations in IndexedDB so a reload hydrates
      // the canvas instantly from the local cache before the socket reconnects and
      // resumes the delta. Browser-only; SSR/tests fall back to in-memory.
      persist: typeof indexedDB !== 'undefined' ? new IDBKV(IDB_NAME) : undefined,
    });
  }
  return client;
}

// Tear down the client + clear the local cache (e.g. on sign-out).
export function resetOrbit() {
  client?.close();
  client = null;
  if (typeof indexedDB !== 'undefined') indexedDB.deleteDatabase(IDB_NAME);
}
