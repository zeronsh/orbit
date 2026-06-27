// Server-side setup for the API routes: pick a database adapter, derive the
// auth context, and build the processors. Swapping Postgres for another backend
// is a one-line change here (replace `nodePg(...)` with another `DBConnection`).

import { PushProcessor, QueryProcessor } from '@zeronsh/orbit/server';
import { nodePg } from '@zeronsh/orbit/server/pg';
import { schema, type Ctx } from './shared.ts';
import { auth } from './auth.ts';

const connection = nodePg(
  process.env.DATABASE_URL ?? {
    host: process.env.PGHOST ?? '127.0.0.1',
    port: Number(process.env.PGPORT ?? 5433),
    user: process.env.PGUSER ?? 'orbit',
    database: process.env.PGDATABASE ?? 'orbit',
  },
);

// Verify the forwarded session and resolve the acting user. Orbit forwards the
// client's bearer token, which better-auth's `bearer` plugin reads here.
async function context(request: Request): Promise<Ctx | null> {
  const session = await auth.api.getSession({ headers: request.headers });
  return session?.user ? { userID: session.user.id } : null;
}

export const pushProcessor = new PushProcessor({ connection, schema, context });
export const queryProcessor = new QueryProcessor({ context });
