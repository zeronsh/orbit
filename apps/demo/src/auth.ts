// better-auth server config. Used by:
//   • the catch-all route src/routes/api/auth.$.ts (auth.handler)
//   • src/orbit-server.ts (auth.api.getSession → ctx.userID)
//   • the migration CLI (`pnpm auth:migrate`) to create its tables.
//
// Auth is fully anonymous: the browser calls `signIn.anonymous()` and gets a
// throwaway user — no email, no password, no sign-in screen. The `bearer` plugin
// lets the browser authenticate with a token (the Orbit WebSocket server lives on
// a different origin, and cookies don't cross origins, but a forwarded
// `Authorization: Bearer …` does).

import { betterAuth } from 'better-auth';
import { bearer } from 'better-auth/plugins';
import { anonymous } from 'better-auth/plugins/anonymous';
import { Pool } from 'pg';

const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: process.env.PGHOST ?? '127.0.0.1',
        port: Number(process.env.PGPORT ?? 5433),
        user: process.env.PGUSER ?? 'orbit',
        database: process.env.PGDATABASE ?? 'orbit',
      },
);

export const authOptions = {
  database: pool,
  secret: process.env.BETTER_AUTH_SECRET ?? 'orbit-dev-secret-change-me',
  baseURL: process.env.BETTER_AUTH_URL ?? 'http://127.0.0.1:5173',
  trustedOrigins: (process.env.BETTER_AUTH_TRUSTED ?? 'http://127.0.0.1:5173').split(','),
  plugins: [bearer(), anonymous()],
};

export const auth = betterAuth(authOptions);
