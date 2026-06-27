// Create/upgrade better-auth's tables (user, session, account, verification).
// Run: pnpm auth:migrate  (node --experimental-strip-types).
import { getMigrations } from 'better-auth/db/migration';
import { authOptions } from '../src/auth.ts';

const { runMigrations } = await getMigrations(authOptions as Parameters<typeof getMigrations>[0]);
await runMigrations();
console.log('✓ better-auth tables migrated');
process.exit(0);
