// Config for `pnpm generate:schema` (the orbit-drizzle CLI). Regenerates
// src/schema.gen.ts from the Drizzle schema in db/schema.ts.
import type { GenerateOptions } from '@zeronsh/orbit/drizzle/cli';

const config: Partial<GenerateOptions> = {
  schemaPath: './db/schema.ts',
  outputPath: './src/schema.gen.ts',
};

export default config;
