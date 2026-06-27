// Config for `orbit-drizzle generate`. Point the CLI at this with
// `orbit-drizzle generate -c example/orbit-drizzle.config.ts`, or pass flags.
import type { GenerateOptions } from '@zeronsh/orbit/drizzle/cli';

const config: Partial<GenerateOptions> = {
  schemaPath: './example/db/schema.ts',
  outputPath: './example/orbit-schema.gen.ts',
  format: false,
};

export default config;
