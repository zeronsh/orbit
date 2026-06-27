#!/usr/bin/env node
// `orbit-drizzle` CLI. Generates an Orbit schema (`orbit-schema.gen.ts`) from a
// Drizzle schema, preserving custom `$type<>()` and enum types via the TypeScript
// compiler. The published bin is plain JS; in-repo, run the source via
// `node --experimental-strip-types drizzle/src/cli/bin.ts`.

import * as fs from 'node:fs';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import { Command } from 'commander';
import { generate, type GenerateOptions } from './generate.ts';

const program = new Command();

program
  .name('orbit-drizzle')
  .description('Generate an Orbit schema from a Drizzle ORM schema.')
  .argument('[schema]', 'path to the Drizzle schema module (overrides config/--schema)')
  .option('-s, --schema <path>', 'path to the Drizzle schema module')
  .option('-o, --output <path>', 'output file (default ./orbit-schema.gen.ts)')
  .option('-t, --tsconfig <path>', 'tsconfig for type resolution')
  .option('-c, --config <path>', 'orbit-drizzle config file', './orbit-drizzle.config.ts')
  .option('-n, --schema-name <name>', 'exported schema const name (default schema)')
  .option('--import-from <module>', 'module to import Orbit helpers from (default @orbit/client)')
  .option('-f, --format', 'format output with Prettier')
  .option('-j, --js-extension', 'append .js to relative imports (Node16/NodeNext ESM)')
  .option('--debug', 'verbose logging')
  .action(async (schemaArg: string | undefined, opts: Record<string, unknown>) => {
    try {
      const fromFile = await loadConfig(opts.config as string);
      const schemaPath = schemaArg ?? (opts.schema as string | undefined) ?? fromFile.schemaPath;
      if (!schemaPath) {
        console.error('❌ orbit-drizzle: no schema given. Pass a path, use --schema, or set `schemaPath` in the config.');
        process.exit(1);
      }
      // CLI flags win over config; both fall back to generate()'s own defaults.
      const options: GenerateOptions = {
        schemaPath,
        outputPath: (opts.output as string) ?? fromFile.outputPath,
        tsConfigPath: (opts.tsconfig as string) ?? fromFile.tsConfigPath,
        schemaName: (opts.schemaName as string) ?? fromFile.schemaName,
        importFrom: (opts.importFrom as string) ?? fromFile.importFrom,
        format: (opts.format as boolean | undefined) ?? fromFile.format,
        jsExtension: (opts.jsExtension as boolean | undefined) ?? fromFile.jsExtension,
        tables: fromFile.tables,
        debug: (opts.debug as boolean | undefined) ?? fromFile.debug,
      };
      const { outputPath } = await generate(options);
      console.log(`✅ orbit-drizzle: wrote ${path.relative(process.cwd(), outputPath)}`);
    } catch (err) {
      console.error(`❌ orbit-drizzle: ${(err as Error).message}`);
      if (opts.debug) console.error(err);
      process.exit(1);
    }
  });

async function loadConfig(configPath: string | undefined): Promise<Partial<GenerateOptions>> {
  if (!configPath) return {};
  const abs = path.resolve(configPath);
  if (!fs.existsSync(abs)) return {};
  const mod = (await import(pathToFileURL(abs).href)) as { default?: Partial<GenerateOptions> };
  return mod.default ?? {};
}

program.parseAsync(process.argv).catch((err) => {
  console.error(err);
  process.exit(1);
});
