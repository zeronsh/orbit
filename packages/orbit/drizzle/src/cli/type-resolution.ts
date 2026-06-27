// Resolve each column's *TypeScript* type with the compiler (via ts-morph), so a
// Drizzle `.$type<...>()` and enum unions survive into the generated schema —
// information that does not exist at runtime. For column `c` of table exported as
// `user`, we resolve `InferSelectModel<typeof user>['c']` (the canonical Drizzle
// row-type accessor), strip nullability (Orbit models it with `optional()`), and
// keep the result as the column's custom type. Named types it references (e.g.
// `PostMeta`, or `import("…/country").Country`) are rewritten to bare names and
// collected as `import type` lines for the generated file.

import * as path from 'node:path';

export interface ResolveColumn {
  readonly jsKey: string;
  readonly dbName: string;
  readonly baseType: 'string' | 'number' | 'boolean' | 'json';
}

export interface ResolveTable {
  /** The export name of the table in the schema module (e.g. `user`). */
  readonly exportName: string;
  /** The Orbit/database table name. */
  readonly tableName: string;
  readonly columns: readonly ResolveColumn[];
}

export interface ResolveOptions {
  /** Absolute path to the user's Drizzle schema module. */
  readonly schemaModulePath: string;
  /** Absolute path of the file being generated (for relative import specifiers). */
  readonly outputFilePath: string;
  /** Optional tsconfig for module resolution. */
  readonly tsConfigFilePath?: string;
  /** Append `.js` to emitted relative import specifiers (Node16/NodeNext). */
  readonly jsExtension?: boolean;
}

export interface ResolvedTypes {
  /** `tableName → dbColumnName → TS type expression`. */
  readonly customTypes: Record<string, Record<string, string>>;
  /** `import type` lines the generated file needs for named custom types. */
  readonly typeImports: { module: string; names: string[] }[];
}

const TRIVIAL = new Set(['string', 'number', 'boolean', 'unknown', 'any', 'never', '{}', 'object']);
const NS = '__orbitDrizzleSchema';

function stripNull(text: string): string {
  return text
    .replace(/\s*\|\s*null\b/g, '')
    .replace(/\bnull\s*\|\s*/g, '')
    .replace(/\s*\|\s*undefined\b/g, '')
    .trim();
}

/** Module specifier from `from` (the generated file) to `to` (a TS module). */
function relSpecifier(fromFile: string, toModule: string, jsExtension: boolean): string {
  let rel = path.relative(path.dirname(fromFile), toModule).replace(/\\/g, '/');
  if (!rel.startsWith('.')) rel = `./${rel}`;
  rel = rel.replace(/\.tsx?$/, '');
  return jsExtension ? `${rel}.js` : rel;
}

export async function resolveCustomTypes(tables: readonly ResolveTable[], options: ResolveOptions): Promise<ResolvedTypes> {
  // ts-morph is an optional peer dependency; import lazily so the runtime path
  // never needs it.
  const { Project, ts } = await import('ts-morph');

  const project = options.tsConfigFilePath
    ? new Project({ tsConfigFilePath: options.tsConfigFilePath, skipAddingFilesFromTsConfig: true })
    : new Project({
        compilerOptions: { strict: true, moduleResolution: ts.ModuleResolutionKind.Bundler, target: ts.ScriptTarget.ES2022 },
        skipAddingFilesFromTsConfig: true,
      });

  project.addSourceFileAtPath(options.schemaModulePath);

  const schemaDir = path.dirname(options.schemaModulePath);
  const schemaSpecifier = `./${path.basename(options.schemaModulePath).replace(/\.tsx?$/, '')}`;

  // One type alias per column, plus an index so we can map back.
  const aliases: { name: string; tableName: string; dbName: string; baseType: ResolveColumn['baseType'] }[] = [];
  let body = `import type { InferSelectModel } from 'drizzle-orm';\nimport type * as ${NS} from ${JSON.stringify(schemaSpecifier)};\n`;
  let i = 0;
  for (const t of tables) {
    for (const c of t.columns) {
      const name = `C_${i++}`;
      body += `type ${name} = InferSelectModel<typeof ${NS}.${t.exportName}>[${JSON.stringify(c.jsKey)}];\n`;
      aliases.push({ name, tableName: t.tableName, dbName: c.dbName, baseType: c.baseType });
    }
  }

  const probe = project.createSourceFile(path.join(schemaDir, '__orbit_drizzle_probe__.ts'), body, { overwrite: true });
  const flags = ts.TypeFormatFlags.NoTruncation;

  const customTypes: Record<string, Record<string, string>> = {};
  // module specifier → set of imported names
  const importsByModule = new Map<string, Set<string>>();
  const addImport = (moduleSpecifier: string, name: string) => {
    const set = importsByModule.get(moduleSpecifier) ?? new Set<string>();
    set.add(name);
    importsByModule.set(moduleSpecifier, set);
  };

  for (const a of aliases) {
    const alias = probe.getTypeAliasOrThrow(a.name);
    const fullType = alias.getType();
    const constituents = fullType.isUnion() ? fullType.getUnionTypes() : [fullType];
    const nonNull = constituents.filter((c) => !c.isNull() && !c.isUndefined());

    // Keep a custom type only when it's a genuine *subtype* of the column's Orbit
    // base type. Otherwise the base type is the right one (e.g. a `timestamp`
    // maps to `number`, even though its Drizzle TS type is `Date`).
    const compatible =
      a.baseType === 'json'
        ? true
        : nonNull.length > 0 &&
          nonNull.every((c) => {
            if (a.baseType === 'string') return c.isString() || c.isStringLiteral() || c.isTemplateLiteral();
            if (a.baseType === 'number') return c.isNumber() || c.isNumberLiteral();
            return c.isBoolean() || c.isBooleanLiteral();
          });
    if (!compatible) continue;

    let text = stripNull(fullType.getText(alias, flags));

    // Rewrite `import("ABS").Name` → `Name`, collecting an import from ABS.
    text = text.replace(/import\("([^"]+)"\)\.(\w+)/g, (_m, abs: string, name: string) => {
      const spec = relSpecifier(options.outputFilePath, abs, Boolean(options.jsExtension));
      addImport(spec, name);
      return name;
    });

    // Rewrite `__orbitDrizzleSchema.Name` → `Name`, importing from the schema module.
    const schemaImportSpec = relSpecifier(options.outputFilePath, options.schemaModulePath, Boolean(options.jsExtension));
    text = text.replace(new RegExp(`${NS}\\.(\\w+)`, 'g'), (_m, name: string) => {
      addImport(schemaImportSpec, name);
      return name;
    });

    // Drop trivial types; for json keep only meaningful object/array/named types.
    if (TRIVIAL.has(text) || text === '') continue;
    if (a.baseType === 'json' && (text === 'unknown' || text === 'any')) continue;

    (customTypes[a.tableName] ??= {})[a.dbName] = text;
  }

  project.removeSourceFile(probe);

  const typeImports = [...importsByModule.entries()].map(([module, names]) => ({ module, names: [...names].sort() }));
  return { customTypes, typeImports };
}
