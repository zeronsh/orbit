import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createBuilder } from '../../client/src/index.ts';
import { generate } from '../src/cli/index.ts';

const schemaPath = path.join(import.meta.dirname, 'fixtures', 'schema.ts');
const outputPath = path.join(import.meta.dirname, '__cli_generated__.gen.ts');

test('generate() emits a typed Orbit schema preserving custom types + relationships', async () => {
  // In-repo the published '@zeronsh/orbit/client' specifier isn't resolvable at
  // runtime, so point the generated file at the client source for the import test.
  const { source } = await generate({ schemaPath, outputPath, importFrom: '../../client/src/index.ts' });
  try {
    // custom $type<>() template literal preserved
    assert.match(source, /email: string<`\$\{string\}@\$\{string\}`>\(\)/);
    // enum resolved to a string-literal union
    assert.match(source, /status: string<"active" \| "inactive" \| "archived">\(\)/);
    // jsonb().$type<PostMeta>() (nullable) → optional(json<PostMeta>()) + a type import
    assert.match(source, /meta: optional\(json<PostMeta>\(\)\)/);
    assert.match(source, /import type \{ PostMeta \} from "\.\/fixtures\/schema"/);
    // relationships block, incl. junction
    assert.match(source, /relationships\(post, \(\{ one, many \}\) => \(\{/);
    assert.match(source, /author: one\(\{ sourceField: \["author_id"\], destField: \["id"\], destSchema: user \}\)/);
    assert.match(source, /tags: many\(/);
    assert.match(source, /destSchema: post_tag/);
    // row-type exports
    assert.match(source, /export type Post = RowOf<typeof post>;/);

    // and it actually compiles + builds an equivalent schema
    const mod = (await import(pathToFileURL(outputPath).href)) as { schema: ReturnType<typeof createBuilder> extends never ? never : any };
    assert.deepEqual(Object.keys(mod.schema.tables).sort(), ['post', 'post_tag', 'tag', 'user']);
    const b = createBuilder(mod.schema as never);
    const tagsRel = (b as never as Record<string, { related(n: string): { ast(): { related?: { hidden?: boolean }[] } } }>).post.related('tags').ast().related![0];
    assert.equal(tagsRel.hidden, true);
  } finally {
    fs.rmSync(outputPath, { force: true });
  }
});
