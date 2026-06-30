---
"@zeronsh/orbit": minor
---

Redesign custom query/mutator authoring around `{ args, handler }` + a bound factory.

- `defineQuery`/`defineMutation` now take `{ args?, handler }`. `args` is any Standard Schema validator (Zod/Valibot/ArkType); its output type is inferred for `args`, and the server validates client input against it at runtime.
- New `createOrbitApi<typeof schema, Ctx>({ schema })` returns `{ defineQuery, defineMutation, builder }` whose handlers have fully-typed `tx`/`args`/`ctx` with no per-def annotations.
- The Orbit client now accepts a typed `context` (a value or `() => Ctx`) so optimistic mutations and local query derivation run with the real ctx. The server still derives ctx authoritatively from the auth token; ctx is never sent over the wire.

BREAKING: `defineMutator` (a bare function) is replaced by `defineMutation({ args, handler })`, and `defineQuery` no longer accepts a bare function. The `MutatorDef`/`MutatorDefs` types are renamed to `MutationDef`/`MutationDefs`.
