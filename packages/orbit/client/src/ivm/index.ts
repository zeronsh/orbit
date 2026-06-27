// Incremental view-maintenance engine (operator graph). A TypeScript port of the
// Rust `oql` IVM, used by the client's reactive views.

export { buildPipeline, type SourceProvider } from './build.ts';
export { MaterializedView } from './view.ts';
export { MemorySource, MemorySourceProvider, SourceConnection } from './source.ts';
export { StoreProvider, tablesOf, nodeToRow } from './store-provider.ts';
export type { Node, Change, SourceChange, Op, FetchRequest, Comparator } from './data.ts';
