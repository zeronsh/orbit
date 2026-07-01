---
"@zeronsh/orbit": patch
---

Server performance: incrementally-sorted join indexes and a leaner poke path.
No wire-format or API changes; verified against the full Zero-differential
harness (5,800 scenarios) with identical results.

- **Sorted secondary indexes.** A constrained fetch (the join-key lookup that
  runs on every join push) used to re-sort its bucket on every call, making a
  join key's push cost grow with its fan-in — 73× slower at 10k children per
  key. Buckets are now kept sorted incrementally (an upper-bound binary insert
  that reproduces the stable sort's ordering exactly, including ties), so the
  per-fetch sort is gone: join push is now flat through fan-in ~100 (1.03M/s,
  up from 713K/s) and 3.7–5.7× faster at fan-in 1,000–10,000. Unconstrained
  fetches are unchanged (no new per-push maintenance cost on tables that joins
  never constrain — filter/fanout workloads are unaffected).

- **Row puts share the IVM row (`Rc<Row>`)** instead of deep-cloning every row
  into every client's patch (`RowPatchOp::Put.value` is now `Rc<Row>`;
  serializes identically). With 200 clients, one change previously deep-cloned
  the same row 200 times just to serialize and discard it.

- **`RowRefs` (per-connection CVR refcounts) is keyed table → pk-values**
  instead of a flat `(String, Vec<Value>)` key, removing a table-name `String`
  allocation per row event.

- New `wire_bench` example decomposes the full per-client serving cost
  (IVM → patch build → JSON). Net effect on the end-to-end poke path:
  **~33% more throughput** (3.9M → 5.2M client-events/s per core); the patch
  build stage alone is ~37% cheaper.
