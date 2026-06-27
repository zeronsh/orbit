// React bindings for Orbit, mirroring Zero's `@rocicorp/zero/react` `useQuery`.

import { useEffect, useRef, useState } from 'react';
import type { Row, Subscribable, ViewLike } from '../../client/src/index.ts';

/**
 * Subscribe to a query and re-render on change. Mirrors `useQuery` from
 * `@rocicorp/zero/react`. Accepts a typed query (`orbit.query.todo`) or a named
 * query (`orbit.queryNamed(...)`); the result is typed either way.
 *
 * ```tsx
 * const todos = orbit.query.todo.orderBy('created', 'asc');
 * const rows = useQuery(todos); // rows: Todo[]
 * ```
 */
export function useQuery<T extends Row>(query: Subscribable<T>): T[] {
  // Track the live view together with the query it belongs to, so a *changed*
  // query (e.g. a route param flipping `issue({id})` to a new id without
  // remounting the component) re-materializes instead of showing the stale view.
  // The caller is expected to keep `query` referentially stable across renders
  // that don't change it (e.g. `useMemo`), as is conventional for query hooks.
  const ref = useRef<{ q: Subscribable<T>; view: ViewLike<T> } | null>(null);
  const [, force] = useState(0);

  // Materialize synchronously on first render or when the query changes, so this
  // render already reflects the current query.
  if (ref.current === null || ref.current.q !== query) {
    ref.current = { q: query, view: query.materialize() };
  }

  useEffect(() => {
    // The view may have been released by a prior cleanup (React Strict Mode's
    // double-invoke, or a query change between render and commit); ensure a live
    // view for THIS query.
    if (ref.current === null || ref.current.q !== query) {
      ref.current = { q: query, view: query.materialize() };
    }
    const entry = ref.current;
    const unsub = entry.view.subscribe(() => force((n) => n + 1));
    return () => {
      unsub();
      entry.view.destroy?.(); // releases the query subscription → enables TTL/GC
      // Only clear the shared ref if it still points at the view we just tore
      // down — a query change already replaced it with the next view.
      if (ref.current === entry) ref.current = null;
    };
  }, [query]);

  return ref.current!.view.data;
}
