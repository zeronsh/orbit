import { createFileRoute } from '@tanstack/react-router';
import { queries } from '../../shared.ts';
import { queryProcessor } from '../../orbit-server.ts';

// orbit-cache forwards named-query subscriptions here to be resolved/authorized.
export const Route = createFileRoute('/api/query')({
  server: {
    handlers: {
      POST: ({ request }) => queryProcessor.process(queries, request),
    },
  },
});
