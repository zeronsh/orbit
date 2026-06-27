import { createFileRoute } from '@tanstack/react-router';
import { mutators } from '../../shared.ts';
import { pushProcessor } from '../../orbit-server.ts';

// orbit-cache forwards custom mutations here (with the client's Bearer token).
export const Route = createFileRoute('/api/push')({
  server: {
    handlers: {
      POST: ({ request }) => pushProcessor.process(mutators, request),
    },
  },
});
