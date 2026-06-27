import { createFileRoute } from '@tanstack/react-router';
import { auth } from '../../auth.ts';

// Mount better-auth: it handles /api/auth/* (sign-in, sign-up, session, …).
const handler = ({ request }: { request: Request }) => auth.handler(request);

export const Route = createFileRoute('/api/auth/$')({
  server: {
    handlers: { GET: handler, POST: handler },
  },
});
