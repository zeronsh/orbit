import { createFileRoute, Outlet, ClientOnly } from '@tanstack/react-router';
import { useEffect, useRef } from 'react';
import { useSession, signIn } from '../auth-client.ts';

// The canvas talks to Orbit over a WebSocket and uses browser auth, so the whole
// tree renders client-side only. There's no sign-in screen: a first-time visitor
// is signed in anonymously and dropped straight onto the shared board.
export const Route = createFileRoute('/_app')({ component: AppLayout });

function AppLayout() {
  return (
    <ClientOnly fallback={<Splash label="Loading" />}>
      <Gate />
    </ClientOnly>
  );
}

function Gate() {
  const { data: session, isPending } = useSession();
  const tried = useRef(false);
  useEffect(() => {
    if (!isPending && !session?.user && !tried.current) {
      tried.current = true;
      void signIn.anonymous();
    }
  }, [isPending, session]);
  if (!session?.user) return <Splash label="Joining the canvas" />;
  return <Outlet />;
}

function Splash({ label }: { label: string }) {
  return (
    <div className="splash">
      <span className="splash-logo" aria-hidden />
      <span className="splash-label">{label}…</span>
    </div>
  );
}
