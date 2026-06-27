import { createFileRoute } from '@tanstack/react-router';
import { useSession } from '../auth-client.ts';
import { Canvas } from '../canvas.tsx';

// `/` — the shared pixel canvas.
export const Route = createFileRoute('/_app/')({ component: Index });

function Index() {
  const { data: session } = useSession();
  if (!session?.user) return null;
  return <Canvas me={{ id: session.user.id }} />;
}
