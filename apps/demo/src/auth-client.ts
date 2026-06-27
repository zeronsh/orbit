// better-auth browser client. Stores the session token from the `set-auth-token`
// response header in localStorage and sends it as a Bearer token on every request
// — including (via getOrbit) to the Orbit sync server, which forwards it to the
// app's /api endpoints where the session is verified.
//
// The `anonymousClient` plugin adds `signIn.anonymous()` — the only sign-in this
// app uses.

import { createAuthClient } from 'better-auth/react';
import { anonymousClient } from 'better-auth/client/plugins';

const TOKEN_KEY = 'orbit_bearer';

export function bearerToken(): string {
  return typeof localStorage !== 'undefined' ? localStorage.getItem(TOKEN_KEY) ?? '' : '';
}

export const authClient = createAuthClient({
  baseURL: import.meta.env.VITE_AUTH_URL ?? undefined, // same-origin by default
  plugins: [anonymousClient()],
  fetchOptions: {
    auth: { type: 'Bearer', token: () => bearerToken() },
    onSuccess: (ctx) => {
      const token = ctx.response.headers.get('set-auth-token');
      if (token) localStorage.setItem(TOKEN_KEY, token);
    },
  },
});

export function clearBearer() {
  if (typeof localStorage !== 'undefined') localStorage.removeItem(TOKEN_KEY);
}

export const { useSession, signIn, signOut } = authClient;
