# Orbit Issues app (TanStack Start) for Railway. Single stage: the SSR build keeps
# its deps external, so the runtime needs node_modules. VITE_ORBIT_SERVER is baked
# into the client bundle at build time.
FROM node:24-bookworm-slim
WORKDIR /repo
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile
ARG VITE_ORBIT_SERVER
ENV VITE_ORBIT_SERVER=$VITE_ORBIT_SERVER
RUN pnpm --filter @zeronsh/orbit-demo build
WORKDIR /repo/apps/demo
ENV PORT=3000
EXPOSE 3000
CMD ["node", "server-prod.mjs"]
