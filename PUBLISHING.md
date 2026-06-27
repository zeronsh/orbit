# Releasing

CI runs on **Blacksmith** runners (`runs-on: blacksmith`). One `release` workflow
publishes **everything** — the npm package and the GHCR server image.

## npm — `@zeronsh/orbit`

Versioning + publishing is automated with [changesets](https://github.com/changesets/changesets)
and the [`release`](./.github/workflows/release.yml) workflow.

**Day to day:** add a changeset with every user-facing change.

```bash
pnpm changeset           # pick a bump (patch/minor/major) + write a summary
git add .changeset && git commit -m "..."
```

**Automated release:** on push to `main`, the workflow opens a *"Version Packages"*
PR that consumes the pending changesets and bumps the version. Merging that PR
publishes `@zeronsh/orbit` to npm.

**Required secret:** add an npm automation token as `NPM_TOKEN` in the repo
(Settings → Secrets and variables → Actions). The token's npm account must own /
be a member of the `@zeronsh` org with publish rights. The package is published with
`access: public` (set in `.changeset/config.json`).

**Manual publish** (if you'd rather not use the workflow):

```bash
pnpm install
pnpm --filter @zeronsh/orbit build
pnpm changeset version          # applies changesets → bumps version + changelog
npm whoami                      # ensure you're logged in (npm login) with @zeronsh access
pnpm release                    # = changeset publish (runs the build via prepublishOnly)
git push --follow-tags
```

> `dist/` is built by `tsup` (see `packages/orbit/tsup.config.ts`) and is created by
> `prepublishOnly`, so it's never committed. The published files are `dist/` + `README.md`.

## Docker — `ghcr.io/zeronsh/orbit-server`

The `docker` job in the [`release`](./.github/workflows/release.yml) workflow builds +
pushes the Rust server image (using `useblacksmith/build-push-action` for fast,
cached builds) on **every push to `main`** — tagged `latest` + the commit `sha`, plus
`v<version>` whenever a release was just published. It authenticates with the built-in
`GITHUB_TOKEN` (no extra secret); ensure **Settings → Actions → General → Workflow
permissions → Read and write**. Trigger a manual image rebuild with *Run workflow*
(`workflow_dispatch`).

Build/run locally:

```bash
docker build -f deploy/Dockerfile -t orbit-server .
ORBIT_TABLES=issue:id,comment:id docker compose -f deploy/docker-compose.yml up
```

See [`deploy/README.md`](./deploy/README.md) for environment variables + hosting.
