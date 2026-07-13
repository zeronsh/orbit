# syntax=docker/dockerfile:1
# Orbit server image — builds the `orbit-server` (single-node) and `orbit-node`
# (cluster: replicator | view-syncer) binaries into a slim runtime image.
# Published as ghcr.io/zeronsh/orbit-server. Build context = repo root:
#   docker build -f deploy/Dockerfile -t orbit-server .

FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# `s3` feature enables object-store snapshots for the cluster `orbit-node` role.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p orbit-cache --features s3 --bin orbit-server --bin orbit-node \
    && cp target/release/orbit-server target/release/orbit-node /usr/local/bin/

FROM debian:bookworm-slim
# curl: container healthchecks poll the metrics/readiness endpoint.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/orbit-server /usr/local/bin/orbit-server
COPY --from=build /usr/local/bin/orbit-node /usr/local/bin/orbit-node

# Bind to all interfaces inside the container (the bin defaults to 127.0.0.1).
ENV ORBIT_LISTEN=0.0.0.0:4848
# 4848 = WebSocket sync; 9090 = metrics/health (GET /metrics /ready /live).
EXPOSE 4848 9090

# Default to the single-node server. Override with `orbit-node` for cluster roles.
CMD ["orbit-server"]
