#!/bin/bash
# Allow logical-replication connections (Orbit connects with replication=database).
# POSTGRES_HOST_AUTH_METHOD=trust covers normal connections but not the separate
# `replication` pg_hba category, so add it explicitly. `all` = any source address,
# which includes the Docker bridge gateway the host connects through.
set -e
{
  echo "host all all all trust"
  echo "host replication all all trust"
} >> "$PGDATA/pg_hba.conf"
