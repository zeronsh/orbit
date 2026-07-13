#!/usr/bin/env bash
# 512 MB acceptance test — proves the Orbit cluster stays inside 512 MB under:
#   • a ~200 MB compressible dataset (half pre-boot, half live-replicated)
#   • individual 10 MB rows
#   • large full-history hydrations from multiple clients
#   • a stalled durable change-log (LOCK TABLE on the source PG)
#   • mid-hydration disconnects + same-client CVR reconnects
# Asserts: no OOM kills / restarts, readiness transitions (503→200, and again
# after a view-syncer restart), peak RSS < RSS_LIMIT, exact row counts.
#
# Usage: scripts/acceptance/run.sh   (Docker required; ~10 minutes)
# Artifacts: metrics poll log in $OUT_DIR (default ./acceptance-out).

set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE=(docker compose -f deploy/acceptance/docker-compose.yml)
PG() { "${COMPOSE[@]}" exec -T pg-source psql -U orbit -d orbit -v ON_ERROR_STOP=1 "$@"; }
OUT_DIR="${OUT_DIR:-./acceptance-out}"
RSS_LIMIT="${RSS_LIMIT:-503316480}" # 480 MB — headroom under the 512 MB cgroup
STALL_SECS="${STALL_SECS:-60}"
mkdir -p "$OUT_DIR"
: >"$OUT_DIR/metrics-poll.log"

SERVICES=(replicator view-syncer-1 view-syncer-2)
metrics_port() { # portable (macOS bash 3.2 has no associative arrays)
  case "$1" in
    replicator) echo 25461 ;;
    view-syncer-1) echo 25463 ;;
    view-syncer-2) echo 25465 ;;
    *) echo "unknown service $1" >&2; exit 2 ;;
  esac
}

fail() { echo "ACCEPTANCE FAILED: $*" >&2; collect_logs; exit 1; }
collect_logs() {
  "${COMPOSE[@]}" logs --no-color >"$OUT_DIR/compose.log" 2>&1 || true
  # Container states (OOMKilled / ExitCode / restarts) for post-mortems —
  # captured BEFORE `down` removes the containers.
  for s in "${SERVICES[@]}"; do
    local cid
    cid=$("${COMPOSE[@]}" ps -aq "$s" 2>/dev/null | head -1)
    [ -n "$cid" ] && docker inspect --format '{{json .State}}' "$cid" >"$OUT_DIR/state-$s.json" 2>/dev/null
  done
}

assert_alive() { # every orbit service must be running, never OOM-killed/restarted
  for s in "${SERVICES[@]}"; do
    local cid oom restarts running
    cid=$("${COMPOSE[@]}" ps -aq "$s" | head -1)
    [ -n "$cid" ] || fail "$s has no container"
    read -r running oom restarts < <(docker inspect -f '{{.State.Running}} {{.State.OOMKilled}} {{.RestartCount}}' "$cid")
    [ "$oom" = "false" ] || fail "$s was OOM-killed"
    [ "$running" = "true" ] || fail "$s is not running (exited under pressure — see $OUT_DIR/state-$s.json)"
    [ "$restarts" = "0" ] || fail "$s restarted $restarts times (crashed under pressure)"
  done
  echo "== all orbit services alive (no OOM, no restarts)"
}
cleanup() { "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap 'collect_logs; cleanup' EXIT

ready_status() { # $1 = service → HTTP status of /ready (000 when unreachable)
  curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$(metrics_port "$1")/ready" || true
}

wait_ready() { # $1 = service, $2 = timeout secs (fails the run on timeout)
  wait_ready_soft "$1" "$2" || fail "$1 never became ready"
}

wait_ready_soft() { # like wait_ready but returns 1 on timeout
  local deadline=$((SECONDS + $2))
  while [ "$(ready_status "$1")" != "200" ]; do
    [ $SECONDS -lt "$deadline" ] || return 1
    sleep 2
  done
  echo "== $1 ready"
}

metric() { # $1 = service, $2 = metric name → value (0 when absent)
  curl -s --max-time 2 "http://127.0.0.1:$(metrics_port "$1")/metrics" \
    | awk -v m="$2" '$1 ~ "^"m"{" { print $2; found=1 } END { if (!found) print 0 }'
}

echo "== build image"
"${COMPOSE[@]}" build --quiet replicator

echo "== start postgres + object store"
"${COMPOSE[@]}" up -d pg-source minio minio-init
until PG -c "select 1" >/dev/null 2>&1; do sleep 1; done

echo "== seed phase 1 (pre-boot: ~100 MB + two 10 MB rows)"
PG <deploy/acceptance/seed-pre.sql >/dev/null

echo "== start orbit cluster (512 MB limits)"
"${COMPOSE[@]}" up -d replicator view-syncer-1 view-syncer-2

# Readiness must transition 503 (or unreachable) → 200 on every node.
wait_ready replicator 300
wait_ready view-syncer-1 300
wait_ready view-syncer-2 300

# Background metrics poller: RSS ceiling + queue observability, every 5 s.
poll_metrics() {
  while true; do
    for s in "${SERVICES[@]}"; do
      rss=$(metric "$s" orbit_process_rss_bytes)
      qd=$(metric "$s" orbit_changelog_queue_bytes)
      ring=$(metric "$s" orbit_change_ring_bytes)
      echo "$(date +%s) $s rss=$rss changelog_queue_bytes=$qd ring_bytes=$ring" >>"$OUT_DIR/metrics-poll.log"
    done
    sleep 5
  done
}
poll_metrics & POLLER=$!
trap 'kill $POLLER 2>/dev/null || true; collect_logs; cleanup' EXIT

echo "== seed phase 2 (live replication: ~100 MB + three 10 MB rows)"
PG <deploy/acceptance/seed-post.sql >/dev/null

echo "== start loadgen (full-history hydrations + churn) in background"
"${COMPOSE[@]}" --profile load up -d loadgen

echo "== stall the durable change-log for ${STALL_SECS}s (LOCK TABLE on pg-source)"
# Blocks the change-log writer's INSERTs while WAL keeps flowing: the bounded
# queue must fill and PLATEAU at its byte budget (backpressure), never OOM.
PG -c "BEGIN; LOCK TABLE orbit_change_log_orbit_slot IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(${STALL_SECS}); COMMIT;" &
STALL=$!
sleep 2
# Write pressure DURING the stall: ~1 MB of replicated events per iteration
# (50 × ~10 KB rows, REPLICA IDENTITY FULL carries old+new). Without this the
# stall window is quiet and the backpressure path never engages.
(
  i=0
  while kill -0 $STALL 2>/dev/null && [ $i -lt 200 ]; do
    PG -q -c "UPDATE acc_small SET body = body WHERE id IN (SELECT id FROM acc_small ORDER BY id LIMIT 50 OFFSET $(( (i * 50) % 9000 )))" >/dev/null 2>&1 || true
    i=$((i + 1))
    sleep 0.2
  done
) &
STALL_WRITER=$!
sleep $((STALL_SECS / 2))
Q_MID=$(metric replicator orbit_changelog_queue_bytes)
echo "== mid-stall changelog queue bytes: $Q_MID (bound: 33554432)"
[ "$Q_MID" -gt 1048576 ] || fail "changelog queue shows no pressure mid-stall ($Q_MID bytes) — stall writer not generating load?"
[ "$Q_MID" -le 33554432 ] || fail "changelog queue exceeded its 32 MiB byte budget mid-stall ($Q_MID)"
wait $STALL
wait $STALL_WRITER 2>/dev/null || true

echo "== stall released; wait for the queue to drain"
deadline=$((SECONDS + 120))
while :; do
  q=$(metric replicator orbit_changelog_queue_bytes)
  [ "$q" -lt 1048576 ] && break
  [ $SECONDS -lt $deadline ] || fail "changelog queue never drained after the stall (still $q bytes)"
  sleep 5
done

echo "== wait for loadgen verdict"
LOADGEN_CID=$("${COMPOSE[@]}" --profile load ps -aq loadgen)
LOADGEN_EXIT=$(docker wait "$LOADGEN_CID" 2>/dev/null || echo 1)
"${COMPOSE[@]}" --profile load logs --no-color loadgen >"$OUT_DIR/loadgen.log" 2>&1 || true
[ "${LOADGEN_EXIT:-1}" = "0" ] || fail "loadgen reported failures (exit $LOADGEN_EXIT; see $OUT_DIR/loadgen.log)"

# Liveness right after the load phase: a node dying mid-run can be masked by
# the loadgen's transient-error tolerance (the other node keeps serving).
assert_alive

echo "== replace view-syncer-1 (readiness must cycle; clients must reconverge)"
# Recreate rather than restart: a fresh container is the "machine replaced"
# case (fresh disk → snapshot re-download → catch-up → ready), matching how a
# real deploy rolls. In-place restart + local-file resume is covered by the
# multinode_sqlite integration tests. Runs AFTER the churn loadgen so Docker
# Desktop's embedded DNS (which drops under sustained network load) can't
# wedge the step on an infra quirk. One recreate retry for the same reason.
"${COMPOSE[@]}" up -d --force-recreate --no-deps view-syncer-1 >/dev/null 2>&1
if ! wait_ready_soft view-syncer-1 180; then
  echo "== view-syncer-1 not ready after 180s; recreating once more (Docker DNS flake guard)"
  "${COMPOSE[@]}" up -d --force-recreate --no-deps view-syncer-1 >/dev/null 2>&1
  wait_ready view-syncer-1 180
fi

echo "== reconvergence: short loadgen against the replaced node"
"${COMPOSE[@]}" --profile load run --rm --no-deps \
  -e LOADGEN_WS=172.28.0.21:4848 \
  -e LOADGEN_CLIENTS=2 -e LOADGEN_DURATION=45 -e LOADGEN_CHURN_PCT=25 \
  loadgen >"$OUT_DIR/loadgen-reconverge.log" 2>&1 \
  || fail "reconvergence loadgen failed (see $OUT_DIR/loadgen-reconverge.log)"

echo "== assert: no OOM kills, no restarts (final)"
assert_alive

echo "== assert: peak RSS < $RSS_LIMIT on every node"
for s in "${SERVICES[@]}"; do
  peak=$(awk -v s="$s" '$2 == s { gsub("rss=", "", $3); if ($3+0 > max) max = $3+0 } END { print max+0 }' "$OUT_DIR/metrics-poll.log")
  echo "   $s peak rss: $peak"
  [ "$peak" -gt 0 ] || fail "$s reported no RSS samples (metrics endpoint down?)"
  [ "$peak" -lt "$RSS_LIMIT" ] || fail "$s peak RSS $peak exceeded $RSS_LIMIT"
done

echo "== assert: exact row counts on the source"
COUNTS=$(PG -tA -c "select (select count(*) from acc_small) || ' ' || (select count(*) from acc_big)")
[ "$COUNTS" = "20000 5" ] || fail "source counts drifted: $COUNTS"

kill $POLLER 2>/dev/null || true
echo "ACCEPTANCE PASSED"
