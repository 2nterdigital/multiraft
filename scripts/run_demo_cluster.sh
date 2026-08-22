#!/usr/bin/env bash
# Launch a 3-process MultiRaft demo over gRPC (`--mode node`).
#
# Each OS process is one Raft node. Admin HTTP per node:
#   node N → http://127.0.0.1:(BASE_PORT + 100 + N - 1)
# Raft gRPC:
#   node N → 127.0.0.1:(BASE_PORT + N - 1)
#
# Optional Standby (Learner):
#   STANDBY=1 → also start node 4 as --role standby (StandbyOffload),
#   then curl the leader to add_standby for group 0.
#
# Compatible with macOS Bash 3.2.
set -euo pipefail

STANDBY="${STANDBY:-0}"
DAISY="${DAISY:-0}"
if [[ "$DAISY" == "1" || "$STANDBY" == "2" ]]; then
  printf '%s\n' 'daisy/live standby restore is unsupported in this candidate' >&2
  exit 2
fi

find_lab_base_port() {
  python3 <<'PY'
import socket
for base in range(24000, 50000):
    sockets=[]
    try:
        for offset in range(4):
            for port in (base+offset, base+100+offset):
                s=socket.socket(); s.bind(("127.0.0.1", port)); sockets.append(s)
        print(base); break
    except OSError:
        pass
    finally:
        for s in sockets: s.close()
else: raise SystemExit("no contiguous lab port block")
PY
}

verify_lab_catalog_ad() {
  lab_root="$(mktemp -d /tmp/standby-containment-lab.XXXXXX)" || exit 1
  base_port="$(find_lab_base_port)" || exit 1
  cleanup_lab() {
    for pid_file in "$lab_root/data"/node-*.pid; do
      test -f "$pid_file" || continue; pid="$(cat "$pid_file")"
      kill "$pid" 2>/dev/null || :
    done
    pids_alive() {
      for pid_file in "$lab_root/data"/node-*.pid; do
        test -f "$pid_file" || continue
        kill -0 "$(cat "$pid_file")" 2>/dev/null && return 0
      done
      return 1
    }
    deadline=$((SECONDS + 15)); while pids_alive && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
    if pids_alive; then
      for pid_file in "$lab_root/data"/node-*.pid; do
        test -f "$pid_file" || continue; kill -KILL "$(cat "$pid_file")" 2>/dev/null || :
      done
      deadline=$((SECONDS + 15)); while pids_alive && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
    fi
    ! pids_alive
    rm -rf "$lab_root"
  }
  trap cleanup_lab EXIT INT TERM
  RUST_LOG=info
  export RUST_LOG
  STANDBY=1 BASE_PORT="$base_port" GROUPS=1 NODES=3 DATA_DIR="$lab_root/data" "$0"
  deadline=$((SECONDS + 45)); role_modes_ready=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    voter_id=1; role_modes_ready=1
    while [ "$voter_id" -le 3 ]; do
      rg -F 'snapshot_mode=Disabled' "$lab_root/data/node-$voter_id.log" >/dev/null 2>&1 || role_modes_ready=0
      voter_id=$((voter_id + 1))
    done
    rg -F 'snapshot_mode=StandbyOffload' "$lab_root/data/node-4.log" >/dev/null 2>&1 || role_modes_ready=0
    test "$role_modes_ready" = 1 && break
    sleep 1
  done
  test "$role_modes_ready" = 1
  voter=""; voter_admin=""; deadline=$((SECONDS + 45)); triggered=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    candidate=1
    while [ "$candidate" -le 3 ]; do
      candidate_admin="http://127.0.0.1:$((base_port + 100 + candidate - 1))"
      if curl -fsS --max-time 2 -X POST "$candidate_admin/admin/standby_snapshot/0" >"$lab_root/trigger.json" 2>/dev/null; then
        voter="$candidate"; voter_admin="$candidate_admin"; triggered=1; break
      fi
      candidate=$((candidate + 1))
    done
    test "$triggered" = 1 && break
    sleep 1
  done
  test "$triggered" = 1
  deadline=$((SECONDS + 45)); entry_dir=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    entry_dir="$(find "$lab_root/data/node-4/snapshots/0" -mindepth 1 -maxdepth 1 -type d -print -quit 2>/dev/null || :)"
    if test -n "$entry_dir" && test -s "$entry_dir/data.bin" && test -s "$entry_dir/meta.json" && test -s "$entry_dir/sha256"; then break; fi
    sleep 1
  done
  test -n "$entry_dir"; test -s "$entry_dir/data.bin"; test -s "$entry_dir/meta.json"; test -s "$entry_dir/sha256"
  deadline=$((SECONDS + 45)); ads_file="$lab_root/ads.json"
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --max-time 2 "$voter_admin/admin/snapshot_ads" >"$ads_file" 2>/dev/null && python3 - "$ads_file" <<'PY'
import json,sys
ads=json.load(open(sys.argv[1])); assert any(int(a["group"]) == 0 for a in ads)
PY
    then break; fi
    sleep 1
  done
  test -s "$ads_file"
  fetch_url="$(python3 - "$ads_file" <<'PY'
import json,sys
ads=json.load(open(sys.argv[1])); print(next(a["fetch_url"] for a in ads if int(a["group"]) == 0))
PY
  )"
  body="$(python3 -c 'import json,sys; print(json.dumps({"fetch_url":sys.argv[1]}))' "$fetch_url")"
  explicit="$(curl -sS -o "$lab_root/explicit.json" -w '%{http_code}' -X POST "$voter_admin/admin/replicate_standby_snapshot/0" -H 'content-type: application/json' -d "$body")"
  no_body="$(curl -sS -o "$lab_root/no-body.json" -w '%{http_code}' -X POST "$voter_admin/admin/replicate_standby_snapshot/0")"
  daisy="$(curl -sS -o "$lab_root/daisy.json" -w '%{http_code}' -X POST "$voter_admin/admin/daisy_sync/0")"
  test "$explicit" = 409; test "$no_body" = 409; test "$daisy" = 409
  for error_file in "$lab_root/explicit.json" "$lab_root/no-body.json" "$lab_root/daisy.json"; do
    python3 - "$error_file" <<'PY'
import json,sys
assert json.load(open(sys.argv[1])) == {"ok":False,"error":"live standby snapshot install is unsupported"}
PY
  done
  trap - EXIT INT TERM; cleanup_lab
}

case "${1:-}" in
  '') ;;
  --verify-lab-catalog-ad) shift; test "$#" = 0 || { printf '%s\n' 'unexpected verifier argument' >&2; exit 2; }; verify_lab_catalog_ad; exit $? ;;
  *) printf '%s\n' "unknown argument: $1" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-21000}"
GROUPS="${GROUPS:-10}"
NODES="${NODES:-3}"
DATA="${DATA_DIR:-$ROOT/.demo-data}"
# gRPC peer table must include Standby ids or voters cannot replicate to it.
PEER_NODES="$NODES"
if [[ "$STANDBY" == "1" ]]; then
  PEER_NODES=$((NODES + 1))
fi

# Jepsen / external clients: disable background propose_loop.
# Set via JEPSEN=1 or NO_AUTO_PROPOSE=1.
# Use a string (not an empty array) so `set -u` + Bash 3.2 does not trip on
# `"${arr[@]}"` when the array is empty.
NO_AUTO_PROPOSE_FLAG=""
if [[ "${JEPSEN:-0}" == "1" || "${NO_AUTO_PROPOSE:-0}" == "1" ]]; then
  NO_AUTO_PROPOSE_FLAG="--no-auto-propose"
fi

export PATH="${HOME}/.cargo/bin:${PATH}"

rm -rf "$DATA"
mkdir -p "$DATA"

cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"

BIN="$ROOT/target/debug/multiraft-demo"

id=1
while [[ "$id" -le "$NODES" ]]; do
  NODE_DATA="$DATA/node-$id"
  mkdir -p "$NODE_DATA"
  # shellcheck disable=SC2086
  "$BIN" \
    --mode node \
    --node-id "$id" \
    --nodes "$NODES" \
    --peer-nodes "$PEER_NODES" \
    --role voter \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$NODE_DATA" \
    $NO_AUTO_PROPOSE_FLAG \
    >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
  # Stagger binds so peers come up cleanly.
  sleep 0.4
  id=$((id + 1))
done

if [[ "$STANDBY" == "1" ]]; then
  STANDBY_ID=$((NODES + 1))
  NODE_DATA="$DATA/node-$STANDBY_ID"
  mkdir -p "$NODE_DATA"
  "$BIN" \
    --mode node \
    --node-id "$STANDBY_ID" \
    --nodes "$NODES" \
    --peer-nodes "$PEER_NODES" \
    --role standby \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$NODE_DATA" \
    --no-auto-propose \
    >"$DATA/node-$STANDBY_ID.log" 2>&1 &
  echo $! >"$DATA/node-$STANDBY_ID.pid"
  sleep 0.6
fi

echo "cluster started (${NODES} OS processes × ${GROUPS} groups, gRPC)"
echo "  data: $DATA"
id=1
while [[ "$id" -le "$NODES" ]]; do
  admin_port=$((BASE_PORT + 100 + id - 1))
  raft_port=$((BASE_PORT + id - 1))
  echo "  node ${id}: pid=$(cat "$DATA/node-$id.pid") raft=127.0.0.1:${raft_port} admin=http://127.0.0.1:${admin_port}/groups/0/value log=$DATA/node-$id.log"
  id=$((id + 1))
done

if [[ "$STANDBY" == "1" ]]; then
  STANDBY_ID=$((NODES + 1))
  admin_port=$((BASE_PORT + 100 + STANDBY_ID - 1))
  raft_port=$((BASE_PORT + STANDBY_ID - 1))
  echo "  standby ${STANDBY_ID}: pid=$(cat "$DATA/node-$STANDBY_ID.pid") raft=127.0.0.1:${raft_port} admin=http://127.0.0.1:${admin_port}/admin/snapshot_ads log=$DATA/node-$STANDBY_ID.log"

  # Wait briefly for leaders, then add_standby on group 0 via each voter admin until one succeeds.
  echo "  adding standby ${STANDBY_ID} to group 0..."
  added=0
  attempt=1
  while [[ "$attempt" -le 30 ]]; do
    id=1
    while [[ "$id" -le "$NODES" ]]; do
      admin_port=$((BASE_PORT + 100 + id - 1))
      if curl -sf -X POST "http://127.0.0.1:${admin_port}/admin/add_standby/0/${STANDBY_ID}" >/dev/null 2>&1; then
        echo "  add_standby ok via node ${id}"
        added=1
        break
      fi
      id=$((id + 1))
    done
    if [[ "$added" -eq 1 ]]; then
      break
    fi
    sleep 0.5
    attempt=$((attempt + 1))
  done
  if [[ "$added" -ne 1 ]]; then
    echo "  warning: add_standby did not succeed (cluster may still be electing); retry manually:"
    echo "    curl -X POST http://127.0.0.1:$((BASE_PORT + 100))/admin/add_standby/0/${STANDBY_ID}"
  fi
  echo "  optional trigger: curl -X POST http://127.0.0.1:$((BASE_PORT + 100))/admin/standby_snapshot/0"
fi

echo "  metrics: http://127.0.0.1:$((BASE_PORT + 100))/metrics/links"
