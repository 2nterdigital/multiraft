#!/usr/bin/env bash
# Prove one bounded dynamic-membership shape with the multi-process Demo:
#
#   voters {1,2,3}
#     -> add learner 4
#     -> promote 4, voters {1,2,3,4}
#     -> demote a different old voter, RF3 includes node 4
#     -> shut down another old voter and commit through a quorum that requires 4
#
# The script records direct per-node membership and FSM observations. It does
# not prove peer discovery, node deletion, Group deletion, restart, or routing
# migration. It is intentionally independent of the dev-only FSM Factory.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROOF_ROOT="${PROOF_ROOT:-}"
ROUNDS="${ROUNDS:-20}"
BASE_PORT="${BASE_PORT:-24000}"
PORT_STRIDE="${PORT_STRIDE:-200}"
ROUND_TIMEOUT_SECONDS="${ROUND_TIMEOUT_SECONDS:-300}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
DEMO_RUST_LOG="multiraft_demo=info,multiraft_net=info,openraft=warn"
GROUP_ID=0

umask 077

log() {
  printf '[membership-proof] utc=%s run_id=%s %s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID" "$*"
}

fail() {
  log "result=FAIL failure_anchor=$*" >&2
  return 1
}

fail_timeout() {
  log "result=FAIL failure_class=timeout failure_anchor=$*" >&2
  return 1
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*) fail "${name}_must_be_a_positive_integer value=${value}"; return 1 ;;
  esac
  if [[ "$value" -lt 1 ]]; then
    fail "${name}_must_be_a_positive_integer value=${value}"
    return 1
  fi
}

require_positive_integer ROUNDS "$ROUNDS"
require_positive_integer BASE_PORT "$BASE_PORT"
require_positive_integer PORT_STRIDE "$PORT_STRIDE"
require_positive_integer ROUND_TIMEOUT_SECONDS "$ROUND_TIMEOUT_SECONDS"
if [[ "$PORT_STRIDE" -lt 104 ]]; then
  fail "port_stride_must_prevent_cross_round_overlap value=${PORT_STRIDE} minimum=104"
  exit 1
fi

case "$RUN_ID" in
  ''|*[!A-Za-z0-9._-]*)
    fail "run_id_must_use_safe_characters value=${RUN_ID}"
    exit 1
    ;;
esac

case "$PROOF_ROOT" in
  '')
    fail "proof_root_must_be_explicit"
    exit 1
    ;;
  /*) ;;
  *)
    fail "proof_root_must_be_absolute path=${PROOF_ROOT}"
    exit 1
    ;;
esac

case "$PROOF_ROOT" in
  /|"$ROOT")
    fail "unsafe_proof_root path=${PROOF_ROOT}"
    exit 1
    ;;
esac

if [[ -z "${CARGO_TARGET_DIR:-}" || "${CARGO_TARGET_DIR}" != /* ]]; then
  fail "cargo_target_dir_must_be_explicit_and_absolute path=${CARGO_TARGET_DIR:-unset}"
  exit 1
fi
if [[ -z "${TMPDIR:-}" || "${TMPDIR}" != /* ]]; then
  fail "tmpdir_must_be_explicit_and_absolute path=${TMPDIR:-unset}"
  exit 1
fi

highest_port=$((BASE_PORT + (ROUNDS - 1) * PORT_STRIDE + 103))
if [[ "$highest_port" -gt 65535 ]]; then
  fail "port_range_exceeds_u16 highest_port=${highest_port}"
  exit 1
fi

if [[ -e "$PROOF_ROOT" ]]; then
  fail "proof_root_must_not_exist path=${PROOF_ROOT}"
  exit 1
fi
mkdir -p "$PROOF_ROOT"

# Ambient demo/chaos routing must not alter this scenario. In particular,
# voter processes must stay SnapshotMode::Disabled rather than inheriting
# STANDBY=1 from run_demo_cluster.sh.
unset STANDBY DAISY NO_AUTO_PROPOSE JEPSEN SCENARIO

TARGET_ROOT="$CARGO_TARGET_DIR"
BIN="$TARGET_ROOT/debug/multiraft-demo"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

log "phase=build command='cargo build -p multiraft-demo'"
cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"
[[ -x "$BIN" ]] || {
  fail "demo_binary_missing path=${BIN}"
  exit 1
}
BIN_SHA_BEFORE="$(sha256_file "$BIN")"
log "phase=build result=PASS binary=${BIN} binary_sha256=${BIN_SHA_BEFORE}"

admin_url() {
  local base_port="$1"
  local node_id="$2"
  printf 'http://127.0.0.1:%s' "$((base_port + 100 + node_id - 1))"
}

compact_json() {
  python3 -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1]), sort_keys=True, separators=(",", ":")))' "$1"
}

record_http_attempt() {
  local round_root="$1"
  local phase="$2"
  local attempt="$3"
  local node_id="$4"
  local method="$5"
  local endpoint="$6"
  local status="$7"
  local body="$8"
  local curl_exit="$9"
  local curl_stderr="${10}"
  python3 - "$round_root/http-attempts.jsonl" "$RUN_ID" "$phase" "$attempt" \
    "$node_id" "$method" "$endpoint" "$status" "$body" "$curl_exit" \
    "$curl_stderr" <<'PY'
import datetime
import json
import sys

(
    path,
    run_id,
    phase,
    attempt,
    node_id,
    method,
    endpoint,
    status,
    body,
    curl_exit,
    curl_stderr,
) = sys.argv[1:]
record = {
    "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "run_id": run_id,
    "phase": phase,
    "attempt": int(attempt),
    "node_id": int(node_id),
    "method": method,
    "endpoint": endpoint,
    "curl_exit": int(curl_exit),
    "http_status": status,
    "raw_response": body,
    "stderr": curl_stderr,
}
with open(path, "a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
PY
}

admin_http_request() {
  local round_root="$1"
  local phase="$2"
  local attempt="$3"
  local base_port="$4"
  local node_id="$5"
  local method="$6"
  local endpoint="$7"
  local payload="${8:-}"
  local max_time="${9:-3}"
  local response curl_stderr_file

  curl_stderr_file="$round_root/.curl-stderr"
  : >"$curl_stderr_file"
  if [[ "$method" == "POST" && -n "$payload" ]]; then
    if response="$(curl -sS --connect-timeout 1 --max-time "$max_time" -w '\n%{http_code}' \
      -X POST "$(admin_url "$base_port" "$node_id")${endpoint}" \
      -H 'content-type: application/json' -d "$payload" 2>"$curl_stderr_file")"; then
      HTTP_CURL_EXIT=0
    else
      HTTP_CURL_EXIT=$?
    fi
  elif [[ "$method" == "POST" ]]; then
    if response="$(curl -sS --connect-timeout 1 --max-time "$max_time" -w '\n%{http_code}' \
      -X POST "$(admin_url "$base_port" "$node_id")${endpoint}" \
      2>"$curl_stderr_file")"; then
      HTTP_CURL_EXIT=0
    else
      HTTP_CURL_EXIT=$?
    fi
  else
    if response="$(curl -sS --connect-timeout 1 --max-time "$max_time" -w '\n%{http_code}' \
      "$(admin_url "$base_port" "$node_id")${endpoint}" \
      2>"$curl_stderr_file")"; then
      HTTP_CURL_EXIT=0
    else
      HTTP_CURL_EXIT=$?
    fi
  fi

  HTTP_STATUS="$(printf '%s\n' "$response" | tail -n 1)"
  HTTP_BODY="$(printf '%s\n' "$response" | sed '$d')"
  HTTP_STDERR="$(cat "$curl_stderr_file")"
  record_http_attempt "$round_root" "$phase" "$attempt" "$node_id" "$method" \
    "$endpoint" "$HTTP_STATUS" "$HTTP_BODY" "$HTTP_CURL_EXIT" "$HTTP_STDERR"
}

json_field() {
  local body="$1"
  local field="$2"
  python3 -c 'import json,sys; print(json.loads(sys.argv[1])[sys.argv[2]])' "$body" "$field"
}

assert_ports_free() {
  local base_port="$1"
  python3 - "$base_port" <<'PY'
import socket
import sys

base = int(sys.argv[1])
ports = [base + i for i in range(4)] + [base + 100 + i for i in range(4)]
sockets = []
try:
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        sockets.append(sock)
except OSError as exc:
    print(f"port_preflight_failed port={port} error={exc}", file=sys.stderr)
    sys.exit(1)
finally:
    for sock in sockets:
        sock.close()
PY
}

cleanup_nodes() {
  local runtime_root="$1"
  local node_id pid command_line attempt cleanup_failed
  cleanup_failed=0
  node_id=1
  while [[ "$node_id" -le 4 ]]; do
    if [[ -f "$runtime_root/node-${node_id}.pid" ]]; then
      pid="$(sed -n '1p' "$runtime_root/node-${node_id}.pid" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        command_line="$(ps -p "$pid" -o command= 2>/dev/null || true)"
        if [[ "$command_line" != *"$BIN"* || "$command_line" != *"$runtime_root/node-${node_id}"* ]]; then
          log "phase=cleanup node=${node_id} pid=${pid} result=SKIP reason=pid_identity_mismatch"
          cleanup_failed=1
          node_id=$((node_id + 1))
          continue
        fi
        kill "$pid" 2>/dev/null || true
        attempt=1
        while [[ "$attempt" -le 40 ]] && kill -0 "$pid" 2>/dev/null; do
          sleep 0.1
          attempt=$((attempt + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
          kill -KILL "$pid" 2>/dev/null || true
        fi
        wait "$pid" 2>/dev/null || true
        if kill -0 "$pid" 2>/dev/null; then
          log "phase=cleanup node=${node_id} pid=${pid} result=FAIL reason=process_still_alive"
          cleanup_failed=1
        else
          log "phase=cleanup node=${node_id} pid=${pid} result=PASS"
        fi
      fi
    fi
    node_id=$((node_id + 1))
  done
  return "$cleanup_failed"
}

start_node() {
  local runtime_root="$1"
  local base_port="$2"
  local node_id="$3"
  local role="$4"
  local node_root="$runtime_root/node-${node_id}"
  mkdir -p "$node_root"
  RUST_LOG="$DEMO_RUST_LOG" \
    "$BIN" \
      --mode node \
      --node-id "$node_id" \
      --nodes 3 \
      --peer-nodes 4 \
      --role "$role" \
      --base-port "$base_port" \
      --groups 1 \
      --data-dir "$node_root" \
      --no-auto-propose \
      >"$runtime_root/node-${node_id}.log" 2>&1 &
  printf '%s\n' "$!" >"$runtime_root/node-${node_id}.pid"
  log "phase=start node=${node_id} role=${role} pid=$! data=${node_root}"
}

wait_admin() {
  local round_root="$1"
  local base_port="$2"
  local node_id="$3"
  local attempt status body endpoint
  endpoint="/admin/groups/${GROUP_ID}/status"
  attempt=1
  while [[ "$attempt" -le 120 ]]; do
    admin_http_request "$round_root" admin_ready "$attempt" "$base_port" "$node_id" GET "$endpoint" "" 2
    status="$HTTP_STATUS"
    body="$HTTP_BODY"
    if [[ "$status" == "200" ]]; then
      printf '%s' "$body" | python3 -c 'import json,sys; assert json.load(sys.stdin).get("ok") is True' \
        >/dev/null 2>&1 && return 0
    fi
    sleep 0.25
    attempt=$((attempt + 1))
  done
  fail_timeout "admin_not_ready node=${node_id}"
}

status_matches() {
  local body="$1"
  local node_id="$2"
  local voters_csv="$3"
  local learners_csv="$4"
  local min_applied="$5"
  python3 -c '
import json,sys
d=json.loads(sys.argv[1])
want_voters=[int(x) for x in sys.argv[3].split(",") if x]
want_learners=[int(x) for x in sys.argv[4].split(",") if x]
assert d.get("ok") is True
assert d.get("group") == 0
assert d.get("node_id") == int(sys.argv[2])
assert d.get("voters") == want_voters, (d.get("voters"), want_voters)
assert d.get("learners") == want_learners, (d.get("learners"), want_learners)
assert int(d.get("applied_index", 0)) >= int(sys.argv[5])
' "$body" "$node_id" "$voters_csv" "$learners_csv" "$min_applied" >/dev/null 2>&1
}

wait_membership() {
  local round_root="$1"
  local label="$2"
  local base_port="$3"
  local node_ids="$4"
  local voters_csv="$5"
  local learners_csv="$6"
  local min_applied="$7"
  local attempt node_id status body all_match endpoint candidate
  endpoint="/admin/groups/${GROUP_ID}/status"
  attempt=1
  while [[ "$attempt" -le 160 ]]; do
    all_match=1
    for node_id in $node_ids; do
      admin_http_request "$round_root" "$label" "$attempt" "$base_port" "$node_id" GET "$endpoint" "" 2
      status="$HTTP_STATUS"
      body="$HTTP_BODY"
      if [[ "$status" != "200" ]] || ! status_matches "$body" "$node_id" "$voters_csv" "$learners_csv" "$min_applied"; then
        all_match=0
        break
      fi
      candidate="$round_root/.status-${label}-node-${node_id}.candidate.json"
      printf '%s\n' "$(compact_json "$body")" >"$candidate"
    done
    if [[ "$all_match" -eq 1 ]]; then
      for node_id in $node_ids; do
        candidate="$round_root/.status-${label}-node-${node_id}.candidate.json"
        body="$(sed -n '1p' "$candidate")"
        status_matches "$body" "$node_id" "$voters_csv" "$learners_csv" "$min_applied" || {
          fail "frozen_membership_response_failed_recheck phase=${label} node=${node_id}"
          return 1
        }
        mv "$candidate" "$round_root/status-${label}-node-${node_id}.json"
        log "phase=${label} oracle_kind=direct node=${node_id} expected_voters=${voters_csv:-none} expected_learners=${learners_csv:-none} min_applied=${min_applied} response=$(compact_json "$body") result=PASS"
      done
      return 0
    fi
    sleep 0.25
    attempt=$((attempt + 1))
  done

  for node_id in $node_ids; do
    admin_http_request "$round_root" "${label}_timeout_snapshot" 161 "$base_port" "$node_id" GET "$endpoint" "" 2
    body="$HTTP_BODY"
    printf '%s\n' "$body" >"$round_root/status-${label}-timeout-node-${node_id}.json"
    log "phase=${label} oracle_kind=direct node=${node_id} response=${body:-unavailable} result=TIMEOUT"
  done
  fail_timeout "membership_timeout phase=${label} expected_voters=${voters_csv} expected_learners=${learners_csv} min_applied=${min_applied}"
}

post_any() {
  local round_root="$1"
  local label="$2"
  local base_port="$3"
  local node_ids="$4"
  local path="$5"
  local body="${6:-}"
  local attempt node_id response_body status
  attempt=1
  while [[ "$attempt" -le 160 ]]; do
    for node_id in $node_ids; do
      admin_http_request "$round_root" "$label" "$attempt" "$base_port" "$node_id" POST "$path" "$body" 3
      status="$HTTP_STATUS"
      response_body="$HTTP_BODY"
      case "$status" in
        2??)
          if printf '%s' "$response_body" | python3 -c 'import json,sys; assert json.load(sys.stdin).get("ok") is True' >/dev/null 2>&1; then
            ACTION_NODE="$node_id"
            ACTION_BODY="$response_body"
            printf '%s\n' "$(compact_json "$response_body")" >"$round_root/action-${label}.json"
            log "phase=${label} oracle_kind=direct actor_node=${node_id} method=POST endpoint=${path} http_status=${status} response=$(compact_json "$response_body") result=PASS"
            return 0
          fi
          ;;
      esac
    done
    sleep 0.25
    attempt=$((attempt + 1))
  done
  fail_timeout "post_failed phase=${label} endpoint=${path} last_http_status=${status:-none} last_response=${response_body:-none}"
}

propose_delta() {
  local round_root="$1"
  local label="$2"
  local base_port="$3"
  local node_ids="$4"
  local delta="$5"
  local idem="$6"
  post_any "$round_root" "$label" "$base_port" "$node_ids" \
    "/groups/${GROUP_ID}/inc" "{\"delta\":${delta},\"idem\":${idem}}"
  PROPOSAL_INDEX="$(json_field "$ACTION_BODY" index)"
  PROPOSAL_TERM="$(json_field "$ACTION_BODY" term)"
  log "phase=${label} proposal_index=${PROPOSAL_INDEX} proposal_term=${PROPOSAL_TERM} delta=${delta} idem=${idem} result=PASS"
}

read_linearizable_any() {
  local round_root="$1"
  local label="$2"
  local base_port="$3"
  local node_ids="$4"
  local expected_value="$5"
  local attempt node_id status body
  attempt=1
  while [[ "$attempt" -le 160 ]]; do
    for node_id in $node_ids; do
      admin_http_request "$round_root" "$label" "$attempt" "$base_port" "$node_id" GET \
        "/admin/groups/${GROUP_ID}/read_linearizable" "" 3
      status="$HTTP_STATUS"
      body="$HTTP_BODY"
      if [[ "$status" == "200" ]] && python3 -c '
import json,sys
d=json.loads(sys.argv[1])
assert d.get("ok") is True
assert d.get("group") == 0
assert d.get("node_id") == int(sys.argv[2])
assert d.get("value") == int(sys.argv[3])
assert d.get("consistency") == "linearizable"
' "$body" "$node_id" "$expected_value" >/dev/null 2>&1; then
        printf '%s\n' "$(compact_json "$body")" >"$round_root/read-${label}.json"
        READ_NODE="$node_id"
        READ_BODY="$body"
        log "phase=${label} oracle_kind=direct node=${node_id} read_value=${expected_value} read_consistency=linearizable http_status=200 response=$(compact_json "$body") result=PASS"
        return 0
      fi
    done
    sleep 0.25
    attempt=$((attempt + 1))
  done
  fail_timeout "linearizable_read_timeout phase=${label} expected_value=${expected_value}"
}

wait_stale_value() {
  local round_root="$1"
  local label="$2"
  local base_port="$3"
  local expected_value="$4"
  local min_applied="$5"
  local attempt status body endpoint
  endpoint="/groups/${GROUP_ID}/stale"
  attempt=1
  while [[ "$attempt" -le 160 ]]; do
    admin_http_request "$round_root" "$label" "$attempt" "$base_port" 4 GET "$endpoint" "" 2
    status="$HTTP_STATUS"
    body="$HTTP_BODY"
    if [[ "$status" == "200" ]] && python3 -c '
import json,sys
d=json.loads(sys.argv[1])
assert d.get("group") == 0
assert d.get("value") == int(sys.argv[2])
assert d.get("consistency") == "stale"
assert d.get("stale") is True
assert int(d.get("applied_index", 0)) >= int(sys.argv[3])
' "$body" "$expected_value" "$min_applied" >/dev/null 2>&1; then
      printf '%s\n' "$(compact_json "$body")" >"$round_root/stale-${label}-node-4.json"
      log "phase=${label} oracle_kind=direct node=4 read_value=${expected_value} min_applied=${min_applied} response=$(compact_json "$body") result=PASS"
      return 0
    fi
    sleep 0.25
    attempt=$((attempt + 1))
  done
  fail_timeout "standby_stale_read_timeout phase=${label} expected_value=${expected_value} min_applied=${min_applied}"
}

current_leader() {
  local round_root="$1"
  local label="$2"
  local base_port="$3"
  local node_ids="$4"
  local node_id body is_leader endpoint
  endpoint="/admin/groups/${GROUP_ID}/status"
  for node_id in $node_ids; do
    admin_http_request "$round_root" "$label" 1 "$base_port" "$node_id" GET "$endpoint" "" 2
    body="$HTTP_BODY"
    if [[ "$HTTP_STATUS" == "200" && -n "$body" ]]; then
      is_leader="$(python3 -c 'import json,sys; print("1" if json.loads(sys.argv[1]).get("is_leader") else "0")' "$body" 2>/dev/null || true)"
      if [[ "$is_leader" == "1" ]]; then
        printf '%s\n' "$node_id"
        return 0
      fi
    fi
  done
  return 1
}

choose_old_to_demote() {
  local leader="$1"
  local node_id
  for node_id in 1 2 3; do
    if [[ "$node_id" != "$leader" ]]; then
      printf '%s\n' "$node_id"
      return 0
    fi
  done
  return 1
}

csv_without() {
  local excluded="$1"
  local out=""
  local node_id
  for node_id in 1 2 3 4; do
    if [[ "$node_id" == "$excluded" ]]; then
      continue
    fi
    if [[ -z "$out" ]]; then out="$node_id"; else out="${out},${node_id}"; fi
  done
  printf '%s\n' "$out"
}

space_from_csv() {
  printf '%s\n' "${1//,/ }"
}

choose_quorum_dependency_victim() {
  local old_to_demote="$1"
  local leader="$2"
  local node_id
  for node_id in 1 2 3; do
    if [[ "$node_id" != "$old_to_demote" && "$node_id" != "$leader" ]]; then
      printf '%s\n' "$node_id"
      return 0
    fi
  done
  return 1
}

assert_runtime_log_contract() {
  local round_root="$1"
  local runtime_root="$2"
  local old_to_demote="$3"
  local merged="$round_root/membership-transition-runtime.log"
  grep -hE 'demo (learner add|membership promotion|membership demotion) committed' \
    "$runtime_root"/node-*.log >"$merged" || true
  python3 - "$merged" "$old_to_demote" <<'PY' || {
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
old_to_demote = sys.argv[2]
lines = path.read_text(encoding="utf-8").splitlines()

def require(marker, action, target):
    matches = [line for line in lines if marker in line]
    assert matches, marker
    assert any(
        f'action="{action}"' in line
        and "group=0" in line
        and f"target_node={target}" in line
        and "actor_node=" in line
        and "local_voters=" in line
        and "local_learners=" in line
        for line in matches
    ), matches

require("demo learner add committed", "add_learner", "4")
require("demo membership promotion committed", "promote", "4")
require("demo membership demotion committed", "demote", old_to_demote)
PY
    fail "runtime_log_contract_mismatch old_to_demote=${old_to_demote}"
    return 1
  }
  log "phase=runtime_log_contract oracle_kind=direct file=${merged} result=PASS"
}

run_round() {
  local iteration="$1"
  local round_root="$2"
  local runtime_root="$round_root/runtime"
  local base_port=$((BASE_PORT + (iteration - 1) * PORT_STRIDE))
  local idem_base=$((iteration * 1000))
  local leader old_to_demote expected_voters final_voters_space victim active_nodes

  mkdir -p "$runtime_root"
  assert_ports_free "$base_port"
  log "iteration=${iteration} phase=preflight topology='4 OS processes, 1 Group, initial voters 1,2,3, predeclared peer 4' base_port=${base_port} runtime_root=${runtime_root} result=PASS"

  start_node "$runtime_root" "$base_port" 1 voter
  sleep 0.4
  start_node "$runtime_root" "$base_port" 2 voter
  sleep 0.4
  start_node "$runtime_root" "$base_port" 3 voter
  sleep 0.4
  start_node "$runtime_root" "$base_port" 4 standby

  wait_admin "$round_root" "$base_port" 1
  wait_admin "$round_root" "$base_port" 2
  wait_admin "$round_root" "$base_port" 3
  wait_admin "$round_root" "$base_port" 4
  wait_membership "$round_root" bootstrap "$base_port" "1 2 3" "1,2,3" "" 0

  propose_delta "$round_root" baseline_proposal "$base_port" "1 2 3" 1 $((idem_base + 1))
  wait_membership "$round_root" baseline_applied "$base_port" "1 2 3" "1,2,3" "" "$PROPOSAL_INDEX"
  read_linearizable_any "$round_root" baseline_read "$base_port" "1 2 3" 1

  post_any "$round_root" add_learner_4 "$base_port" "1 2 3" "/admin/add_standby/${GROUP_ID}/4"
  wait_membership "$round_root" learner_added "$base_port" "1 2 3 4" "1,2,3" "4" "$PROPOSAL_INDEX"
  wait_stale_value "$round_root" learner_caught_up "$base_port" 1 "$PROPOSAL_INDEX"

  post_any "$round_root" promote_4 "$base_port" "1 2 3" "/admin/promote_standby/${GROUP_ID}/4"
  wait_membership "$round_root" promoted_4 "$base_port" "1 2 3 4" "1,2,3,4" "" "$PROPOSAL_INDEX"
  propose_delta "$round_root" post_promote_proposal "$base_port" "1 2 3 4" 2 $((idem_base + 2))
  wait_membership "$round_root" post_promote_applied "$base_port" "1 2 3 4" "1,2,3,4" "" "$PROPOSAL_INDEX"
  read_linearizable_any "$round_root" post_promote_read "$base_port" "1 2 3 4" 3
  wait_stale_value "$round_root" promoted_node_caught_up "$base_port" 3 "$PROPOSAL_INDEX"

  leader="$(current_leader "$round_root" leader_before_demote "$base_port" "1 2 3 4")" || fail "leader_not_found_before_demote"
  old_to_demote="$(choose_old_to_demote "$leader")" || fail "old_voter_to_demote_not_found leader=${leader}"
  expected_voters="$(csv_without "$old_to_demote")"
  log "phase=choose_different_old_voter leader=${leader} target_node=${old_to_demote} expected_final_voters=${expected_voters} result=PASS"
  post_any "$round_root" demote_old_voter "$base_port" "1 2 3 4" "/admin/demote_standby/${GROUP_ID}/${old_to_demote}"
  wait_membership "$round_root" different_voter_replacement "$base_port" "1 2 3 4" "$expected_voters" "$old_to_demote" "$PROPOSAL_INDEX"

  final_voters_space="$(space_from_csv "$expected_voters")"
  propose_delta "$round_root" post_replacement_proposal "$base_port" "$final_voters_space" 4 $((idem_base + 3))
  wait_membership "$round_root" post_replacement_applied "$base_port" "1 2 3 4" "$expected_voters" "$old_to_demote" "$PROPOSAL_INDEX"
  read_linearizable_any "$round_root" post_replacement_read "$base_port" "$final_voters_space" 7
  wait_stale_value "$round_root" replacement_node_value "$base_port" 7 "$PROPOSAL_INDEX"

  leader="$(current_leader "$round_root" leader_before_quorum_dependency "$base_port" "$final_voters_space")" || fail "leader_not_found_before_quorum_dependency"
  victim="$(choose_quorum_dependency_victim "$old_to_demote" "$leader")" || fail "quorum_dependency_victim_not_found leader=${leader} old_to_demote=${old_to_demote}"
  post_any "$round_root" shutdown_old_voter "$base_port" "$victim" "/admin/shutdown_node/${victim}"
  active_nodes=""
  for node_id in $final_voters_space; do
    if [[ "$node_id" != "$victim" ]]; then
      active_nodes="${active_nodes:+$active_nodes }${node_id}"
    fi
  done
  log "phase=quorum_dependency old_demoted=${old_to_demote} shutdown_old_voter=${victim} active_voters='${active_nodes}' replacement_node=4 result=PASS"

  propose_delta "$round_root" replacement_required_proposal "$base_port" "$active_nodes" 8 $((idem_base + 4))
  wait_membership "$round_root" replacement_required_applied "$base_port" "$active_nodes" "$expected_voters" "$old_to_demote" "$PROPOSAL_INDEX"
  read_linearizable_any "$round_root" replacement_required_read "$base_port" "$active_nodes" 15
  wait_stale_value "$round_root" replacement_required_node_4_value "$base_port" 15 "$PROPOSAL_INDEX"
  assert_runtime_log_contract "$round_root" "$runtime_root" "$old_to_demote"

  BIN_SHA_AFTER="$(sha256_file "$BIN")"
  [[ "$BIN_SHA_AFTER" == "$BIN_SHA_BEFORE" ]] || fail "binary_hash_changed before=${BIN_SHA_BEFORE} after=${BIN_SHA_AFTER}"
  log "iteration=${iteration} phase=complete binary_sha256=${BIN_SHA_AFTER} result=PASS"
  : >"$round_root/round-complete"
}

declared="$ROUNDS"
executed=0
passed=0
failed=0
timed_out=0
iteration=1
while [[ "$iteration" -le "$ROUNDS" ]]; do
  round_root="$PROOF_ROOT/round-${iteration}"
  mkdir -p "$round_root"
  executed=$((executed + 1))
  log "iteration=${iteration} phase=begin"
  set +e
  (
    set -euo pipefail
    ROUND_RUNTIME_ROOT="$round_root/runtime"
    ROUND_WATCHDOG_PID=""
    round_cleanup() {
      local round_rc=$?
      local cleanup_rc=0
      if [[ -n "$ROUND_WATCHDOG_PID" ]]; then
        kill "$ROUND_WATCHDOG_PID" 2>/dev/null || true
        wait "$ROUND_WATCHDOG_PID" 2>/dev/null || true
      fi
      cleanup_nodes "$ROUND_RUNTIME_ROOT" || cleanup_rc=$?
      if [[ "$cleanup_rc" -eq 0 ]]; then
        : >"$round_root/cleanup-complete"
      fi
      if [[ "$round_rc" -eq 0 && "$cleanup_rc" -ne 0 ]]; then
        round_rc=127
      fi
      exit "$round_rc"
    }
    trap round_cleanup EXIT
    trap 'exit 124' USR1
    trap 'exit 130' INT
    trap 'exit 143' TERM
    ROUND_SHELL_PID="${BASHPID:-}"
    if [[ -z "$ROUND_SHELL_PID" ]]; then
      ROUND_SHELL_PID="$(sh -c 'printf %s "$PPID"')"
    fi
    python3 - "$ROUND_SHELL_PID" "$ROUND_TIMEOUT_SECONDS" \
      "$round_root/whole-round-timeout" "$round_root/watchdog.log" \
      "$RUN_ID" "$iteration" <<'PY' >/dev/null 2>&1 &
import datetime
import os
import pathlib
import signal
import sys
import time

parent_pid = int(sys.argv[1])
timeout_seconds = int(sys.argv[2])
marker = pathlib.Path(sys.argv[3])
watchdog_log = pathlib.Path(sys.argv[4])
run_id = sys.argv[5]
iteration = int(sys.argv[6])
time.sleep(timeout_seconds)
marker.touch()
with watchdog_log.open("a", encoding="utf-8") as stream:
    utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    stream.write(
        f"utc={utc} run_id={run_id} iteration={iteration} "
        f"phase=whole_round result=TIMEOUT timeout_seconds={timeout_seconds}\n"
    )
os.kill(parent_pid, signal.SIGUSR1)
PY
    ROUND_WATCHDOG_PID=$!
    run_round "$iteration" "$round_root"
  ) 2>&1 | tee "$round_root/harness.log"
  pipe_status=("${PIPESTATUS[@]}")
  round_rc="${pipe_status[0]}"
  tee_rc="${pipe_status[1]}"
  tee_failed=0
  if [[ "$tee_rc" -ne 0 ]]; then
    round_rc=125
    tee_failed=1
  fi
  set -e
  if [[ "$tee_failed" -ne 0 ]]; then
    failed=$((failed + 1))
    round_result=FAIL
  elif [[ "$round_rc" -eq 124 ]] || [[ -f "$round_root/whole-round-timeout" ]] || grep -q 'failure_class=timeout' "$round_root/harness.log"; then
    round_rc=124
    timed_out=$((timed_out + 1))
    round_result=TIMEOUT
  elif [[ "$round_rc" -eq 0 && -f "$round_root/round-complete" && -f "$round_root/cleanup-complete" ]]; then
    passed=$((passed + 1))
    round_result=PASS
  else
    if [[ ! -f "$round_root/round-complete" ]]; then
      log "iteration=${iteration} phase=harness_contract result=FAIL failure_anchor=missing_round_complete_marker"
    fi
    if [[ ! -f "$round_root/cleanup-complete" ]]; then
      log "iteration=${iteration} phase=harness_contract result=FAIL failure_anchor=missing_cleanup_complete_marker"
    fi
    failed=$((failed + 1))
    round_result=FAIL
  fi
  printf '{"iteration":%s,"exit_code":%s,"result":"%s"}\n' \
    "$iteration" "$round_rc" "$round_result" \
    >"$round_root/iteration-result.json"
  iteration=$((iteration + 1))
done

BIN_SHA_FINAL="$(sha256_file "$BIN")"
cat >"$PROOF_ROOT/summary.json" <<EOF
{"run_id":"${RUN_ID}","declared":${declared},"executed":${executed},"passed":${passed},"failed":${failed},"timeout":${timed_out},"concurrency":1,"port_stride":${PORT_STRIDE},"round_timeout_seconds":${ROUND_TIMEOUT_SECONDS},"seed":"not-exposed","rust_log":"${DEMO_RUST_LOG}","binary_sha256_before":"${BIN_SHA_BEFORE}","binary_sha256_after":"${BIN_SHA_FINAL}"}
EOF
log "phase=summary declared=${declared} executed=${executed} passed=${passed} failed=${failed} timeout=${timed_out} concurrency=1 port_stride=${PORT_STRIDE} round_timeout_seconds=${ROUND_TIMEOUT_SECONDS} seed=not-exposed binary_sha256_before=${BIN_SHA_BEFORE} binary_sha256_after=${BIN_SHA_FINAL}"

if [[ "$executed" -ne "$declared" ]] || [[ $((passed + failed + timed_out)) -ne "$declared" ]] || \
  [[ "$failed" -ne 0 ]] || [[ "$timed_out" -ne 0 ]] || [[ "$passed" -ne "$declared" ]] || \
  [[ "$BIN_SHA_FINAL" != "$BIN_SHA_BEFORE" ]]; then
  exit 1
fi
