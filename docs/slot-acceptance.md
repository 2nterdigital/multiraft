# Fixed-slot local acceptance

`multiraft-slot-demo` is an acceptance-only consumer of `multiraft-slot`. It uses
new data, immutable layouts, RocksDB logs and NativeDurable FSM snapshots. Its
counter and command-id table demonstrate opaque effects, business-key identity
validation and deterministic deduplication. This is not the production Admit
state machine, TiKV integration, an ordered message protocol, or a capacity test.

The first verification run at `76ce0dd` passed all seven cases across the three scenario sets; exact source bindings, limits, retained failures and the open historical observer risk are in the [evidence ledger](./evidence/2026-10-16-slot-foundation.md).

Build from the repository root:

```sh
mkdir -p .tmp/slot-foundation/tmp
env TMPDIR="$PWD/.tmp/slot-foundation/tmp" \
  CARGO_TARGET_DIR="$PWD/.tmp/slot-foundation/target" \
  cargo build -p multiraft-slot-demo
python3 scripts/slot_acceptance.py
```

On macOS, a source RocksDB build may also require `LIBCLANG_PATH` and
`DYLD_LIBRARY_PATH` pointing at the Xcode toolchain's `usr/lib` directory.
`--binary PATH` selects a prebuilt binary. `--output-dir PATH` must name a new
directory; the default is a unique directory below `.tmp/slot-foundation/`.
`--case normal`, `--case recovery` and `--case fingerprint` run the three independent scenario sets; `--case all` runs all three.

The Python standard-library harness launches three actual `Popen` OS processes,
with distinct PIDs, loopback Raft/application/admin ports and per-node data.
It reserves candidate ports simultaneously, releases them before spawning, and
fails visibly if another process wins that local bind race. No lab, SSH or
external process is touched. Each child owns a new process group. Cleanup sends
SIGTERM only to those groups, waits, then uses SIGKILL if necessary. The deliberate
leader-crash scenario uses SIGKILL. SIGINT/SIGTERM and KeyboardInterrupt produce a
failed receipt through mandatory cleanup. Signal handlers only record the first
signal; bounded HTTP operations (at most a 10-second socket timeout), polling and
phase checkpoints stop cooperatively. The same non-throwing handlers remain
installed during bounded cleanup, so repeated signals cannot interrupt it. The
final receipt commit is the completion point. After child cleanup and evidence
closure, both signals are briefly blocked together while restoring both original
handlers, then the original mask is restored; later signals follow those original
handlers. This mask is never inherited by a newly spawned child. SIGKILL cannot be intercepted. Normal shutdown must exit zero and the demo
also requires a complete owner shutdown with no retained requests.

Assertions cover:

- Identical manifests, RocksDB/NativeDurable observations, multiple fixed slots,
  follower forwarding, exact applied effects and linearizable queries.
- Same-id same-intent retries returning original effects and changed intents
  returning business refusal without changing counters.
- A test-only, bounded Condvar apply gate on the leader. Entry is observed before
  checking caller Unknown, retained per-slot inflight, immediate Busy, progress
  on another slot, release and the absence of automatic resubmission.
- A real public HTTP socket sends a gated write. Gate entry is observed before
  the client closes with RST without reading any response. The write retains its
  slot budget, completes after gate release, and an explicit same-ID retry returns
  original effects without a second business increment. Raw socket events record
  this ordering; the caller is never described as having received Unknown.
- Actual leader death, quorum election and write, then restart at the same ports
  with the same data directory and recovery of the committed value.
- Compaction submission followed by polling of completed durable snapshots and
  purge facts on every slot/node; a later committed suffix; clean all-process
  shutdown/reopen; restored values and original dedup results.
- Startup rejection of a changed on-disk layout identity followed by reopening
  the untouched original data with its original layout.
- A separate fresh-data RF3 scenario with two digest-A nodes and one digest-B
  node. A+A can commit; one A plus B cannot return Applied; B has no applied A
  value. Restoring A quorum resolves the potentially late write with an explicit
  same-command-id retry. Unknown never proves absence of commit.

The `/gate` endpoint is enabled only with `--test-controls`. Never expose this
local demo as a production service. A gate has a 45-second safety bound; admin
handlers use independent Tokio workers to release it. `/compact` reports
submission only. `/status` reports local point observations, not quorum readiness. A local `/read`
keeps the `result` schema and adds `applied` (term, node ID, index, or null),
captured with the returned value under the same FSM lock.

Every attempted HTTP operation and its raw response or transport error goes to
`requests.jsonl`. Layouts, per-start child logs, retained data and `receipt.json`
remain on success and failure. The receipt lists PIDs, exact child commands,
completed cases, cleanup results and the failure if any. Exit zero requires all
selected cases and cleanup to pass. Evidence is correctness evidence for this
local run, not throughput, latency or production recovery certification.
