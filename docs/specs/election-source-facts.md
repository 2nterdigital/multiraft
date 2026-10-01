# Bounded election source facts

This additive source capability preserves the OpenRaft/openraft-multi exact
`=0.10.0-alpha.30` version, consensus algorithms, parameters, storage formats,
and existing default constructors. Its optional native observer is based on
exact upstream revision `19be0c27e5141d8acea3468cdb8a90875f117c27`; the paired
source revision is fixed by workspace dependencies/lock. This is the library
portion of Ech0 [#199](https://github.com/2nterdigital/ech0-delivery/issues/199),
governed by [#198](https://github.com/2nterdigital/ech0-delivery/issues/198) and
native prerequisite [#201](https://github.com/2nterdigital/ech0-delivery/issues/201).
Chinese: [election-source-facts.zh-CN.md](election-source-facts.zh-CN.md).

Create `ElectionSource` before starting the runtime and subscribe before
construction. `NodeOwner::start_with_election_source` installs it before any
native Group core can consume events. Each source attaches once to one node;
a restarted node uses a distinct consumer-supplied boot identity. Run/boot
identities allow at most 128 safe ASCII bytes and must be opaque identifiers,
never credentials. `RuntimeHandle::sample_election_state` adds serialized public
state points. Default `NodeOwner::start` creates no source or source tasks.

## Ownership and coverage

The library owns the bounded ring, operation identity, transport/native projection
and source lifecycle. Receivers retain a bounded source buffer only: no Raft,
FSM, listener or task. Subscription and waiting spawn no work. Point reads use
existing request admission and a retained source-job registry. Cancellation
abandons the caller waiter; accepted jobs terminate and are joined on shutdown.
Native callbacks never invoke consumer callbacks, serialize data or wait for
output or acquire the ring lock. They only submit owned metadata through the
existing Tokio bounded MPSC `try_send`, count a full/closed queue as explicit
loss, and notify readers. Ordinary ring owners (emit/read/status/close) drain
and project at most capacity packets per call; no drainer task/runtime is added.
The ingress queue and retained ring each have the configured capacity. Status
reports `native_pending`; terminal closure fences ingress and drains all accepted
packets after native join, so terminal pending must be zero. Native collection
also bounds joint-voter/granter copies to 1024 items.

Sequences describe node/boot-local ring admission order, not native emission
chronology across independent producers. Native source time remains intact.
Sequences are node/boot local. Source `attempt_id` identifies init/RPC/point-read
operations, never native campaigns. Native `campaign_id` is transient and scoped
to Group + node + boot; it is not a term or globally unique ballot. Deduplicate
records by run/boot/node/sequence, and correlate campaigns using their separate
native identity. `local_elapsed`, native `source_elapsed`, state sample offsets
and ACK-metrics offsets use one native local monotonic clock. Do not compare
these clocks across nodes or use record collection time as native event time.

The ring retires old records at capacity (1–8192). Receivers report exact missing
buffer-sequence ranges and independently report `NativeDropped` totals/deltas.
Global `evicted` counts retirement, not loss by one receiver; native loss has no
fabricated record sequence, timestamp or campaign identity. Separate subscribers
can return the same record. Capabilities describe available source hooks, not
complete observation of a window. Dropped/retired/limited/missing/unobserved
facts make that window partial; absence of records never proves zero elections.
Public membership points preserve complete joint voter sets and learners up to
1024 items; beyond that limit, the fact is explicitly unknown.

Source closure is emitted only after native/listener/task/FSM release completes,
or before any native Group exists when transport construction fails/cancels.
Receivers do not prevent port/data/provider resource reuse.

## What the source actually proves

- Init dispatch/reply, including native rejection disposition, are independent
  from campaign initiation and quorum formation.
- Outbound typed RequestVote preserves vote, full log id and transfer flag;
  replies preserve vote, grant and full log id. RPC error categories are typed,
  without arbitrary error text. RPC `native_consumed` stays unknown; a network
  grant never proves a native grant. Dropped in-flight attempts report cancellation,
  which cannot retract an already dispatched request.
- Outbound transfer preserves from-vote/target/required-log and typed rejections.
  RPC acceptance does not prove the target has become leader.
- Source-native events preserve actual initialize/automatic/external/transfer
  origins, campaign phases/IDs and resampled timers. Automatic branches include
  actual leased vote update, lease/enabled, random timeout and greater-log state;
  switches not read by an earlier branch remain absent. No observer repeats
  eligibility or computes a substitute election oracle.
- Native inbound requests preserve previous/request ballots and logs, transfer
  flag and actual lease/log/vote rejection branch. Native responses preserve
  accepted/rejected/ignored disposition, followed by actual native tally granters,
  quorum and accepted leader state. Local PreVote grants remain separate from
  network replies. These do not prove IO completion or business authority.
- Serialized public points preserve current full vote, vote update age, role,
  effective/committed memberships and local/cluster commit identities. Actual
  private timer fields remain `PublicPointNotExposed` in that point; only native
  source events supply them. Point reads are not per-renewal streams.
- `last_quorum_acked` is a separate latest metrics point with its own capture
  offset. It means committed AppendEntries quorum ACK, not RequestVote quorum
  or a permission to execute business reads/writes.

The minimal native extension was necessary: stock exact alpha30 exposes neither
campaign origins/IDs, selected timer/lease conditions nor native-consumed vote
results. Public state/network/recorder seams alone could not close attribution.
The extension observes existing owners; no source from Debug text, term/role
deltas or temporal proximity is used. Native storage and transport defaults stay
unchanged; no Cargo cache is edited to impersonate compiled source.

## Public verification seam

`election_source_consumer` uses an independent leased application provider and
actual RF3 gRPC: native initialize/quorum/leadership, inbound lease denial, real
transfer origin, controlled leader loss and automatic campaign source, opaque
business submission, point facts, missing public timer/consumption facts, bounded
lag/repeated subscribers, cancelled accepted jobs, construction failure/owner
Drop, terminal closure and actual listener/FSM reuse. A buffer unit regression
holds the ring lock and proves native ingress still completes without loss or
projection; a separate full-queue regression verifies explicit loss for each
receiver and final draining. Public 20-Group concurrent startup supplements the
single-Group source traces. Source-lock loss was reproduced before this repair;
test assertion failures now await owned cleanup and retain the original failure
and any unconfirmed data root. Existing startup/control/resource regressions cover
unchanged behavior. Root-cause repair and physical comparisons remain separate
integration stages; this observation delivery does not itself claim stability
repair or historical causality.
