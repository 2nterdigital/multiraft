# Normative library control API

Control belongs to the local MultiRaft runtime. A weak `RuntimeHandle<S>` exposes
`read_group_control_sample`, `try_transfer_group_leader`, and
`observe_group_control_layout`. All take a `ControlContext`: opaque transient
`ControlInvocationId([u8; 16])`, absolute Tokio deadline, and sample/ACK freshness
limits (defaults one second). The caller supplies the static expected voter set.
The identity correlates source facts; it is never persisted, deduplicated, or used
as a guarantee that an effect occurred. Reuse it for an associated independent
layout observation; fresh request identity remains a caller choice.

The existing facade offers corresponding `_at` methods and bounded legacy
methods. One nonqueued control slot per local Node is shared by sampling,
transfer, and layout observation. Owned-runtime request admission/readiness and
shutdown fencing apply first. No raw Raft handle is needed by consumers.

Sampling preserves the existing native ReadIndex/state-point/metrics checks and
qualifications. A transfer binds Group and expected source to the owned local
Raft, takes a fresh sample, checks unchanged observed conditions, then makes at
most one native trigger. The inherited deadline covers every phase. No async
queue or orchestration is inserted between successful precheck and trigger;
this is not an atomic check-and-transfer. Typed rejection retains complete
expected/observed vote and membership, target qualification, ACK age, and full
matched/required log identity. Membership and sample identity error evidence is
boxed to keep errors small without losing facts; inspect/dereference it normally.

`ControlTransferOutcome` retains invocation/local identity, the fresh sample
when obtained, and `GroupControlRequestResult`. Pre-trigger deadline/closed/
busy/precheck failures make zero triggers. A known native failed channel send is
`NotSubmitted` with `Stopped` or existing `NativeFailure`. Deadline/interruption
once the trigger starts is `OutcomeUnknown`; waiter cancellation logs that stage
and cannot retract already-queued native work. Unknown never repeats a trigger.
`TriggerQueued` is submission only; it does not prove leadership changed.

`GroupControlSampleError` preserves typed `ReadIndexFailure` (including quorum
responders and native Storage/Panicked), source hint/identity, membership and
collection-age evidence. Terminal sampler state is Closed. Layout `Unavailable`
retains the original typed error. `TargetObserved`, `SourceObserved`, and
`DifferentLeaderObserved` independently classify a fresh sample; they establish
no request causality and never rewrite submission. Wrong-Group samples are
refused. A sampled collection duration is not a self-updating freshness token.

## Source logs and downstream field integration

Target `multiraft::control`: bounded INFO start/terminal facts and leader/vote
change observations; DEBUG carries finite source detail (at most 16 targets,
two voter configurations with 16 members each and 16 learners per membership).
Complete evidence remains in typed returns; `detail_truncated` explicitly marks
bounded membership output. No command/query bytes, consumer error/panic text,
credentials, or additional ReadIndex calls are used for logging. Exactly one
retained native metrics watcher per Group emits initial/change observations;
unchanged updates are quiet, registration reuses the existing watcher. Changes
without direct causal evidence are explicitly `cause_unknown`.

Fields to allow and preserve in downstream structured sinks:

- Common: `invocation_id`, `group_id`, `local_node_id`, `source_node_id`,
  `target_node_id`, `stage`, `result`, `reason_code`, `duration_ms`.
- Vote/leader: `leader_node_id`, `previous_leader_node_id`, `vote_term`,
  `vote_node_id`, `vote_committed`, `previous_vote_term`, `previous_vote_node_id`,
  `previous_vote_committed`, `expected_vote_term`, `expected_vote_node_id`,
  `expected_vote_committed`, `observed_vote_term`, `observed_vote_node_id`,
  `observed_vote_committed`, `sample_age_ms`.
- Target detail (DEBUG): `qualification`, `target_ack_age_ms`, `target_ack_max_age_ms`,
  `matched_term`, `matched_node_id`, `matched_index`, `required_term`,
  `required_node_id`, `required_index`, `target_detail_truncated`.
  Missing facts are explicitly absent, not zero; structured returns remain primary.
- Membership detail: `membership_scope`, `membership_kind`, `membership_log_term`,
  `membership_log_node_id`, `membership_log_index`, `voter_config_count`,
  `voter_config_index`, `voter_node_id`, `learner_count`, `learner_node_id`,
  `detail_truncated`.

A consumer sink can discard unknown names, redact/cap values, filter a target or
fail to write; missing logs must never be represented as complete source proof.
Ech0's actual JSONL allowlist/caps and control RPC/report propagation are paired
integration work in #176. The library does not install its own subscriber or
interpret Ech0 service/business identities.

Validation seam: `tests/control_consumer.rs` is an independent simple FSM
consumer using only public owned APIs over real loopback RF3 gRPC. Private
`group_control/tests.rs` deterministically exercises zero/one trigger and staged
cancel/deadline source evidence; these helpers are not public production probes.
