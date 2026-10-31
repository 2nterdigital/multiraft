//! Fresh committed operation checkpoints establish a native vote/log floor before route publication.
use super::*;
use multiraft_core::typ::{self, LogId, SnapshotMeta, Vote};
use multiraft_store::disk_snapshot::ChunkSource;
use openraft::storage::{RaftLogReader, RaftLogStorage, RaftStateMachine};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeedAnchor {
    node: NodeId,
    group: GroupId,
    generation: u64,
    identity: Vec<u8>,
    specification: ReplacementSpec,
    record: multiraft_core::OperationRecord,
    vote: Vote,
    meta: SnapshotMeta,
}
fn anchor_path(root: &std::path::Path, group: GroupId, generation: u64) -> PathBuf {
    root.join("controlled-seed")
        .join(format!("group-{group}"))
        .join(format!("generation-{generation}.json"))
}

impl<S: StateMachine> ControlledGroup<S> {
    /// Capture a fresh actual native checkpoint containing this operation's committed Begin.
    /// A cached pre-operation snapshot or fabricated header can never create this descriptor.
    pub async fn prepare_seed(
        &self,
        executor: &ControlledExecutor,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledSeed, ControlledError> {
        let this = self.clone();
        let executor = executor.clone();
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, mut sm) = node.controlled_parts(this.group)?;
                let record =
                    super::source::checked_executor(&raft, &sm, &executor, node.node_id).await?;
                if sm.applied_state().await.map_err(native)?.1
                    != record.specification.expected_start
                {
                    return Err(ControlledError::Rejected(
                        "fresh seeding requires the exact starting native configuration",
                    ));
                }
                let reservation = sm.reserve_native_compaction().await.map_err(|error| {
                    ControlledError::Native(format!("native seed capture: {error:?}"))
                })?;
                struct Pending<T: StateMachine>(StateMachineStore<T>);
                impl<T: StateMachine> Drop for Pending<T> {
                    fn drop(&mut self) {
                        self.0.release_native_reservation();
                    }
                }
                let _pending = Pending(sm.clone());
                raft.trigger().snapshot().await.map_err(native)?;
                reservation.completion.await.map_err(native)?;
                sm.wait_native_quiescent().await;
                let snapshot = sm
                    .get_current_snapshot()
                    .await?
                    .ok_or(ControlledError::Rejected("fresh native checkpoint missing"))?;
                if snapshot.meta.last_log_id < reservation.target
                    || snapshot.meta.last_membership != record.specification.expected_start
                {
                    return Err(ControlledError::Rejected(
                        "fresh native checkpoint cut mismatch",
                    ));
                }
                let manifest = snapshot.snapshot.into_inner();
                let actual = sm
                    .inspect_controlled_checkpoint(&snapshot.meta, &manifest)
                    .await?;
                if actual.ledger.current.as_ref() != Some(&record)
                    || actual.membership != record.specification.expected_start
                    || Some(actual.applied) != snapshot.meta.last_log_id
                {
                    return Err(ControlledError::Rejected(
                        "actual checkpoint does not contain the current committed operation",
                    ));
                }
                super::source::checked_executor(&raft, &sm, &executor, node.node_id).await?;
                let vote = raft.metrics().borrow_watched().vote;
                Ok(ControlledSeed {
                    specification: record.specification.clone(),
                    record,
                    vote,
                    meta: snapshot.meta,
                    manifest,
                })
            })
            .await
    }
}
fn native(error: impl std::fmt::Display) -> ControlledError {
    ControlledError::Native(error.to_string())
}
fn covers(actual: LogId, floor: LogId) -> bool {
    actual >= floor
        && actual.index >= floor.index
        && (actual.index != floor.index || actual == floor)
}
fn validate_anchor<S: StateMachine>(
    node: &MultiRaft<S>,
    request: &ControlledOpen,
    anchor: &SeedAnchor,
) -> Result<LogId, ControlledError> {
    let old: BTreeSet<_> = anchor
        .specification
        .expected_start
        .membership()
        .voter_ids()
        .collect();
    let target: Vec<_> = anchor
        .specification
        .target_voters
        .difference(&old)
        .copied()
        .collect();
    if anchor.node != node.node_id
        || anchor.group != request.group_id
        || anchor.generation != request.generation
        || anchor.identity != request.identity
        || anchor.specification.target_generation != request.generation
        || target != [node.node_id]
        || anchor.record.specification != anchor.specification
        || anchor.record.terminal.is_some()
        || anchor.record.executor_epoch == 0
        || anchor.meta.last_membership != anchor.specification.expected_start
        || !anchor.vote.committed
    {
        return Err(ControlledError::Rejected(
            "seed operation or generation identity mismatch",
        ));
    }
    anchor
        .meta
        .last_log_id
        .ok_or(ControlledError::Rejected("seed native log floor missing"))
}

pub(super) async fn install<S: StateMachine>(
    node: &Arc<MultiRaft<S>>,
    request: &ControlledOpen,
    authority: &Arc<dyn StartupAuthority>,
    seed: ControlledSeed,
) -> Result<(), ControlledError> {
    let anchor = SeedAnchor {
        node: node.node_id,
        group: request.group_id,
        generation: request.generation,
        identity: request.identity.clone(),
        specification: seed.specification,
        record: seed.record,
        vote: seed.vote,
        meta: seed.meta,
    };
    let cut = validate_anchor(node, request, &anchor)?;
    let catalog = authority.read(request.group_id).await?;
    if catalog.active_operation.as_ref().is_none_or(|binding| {
        binding.specification != anchor.specification
            || binding.target.node_id != node.node_id
            || binding.target.generation != request.generation
            || binding.target.consumption_id != request.identity
    }) {
        return Err(ControlledError::Rejected(
            "fresh seed is not the durable active target operation",
        ));
    }
    if recover_anchor(node, request).await? {
        return Ok(());
    }
    if !node
        .quarantined_groups
        .lock()
        .expect("quarantined group lock poisoned")
        .contains_key(&request.group_id)
    {
        return Err(ControlledError::Rejected(
            "target seed requires an unpublished native core",
        ));
    }
    let (raft, sm) = node.controlled_parts(request.group_id)?;
    let manifest = seed.manifest;
    let source_manifest = manifest.clone();
    let group = request.group_id;
    let mut source = tokio::task::spawn_blocking(move || ChunkSource::open(source_manifest, group))
        .await
        .map_err(native)??;
    let mut stopping = node.group_owner(group).stopping();
    loop {
        if *stopping.borrow_and_update() {
            return Err(ControlledError::Closed);
        }
        let (returned, chunk) = tokio::task::spawn_blocking(move || {
            let chunk = source.next_chunk();
            (source, chunk)
        })
        .await
        .map_err(native)?;
        source = returned;
        let Some(chunk) = chunk? else {
            break;
        };
        sm.receive_disk_chunk(chunk).await?;
    }
    tokio::task::spawn_blocking(move || drop(source))
        .await
        .map_err(native)?;
    sm.verify_disk_snapshot(&anchor.meta, &manifest).await?;
    raft.install_full_snapshot(
        anchor.vote,
        openraft::alias::SnapshotOf::<TypeConfig, typ::SnapshotData> {
            meta: anchor.meta.clone(),
            snapshot: std::io::Cursor::new(manifest),
        },
    )
    .await
    .map_err(native)?;
    sm.wait_native_quiescent().await;
    verify_actual(node, request, &anchor, true).await?;
    native_basis(node, request.group_id, cut, anchor.vote).await?;
    let bytes = serde_json::to_vec(&anchor).map_err(std::io::Error::other)?;
    super::durable_record::publish(
        &node.config.data_dir,
        &anchor_path(&node.config.data_dir, request.group_id, request.generation),
        &bytes,
    )?;
    Ok(())
}

pub(super) async fn recover_anchor<S: StateMachine>(
    node: &Arc<MultiRaft<S>>,
    request: &ControlledOpen,
) -> Result<bool, ControlledError> {
    let bytes = match std::fs::read(anchor_path(
        &node.config.data_dir,
        request.group_id,
        request.generation,
    )) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let anchor: SeedAnchor = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
    let floor = validate_anchor(node, request, &anchor)?;
    verify_actual(node, request, &anchor, false).await?;
    native_basis(node, request.group_id, floor, anchor.vote).await?;
    // Confirm prior interrupted publication is durable before exposing any native route.
    super::durable_record::publish(
        &node.config.data_dir,
        &anchor_path(&node.config.data_dir, request.group_id, request.generation),
        &bytes,
    )?;
    Ok(true)
}
async fn verify_actual<S: StateMachine>(
    node: &Arc<MultiRaft<S>>,
    request: &ControlledOpen,
    anchor: &SeedAnchor,
    exact: bool,
) -> Result<(), ControlledError> {
    let floor = validate_anchor(node, request, anchor)?;
    let (raft, mut sm) = node.controlled_parts(request.group_id)?;
    let snapshot = sm
        .get_current_snapshot()
        .await?
        .ok_or(ControlledError::Rejected(
            "actual target checkpoint missing",
        ))?;
    let actual = sm
        .inspect_controlled_checkpoint(&snapshot.meta, snapshot.snapshot.get_ref())
        .await?;
    if !covers(actual.applied, floor)
        || actual.ledger.high_water < anchor.specification.operation.sequence
        || (actual.ledger.high_water == anchor.specification.operation.sequence
            && actual
                .ledger
                .current
                .as_ref()
                .is_none_or(|record| record.specification != anchor.specification))
        || (exact
            && (actual.applied != floor
                || actual.membership != anchor.specification.expected_start
                || actual.ledger.current.as_ref() != Some(&anchor.record)))
    {
        return Err(ControlledError::Rejected(
            "installed target checkpoint operation or durable floor mismatch",
        ));
    }
    let (applied, membership) = sm.applied_state().await.map_err(native)?;
    if applied.is_none_or(|applied| !covers(applied, floor))
        || membership.log_id() < anchor.specification.expected_start.log_id()
        || (membership.log_id() == anchor.specification.expected_start.log_id()
            && membership != anchor.specification.expected_start)
    {
        return Err(ControlledError::Rejected(
            "target application recovery is behind its seed anchor",
        ));
    }
    let native_membership = membership.clone();
    let native = raft
        .with_raft_state(move |state| {
            state.membership_state.committed().as_ref() == &native_membership
                && state.membership_state.effective().as_ref() == &native_membership
        })
        .await
        .map_err(native)?;
    if !native {
        return Err(ControlledError::Rejected(
            "target native membership differs from its durable application floor",
        ));
    }
    Ok(())
}
async fn native_basis<S: StateMachine>(
    node: &Arc<MultiRaft<S>>,
    group: GroupId,
    floor: LogId,
    vote: Vote,
) -> Result<(), ControlledError> {
    let mut log = node.open_native_log(group)?;
    let (raft, sm) = node.controlled_parts(group)?;
    let mut stopping = node.group_owner(group).stopping();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(ControlledError::Rejected(
                "seed native durable floor was not observed",
            ));
        }
        if *stopping.borrow_and_update() || raft.metrics().borrow_watched().running_state.is_err() {
            return Err(ControlledError::Closed);
        }
        let persisted_vote = log.read_vote().await?;
        let state = log.get_log_state().await?;
        let committed = log.read_committed().await?;
        if persisted_vote.is_some_and(|actual| actual >= vote)
            && state
                .last_purged_log_id
                .is_some_and(|purged| covers(purged, floor))
            && committed.is_none_or(|committed| covers(committed, floor))
        {
            sm.check_disk_log_basis(&mut log).await?;
            return Ok(());
        }
        tokio::select! { _ = stopping.changed() => return Err(ControlledError::Closed), _ = tokio::time::sleep(Duration::from_millis(20)) => {} }
    }
}
