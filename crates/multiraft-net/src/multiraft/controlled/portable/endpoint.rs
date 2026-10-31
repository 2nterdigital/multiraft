//! Target-local dispatcher: decoded requests never grant startup, executor, or cleanup authority.
use super::super::seed_stream::{ChunkResult, SeedInput};
use super::*;
use multiraft_store::{
    disk_snapshot::{DiskManifest, MAX_CHUNK_BYTES, MAX_MANIFEST_BYTES},
    ControlledGroupStage,
};
use openraft::{storage::RaftStateMachine, vote::RaftLeaderId};
use tokio::sync::{mpsc, Notify};

fn classify_error(error: &ControlledError) -> NativeTargetOutcome {
    match error {
        ControlledError::Deadline {
            outcome_unknown: true,
        }
        | ControlledError::Closed => NativeTargetOutcome::Unknown,
        ControlledError::Deadline {
            outcome_unknown: false,
        } => NativeTargetOutcome::Unsent,
        ControlledError::TargetOutcome { outcome, .. } => *outcome,
        _ => NativeTargetOutcome::Rejected,
    }
}
struct Installation {
    scope: NativeTargetScope,
    header: NativeSeedHeader,
    admission: StartupAdmission,
    sender: mpsc::Sender<ChunkResult>,
    next: u64,
    last: Option<(u64, Vec<u8>)>,
    outcome: Arc<Mutex<Option<Result<(), NativeTargetTransportError>>>>,
    changed: Arc<Notify>,
}
/// Dispatcher bound to one locally reserved native target, served only over authenticated channels.
/// The host authenticates its caller and grants this capability only to the controller for this
/// data space; the dispatcher checks every field again against its own immutable native handle.
pub struct NativeTargetEndpoint<S: StateMachine> {
    target: ControlledGroup<S>,
    specification: ReplacementSpec,
    identity: NativeTargetIdentity,
    installation: Arc<tokio::sync::Mutex<Option<Installation>>>,
}
impl<S: StateMachine> std::fmt::Debug for NativeTargetEndpoint<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeTargetEndpoint")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl<S: StateMachine> Clone for NativeTargetEndpoint<S> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            specification: self.specification.clone(),
            identity: self.identity.clone(),
            installation: self.installation.clone(),
        }
    }
}
impl<S: StateMachine> NativeTargetEndpoint<S> {
    /// Bind a dispatcher to a local reservation (or its exact reopened durable generation).
    pub fn new(
        target: ControlledGroup<S>,
        specification: ReplacementSpec,
    ) -> Result<Self, ControlledError> {
        let node = target.node()?;
        let receipt = target.checked_receipt()?;
        let old: BTreeSet<_> = specification
            .expected_start
            .membership()
            .voter_ids()
            .collect();
        let added: Vec<_> = specification
            .target_voters
            .difference(&old)
            .copied()
            .collect();
        if (receipt.stage == ControlledGroupStage::Reserved
            && target.target_specification.as_ref() != Some(&specification))
            || added != [node.node_id]
            || target.generation != specification.target_generation
            || target
                .target_specification
                .as_ref()
                .is_some_and(|actual| actual != &specification)
        {
            return Err(ControlledError::Rejected(
                "endpoint target specification mismatch",
            ));
        }
        let identity = NativeTargetIdentity {
            node: node.node_id,
            domain: node.config.raft_transport_fingerprint,
            group: target.group,
            generation: target.generation,
            consumption: receipt.identity,
        };
        Ok(Self {
            target,
            specification,
            identity,
            installation: Arc::default(),
        })
    }
    pub fn identity(&self) -> &NativeTargetIdentity {
        &self.identity
    }
    fn validate_scope(&self, scope: &NativeTargetScope) -> Result<(), ControlledError> {
        if scope.version != NATIVE_TARGET_VERSION
            || scope.target != self.identity
            || scope.specification != self.specification
        {
            return Err(ControlledError::Rejected(
                "native target wire scope mismatch",
            ));
        }
        self.target.checked_receipt()?;
        Ok(())
    }
    /// Decode a bounded frame and dispatch locally. The host must authenticate before calling.
    pub async fn dispatch(&self, bytes: &[u8]) -> Result<Vec<u8>, ControlledError> {
        if bytes.len() > 6 * MAX_MANIFEST_BYTES {
            return Err(ControlledError::Rejected(
                "native target request exceeds bound",
            ));
        }
        let request: NativeTargetRequest =
            serde_json::from_slice(bytes).map_err(std::io::Error::other)?;
        let result = match self.validate_scope(&request.scope) {
            Ok(()) => self.execute(&request).await,
            Err(error) => Err(error),
        };
        let (outcome, observation, error) = match result {
            Ok((outcome, observation)) => (outcome, observation, None),
            Err(error) => (classify_error(&error), None, Some(error.to_string())),
        };
        serde_json::to_vec(&NativeTargetReply {
            scope: request.scope,
            command: request.command,
            outcome,
            observation,
            error,
        })
        .map_err(std::io::Error::other)
        .map_err(Into::into)
    }
    async fn execute(
        &self,
        request: &NativeTargetRequest,
    ) -> Result<(NativeTargetOutcome, Option<NativeTargetObservation>), ControlledError> {
        match &request.command {
            NativeTargetCommand::Observe {
                configuration,
                barrier,
            } => Ok((
                NativeTargetOutcome::Durable,
                Some(self.observe(configuration.clone(), *barrier).await?),
            )),
            NativeTargetCommand::StartSeed { header, admission } => {
                self.start(request.scope.clone(), (**header).clone(), admission.clone())
                    .await
            }
            NativeTargetCommand::SeedChunk { sequence, chunk } => {
                if let Some(DiskChunk::Data { bytes, .. }) = chunk {
                    if bytes.len() > MAX_CHUNK_BYTES {
                        return Err(ControlledError::Rejected("seed chunk exceeds bound"));
                    }
                }
                let mut state = self.installation.lock().await;
                let state = state
                    .as_mut()
                    .ok_or(ControlledError::Rejected("seed stream was not started"))?;
                if state.scope != request.scope {
                    return Err(ControlledError::Rejected(
                        "seed transfer correlation mismatch",
                    ));
                }
                let bytes = serde_json::to_vec(chunk).map_err(std::io::Error::other)?;
                if state.last.as_ref() == Some(&(*sequence, bytes.clone())) {
                    return Ok((NativeTargetOutcome::InProgress, None));
                }
                if *sequence != state.next {
                    return Err(ControlledError::Rejected("seed chunk sequence mismatch"));
                }
                if state.sender.send(Ok(chunk.clone())).await.is_err() {
                    let outcome = state.outcome.clone();
                    let changed = state.changed.clone();
                    loop {
                        let notified = changed.notified();
                        let result = outcome.lock().expect("seed outcome lock poisoned").clone();
                        if let Some(result) = result {
                            result.map_err(|error| ControlledError::TargetOutcome {
                                outcome: error.outcome,
                                message: error.message,
                            })?;
                            break;
                        }
                        notified.await;
                    }
                    return Ok((
                        NativeTargetOutcome::Durable,
                        Some(self.observe(None, None).await?),
                    ));
                }
                state.next += 1;
                state.last = Some((*sequence, bytes));
                Ok((NativeTargetOutcome::InProgress, None))
            }
            NativeTargetCommand::FinishSeed => {
                let (outcome, changed) = {
                    let state = self.installation.lock().await;
                    let state = state
                        .as_ref()
                        .ok_or(ControlledError::Rejected("seed stream was not started"))?;
                    if state.scope != request.scope {
                        return Err(ControlledError::Rejected(
                            "seed transfer correlation mismatch",
                        ));
                    }
                    (state.outcome.clone(), state.changed.clone())
                };
                loop {
                    let notified = changed.notified();
                    let result = outcome.lock().expect("seed outcome lock poisoned").clone();
                    if let Some(result) = result {
                        result.map_err(|error| ControlledError::TargetOutcome {
                            outcome: error.outcome,
                            message: error.message,
                        })?;
                        break;
                    }
                    notified.await;
                }
                Ok((
                    NativeTargetOutcome::Durable,
                    Some(self.observe(None, None).await?),
                ))
            }
        }
    }
    async fn start(
        &self,
        scope: NativeTargetScope,
        header: NativeSeedHeader,
        admission: StartupAdmission,
    ) -> Result<(NativeTargetOutcome, Option<NativeTargetObservation>), ControlledError> {
        let manifest = DiskManifest::decode(&header.manifest, self.identity.group)?;
        if manifest.meta != header.meta
            || header.record.specification != self.specification
            || header.record.terminal.is_some()
            || header.meta.last_membership != self.specification.expected_start
            || !header.vote.committed
            || header
                .meta
                .last_log_id
                .is_none_or(|cut| header.vote.leader_id().to_committed() != cut.leader_id)
        {
            return Err(ControlledError::Rejected(
                "seed header operation or configuration mismatch",
            ));
        }
        let catalog = self.target.authority.read(self.identity.group).await?;
        if !catalog.admissions.contains(&admission)
            || admission.request.node_id != self.identity.node
            || admission.request.group_id != self.identity.group
            || admission.request.generation != self.identity.generation
        {
            return Err(ControlledError::Rejected(
                "seed admission is not authoritative",
            ));
        }
        let bound = catalog.active_operation.as_ref().is_some_and(|binding| {
            binding.specification == self.specification
                && binding.target.node_id == self.identity.node
                && binding.target.generation == self.identity.generation
                && binding.target.consumption_id == self.identity.consumption
        });
        let completed = catalog.completed.iter().any(|proof| {
            proof.native_record.specification == self.specification
                && proof.target_opened.node_id == self.identity.node
                && proof.target_opened.generation == self.identity.generation
                && proof.target_opened.consumption_id == self.identity.consumption
        });
        if !bound && !completed {
            return Err(ControlledError::Rejected(
                "seed operation not bound by catalog",
            ));
        }
        if self.target.checked_receipt()?.stage == ControlledGroupStage::Opened {
            return Ok((
                NativeTargetOutcome::Durable,
                Some(self.observe(None, None).await?),
            ));
        }
        let mut state = self.installation.lock().await;
        if let Some(existing) = state.as_ref() {
            if existing.scope != scope
                || existing.header != header
                || existing.admission != admission
            {
                return Err(ControlledError::Rejected("seed transfer identity conflict"));
            }
            return Ok((NativeTargetOutcome::InProgress, None));
        }
        let (sender, receiver) = mpsc::channel(1);
        let outcome = Arc::new(Mutex::new(None));
        let changed = Arc::new(Notify::new());
        let seed = ControlledSeed {
            specification: self.specification.clone(),
            record: header.record.clone(),
            vote: header.vote,
            fingerprint: self.identity.domain,
            meta: header.meta.clone(),
            manifest: header.manifest.clone(),
            input: SeedInput::received(receiver),
        };
        let target = self.target.clone();
        let opening_admission = admission.clone();
        let saved_outcome = outcome.clone();
        let saved_changed = changed.clone();
        let node = self.target.node()?;
        if !node
            .group_owner(self.identity.group)
            .watchers
            .spawn(async move {
                let result = target
                    .open_target_input(
                        seed,
                        opening_admission,
                        tokio::time::Instant::now() + Duration::from_secs(120),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| NativeTargetTransportError {
                        outcome: classify_error(&error),
                        message: error.to_string(),
                    });
                *saved_outcome.lock().expect("seed outcome lock poisoned") = Some(result);
                saved_changed.notify_waiters();
            })
        {
            return Err(ControlledError::Closed);
        }
        *state = Some(Installation {
            scope,
            header,
            admission,
            sender,
            next: 0,
            last: None,
            outcome,
            changed,
        });
        Ok((NativeTargetOutcome::InProgress, None))
    }
    async fn observe(
        &self,
        configuration: Option<StoredMembership>,
        barrier: Option<LogId>,
    ) -> Result<NativeTargetObservation, ControlledError> {
        if configuration.as_ref().and_then(|config| *config.log_id()) != barrier
            && configuration.is_some()
        {
            return Err(ControlledError::Rejected(
                "target barrier does not identify exact configuration",
            ));
        }
        let receipt = self.target.checked_receipt()?;
        let stage = match receipt.stage {
            ControlledGroupStage::Reserved => NativeTargetStage::Reserved,
            ControlledGroupStage::Opening => NativeTargetStage::Opening,
            ControlledGroupStage::Opened => NativeTargetStage::Opened,
            ControlledGroupStage::Retired => NativeTargetStage::Retired,
            ControlledGroupStage::LogsCleaned => NativeTargetStage::LogsCleaned,
            ControlledGroupStage::Cleaned => NativeTargetStage::Cleaned,
        };
        if stage != NativeTargetStage::Opened {
            if configuration.is_some() {
                return Err(ControlledError::Rejected("target core not opened"));
            }
            return Ok(NativeTargetObservation {
                stage,
                configuration: None,
                applied: None,
                record: None,
            });
        }
        let target = self.target.clone();
        let node = self.target.node()?;
        let owner = node.group_owner(target.group);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Reads must remain owned for cleanup, but cannot acquire the actor serial lock:
        // this target can itself be the source leader executing Complete through a local port.
        if !owner.watchers.spawn(async move {
            let result = async {
                let (raft, mut sm) = node.controlled_parts(target.group)?;
                let mut stopping = node.group_owner(target.group).stopping();
                loop {
                    target.checked_active()?;
                    if *stopping.borrow_and_update() || raft.metrics().borrow_watched().running_state.is_err() { return Err(ControlledError::Closed); }
                    let (applied, actual) = sm.applied_state().await.map_err(|error| ControlledError::Native(error.to_string()))?;
                    let record = sm.controlled_ledger().await?.current;
                    if configuration.as_ref().is_none_or(|configuration| actual == *configuration && applied.is_some_and(|applied| barrier.is_none_or(|floor| super::super::seed::covers(applied, floor)))) {
                        target.checked_active()?;
                        return Ok(NativeTargetObservation { stage, configuration: Some(actual), applied, record });
                    }
                    if configuration.as_ref().is_some_and(|wanted| actual.log_id() >= wanted.log_id() && actual != *wanted) {
                        return Err(ControlledError::Rejected("target applied a different configuration"));
                    }
                    tokio::select! { _ = stopping.changed() => return Err(ControlledError::Closed), _ = tokio::time::sleep(Duration::from_millis(20)) => {} }
                }
            }.await;
            let _ = sender.send(result);
        }) { return Err(ControlledError::Closed); }
        receiver.await.map_err(|_| ControlledError::Closed)?
    }
}
impl<S: StateMachine> NativeTargetTransport for NativeTargetEndpoint<S> {
    fn peer(&self) -> (NodeId, Option<[u8; 32]>) {
        (self.identity.node, self.identity.domain)
    }
    fn exchange(
        &self,
        request: Vec<u8>,
    ) -> StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            self.dispatch(&request)
                .await
                .map_err(|error| NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: error.to_string(),
                })
        })
    }
}
