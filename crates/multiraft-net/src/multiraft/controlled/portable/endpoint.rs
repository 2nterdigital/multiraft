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
    sender: Option<mpsc::Sender<ChunkResult>>,
    accepting: bool,
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
    restarting: Arc<tokio::sync::Mutex<()>>,
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
            restarting: self.restarting.clone(),
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
        if added != [node.node_id]
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
            restarting: Arc::default(),
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
                if state.scope != request.scope || !state.accepting {
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
                if state
                    .sender
                    .as_ref()
                    .ok_or(ControlledError::Rejected("superseded seed input"))?
                    .send(Ok(chunk.clone()))
                    .await
                    .is_err()
                {
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
                    if state.scope != request.scope || !state.accepting {
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
            || header.record.executor_epoch == 0
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
        let _restarting = self.restarting.lock().await;
        let catalog = self.target.authority.read(self.identity.group).await?;
        if catalog.group_id != self.identity.group
            || !catalog.admissions.contains(&admission)
            || admission.request.node_id != self.identity.node
            || admission.request.group_id != self.identity.group
            || admission.request.generation != self.identity.generation
        {
            return Err(ControlledError::Rejected(
                "seed admission is not authoritative",
            ));
        }
        let bound = catalog.active_operation.as_ref().is_some_and(|binding| {
            binding.group_id == self.identity.group
                && binding.specification == self.specification
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
        let old =
            {
                let mut state = self.installation.lock().await;
                if let Some(existing) = state.as_mut() {
                    if existing.scope == scope
                        && existing.header == header
                        && existing.admission == admission
                        && existing.accepting
                    {
                        return Ok((NativeTargetOutcome::InProgress, None));
                    }
                    if existing.scope.correlation == scope.correlation {
                        return Err(ControlledError::Rejected(
                            "seed correlation was consumed or changed",
                        ));
                    }
                    let outcome = existing.outcome.lock().expect("seed outcome lock poisoned");
                    // Failed unverified input cannot permanently raise the native operation fence.
                    if outcome.as_ref().is_none_or(|outcome| outcome.is_ok())
                        && (header.record.executor_epoch < existing.header.record.executor_epoch
                            || !super::super::seed::covers(
                                header
                                    .meta
                                    .last_log_id
                                    .ok_or(ControlledError::Rejected("seed floor missing"))?,
                                existing.header.meta.last_log_id.ok_or(
                                    ControlledError::Rejected("accepted seed floor missing"),
                                )?,
                            ))
                    {
                        return Err(ControlledError::Rejected(
                            "seed retry regresses accepted epoch or floor",
                        ));
                    }
                    drop(outcome);
                    existing.accepting = false;
                    Some((
                        existing.sender.take(),
                        existing.outcome.clone(),
                        existing.changed.clone(),
                    ))
                } else {
                    None
                }
            };
        if let Some((sender, outcome, changed)) = old {
            if let Some(sender) = sender {
                let _ = sender
                    .send(Err("seed transfer explicitly superseded".to_owned()))
                    .await;
            }
            // This is the real owned command completion, with no waiter deadline.
            loop {
                let notified = changed.notified();
                if outcome
                    .lock()
                    .expect("seed outcome lock poisoned")
                    .is_some()
                {
                    break;
                }
                notified.await;
            }
            let node = self.target.node()?;
            let owner = node.group_owner(self.identity.group);
            let anchor_ready = {
                let _command = owner.serial.lock().await;
                let receipt = self.target.checked_receipt()?;
                if receipt.stage == ControlledGroupStage::Opened {
                    return Ok((
                        NativeTargetOutcome::Durable,
                        Some(self.observe(None, None).await?),
                    ));
                }
                if !matches!(
                    receipt.stage,
                    ControlledGroupStage::Reserved | ControlledGroupStage::Opening
                ) {
                    return Err(ControlledError::Rejected(
                        "superseded target is no longer constructible",
                    ));
                }
                if let Ok((_, mut sm)) = node.controlled_parts(self.identity.group) {
                    sm.wait_native_quiescent().await;
                    let actual = sm.controlled_ledger().await?.current;
                    if let Some(actual) =
                        actual.filter(|actual| actual.specification == self.specification)
                    {
                        let (applied, _) = sm
                            .applied_state()
                            .await
                            .map_err(|error| ControlledError::Native(error.to_string()))?;
                        if header.record.executor_epoch < actual.executor_epoch
                            || applied.is_some_and(|floor| {
                                header
                                    .meta
                                    .last_log_id
                                    .is_none_or(|cut| !super::super::seed::covers(cut, floor))
                            })
                        {
                            return Err(ControlledError::Rejected(
                                "seed retry regresses actual native operation",
                            ));
                        }
                    }
                    super::super::seed::recover_anchor(&node, &self.open_request()).await?
                } else {
                    false
                }
            };
            if anchor_ready {
                let mut request = self.open_request();
                request.mode = ControlledOpenMode::Recover;
                node.open_controlled_group_completion(
                    request,
                    self.target.authority.clone(),
                    admission.clone(),
                    None,
                    None,
                )
                .await?;
                return Ok((
                    NativeTargetOutcome::Durable,
                    Some(self.observe(None, None).await?),
                ));
            }
        }
        // Publish the session in the same poll as native task registration: losing the
        // RPC waiter cannot leave an admitted command without its retained completion.
        let mut state = self.installation.lock().await;
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
                    .open_target_input(seed, opening_admission)
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
            sender: Some(sender),
            accepting: true,
            next: 0,
            last: None,
            outcome,
            changed,
        });
        Ok((NativeTargetOutcome::InProgress, None))
    }
    fn open_request(&self) -> ControlledOpen {
        ControlledOpen {
            group_id: self.identity.group,
            generation: self.identity.generation,
            identity: self.identity.consumption.clone(),
            mode: ControlledOpenMode::Target {
                operation: self.specification.operation,
            },
        }
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
        if stage == NativeTargetStage::Reserved && self.target.target_specification.is_none() {
            let catalog = self.target.authority.read(self.identity.group).await?;
            if catalog.group_id != self.identity.group
                || catalog.active_operation.as_ref().is_none_or(|binding| {
                    binding.group_id != self.identity.group
                        || binding.specification != self.specification
                        || binding.target.node_id != self.identity.node
                        || binding.target.generation != self.identity.generation
                        || binding.target.consumption_id != receipt.identity
                })
            {
                return Err(ControlledError::Rejected(
                    "inspected reservation lacks exact active native binding",
                ));
            }
        }
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
