//! Membership steps and terminal proof run under one local per-group command owner.
use super::portable::{NativeTargetStage, SourceTargetPort};
use super::*;
use crate::controlled_startup::{StartupAuthorityResult, StartupBinding, StartupTargetGeneration};
use multiraft_core::{ControlReply, NativeControl};
use multiraft_store::ControlledGroupStage;
use openraft::{vote::RaftLeaderId, MembershipChangeGuard};

impl<S: StateMachine> ControlledGroup<S> {
    /// Compatibility wrapper through the same authenticated encoded local target port.
    pub async fn begin(
        &self,
        specification: ReplacementSpec,
        epoch: u64,
        target: &Self,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledExecutor, ControlledError> {
        let port = target.target_port(specification.clone())?;
        self.begin_with_port(specification, epoch, &port, deadline)
            .await
    }
    async fn compatibility_port(
        &self,
        target: &Self,
        deadline: tokio::time::Instant,
    ) -> Result<SourceTargetPort, ControlledError> {
        let record = self
            .local_ledger(deadline)
            .await?
            .current
            .ok_or(ControlledError::Rejected("no native operation"))?;
        target.target_port(record.specification)
    }
    /// Compatibility learner open through the portable native target boundary.
    pub async fn add_learner(
        &self,
        executor: &ControlledExecutor,
        target: &Self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        self.add_learner_with_port(
            executor,
            &self.compatibility_port(target, deadline).await?,
            deadline,
        )
        .await
    }
    /// Compatibility joint step through the portable native application witness.
    pub async fn enter_joint(
        &self,
        executor: &ControlledExecutor,
        target: &Self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        self.enter_joint_with_port(
            executor,
            &self.compatibility_port(target, deadline).await?,
            deadline,
        )
        .await
    }
    /// Compatibility completion through the portable native application witness.
    pub async fn complete(
        &self,
        executor: &ControlledExecutor,
        target: &Self,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledTerminal, ControlledError> {
        self.complete_with_port(
            executor,
            &self.compatibility_port(target, deadline).await?,
            deadline,
        )
        .await
    }
    /// Bind the durable startup fence, then commit immutable native operation arguments.
    pub async fn begin_with_port(
        &self,
        specification: ReplacementSpec,
        epoch: u64,
        target: &SourceTargetPort,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledExecutor, ControlledError> {
        let this = self.clone();
        let target = target.with_source_stop(self.node()?.group_owner(self.group).stopping());
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, sm) = node.controlled_parts(this.group)?;
                let leader = current_leader(&raft, node.node_id).await?;
                target.validate_source(&node, this.group, &specification)?;
                let observation = target.observe(None, None).await?;
                if observation.stage != NativeTargetStage::Reserved {
                    return Err(ControlledError::Rejected(
                        "target is not the reserved replacement generation",
                    ));
                }
                let old: BTreeSet<_> = specification
                    .expected_start
                    .membership()
                    .voter_ids()
                    .collect();
                let source: Vec<_> = old
                    .difference(&specification.target_voters)
                    .copied()
                    .collect();
                let added: Vec<_> = specification
                    .target_voters
                    .difference(&old)
                    .copied()
                    .collect();
                if source.len() != 1 || added != [target.identity().node] {
                    return Err(ControlledError::Rejected(
                        "replacement must exchange exactly one voter",
                    ));
                }
                let binding = StartupBinding {
                    group_id: this.group,
                    specification: specification.clone(),
                    source: source[0],
                    source_generation: *this
                        .authority
                        .read(this.group)
                        .await?
                        .authorized
                        .get(&source[0])
                        .ok_or(ControlledError::Rejected(
                            "source generation is not authorized",
                        ))?,
                    target: StartupTargetGeneration {
                        node_id: target.identity().node,
                        generation: target.identity().generation,
                        consumption_id: target.identity().consumption.clone(),
                    },
                };
                match this.authority.bind_active(binding.clone()).await? {
                    StartupAuthorityResult::Applied(actual) if actual == binding => {}
                    StartupAuthorityResult::Unknown
                        if this
                            .authority
                            .read(this.group)
                            .await?
                            .active_operation
                            .as_ref()
                            == Some(&binding) => {}
                    _ => {
                        return Err(ControlledError::Rejected(
                            "startup binding was not durably confirmed",
                        ))
                    }
                }
                let operation = specification.operation;
                write_control(
                    &raft,
                    &sm,
                    leader,
                    NativeControl::Begin {
                        specification,
                        executor_epoch: epoch,
                    },
                )
                .await?;
                Ok(ControlledExecutor {
                    operation,
                    epoch,
                    leader,
                })
            })
            .await
    }

    /// Reacquire the same operation with a new executor epoch and fixed leader incarnation.
    pub async fn claim(
        &self,
        operation: OperationId,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledExecutor, ControlledError> {
        let group = self.group;
        let authority = self.authority.clone();
        let this = self.clone();
        self.node()?
            .controlled_run(group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, sm) = node.controlled_parts(group)?;
                let leader = current_leader(&raft, node.node_id).await?;
                if authority
                    .read(group)
                    .await?
                    .active_operation
                    .as_ref()
                    .is_none_or(|binding| binding.specification.operation != operation)
                {
                    return Err(ControlledError::Rejected("operation is not durably active"));
                }
                write_control(
                    &raft,
                    &sm,
                    leader,
                    NativeControl::Claim {
                        operation,
                        executor_epoch: epoch,
                    },
                )
                .await?;
                Ok(ControlledExecutor {
                    operation,
                    epoch,
                    leader,
                })
            })
            .await
    }

    /// Append one guarded learner membership after the target core has durably opened.
    pub async fn add_learner_with_port(
        &self,
        executor: &ControlledExecutor,
        target: &SourceTargetPort,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        let this = self.clone();
        let executor = executor.clone();
        let target = target.with_source_stop(self.node()?.group_owner(self.group).stopping());
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, sm) = node.controlled_parts(this.group)?;
                let record = checked_executor(&raft, &sm, &executor, node.node_id).await?;
                target.validate_source(&node, this.group, &record.specification)?;
                if target.observe(None, None).await?.stage != NativeTargetStage::Opened {
                    return Err(ControlledError::Rejected(
                        "target core is not durably opened",
                    ));
                }
                let guard = membership_guard(&raft, &executor).await?;
                if guard
                    .effective
                    .membership()
                    .get_node(&target.identity().node)
                    .is_some()
                {
                    return Ok(());
                }
                let addr = node
                    .peer_carrier_address(target.identity().node)
                    .map(|address| address.to_string())
                    .ok_or(ControlledError::Rejected("target peer address is unknown"))?;
                raft.change_membership_guarded(
                    ChangeMembers::AddNodes(BTreeMap::from([(
                        target.identity().node,
                        BasicNode { addr },
                    )])),
                    false,
                    guard,
                )
                .await
                .map_err(|error| ControlledError::Native(error.to_string()))?;
                Ok(())
            })
            .await
    }

    /// Append exactly the joint step; caller cancellation cannot release its command fence.
    pub async fn enter_joint_with_port(
        &self,
        executor: &ControlledExecutor,
        target: &SourceTargetPort,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        self.membership_step(executor, Some(target.clone()), false, deadline)
            .await
    }
    /// Append exactly the final uniform step, dropping the excluded source entirely.
    pub async fn finalize_uniform(
        &self,
        executor: &ControlledExecutor,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        self.membership_step(executor, None, true, deadline).await
    }
    async fn membership_step(
        &self,
        executor: &ControlledExecutor,
        target: Option<SourceTargetPort>,
        finalize: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlledError> {
        let group = self.group;
        let executor = executor.clone();
        let this = self.clone();
        let stopping = self.node()?.group_owner(group).stopping();
        let target = target.map(|target| target.with_source_stop(stopping.clone()));
        self.node()?
            .controlled_run(group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, sm) = node.controlled_parts(group)?;
                let record = checked_executor(&raft, &sm, &executor, node.node_id).await?;
                let guard = membership_guard(&raft, &executor).await?;
                let membership = guard.effective.membership();
                let exact = membership.get_joint_config().len() == 1
                    && membership.voter_ids().collect::<BTreeSet<_>>()
                        == record.specification.target_voters
                    && membership.learner_ids().next().is_none();
                if exact {
                    return Ok(());
                }
                if finalize {
                    if membership.get_joint_config().len() != 2 {
                        return Err(ControlledError::Rejected(
                            "finalization requires the committed joint configuration",
                        ));
                    }
                    raft.change_membership_guarded(
                        ChangeMembers::AddVoterIds(BTreeSet::new()),
                        false,
                        guard,
                    )
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                } else {
                    if membership.get_joint_config().len() == 2 {
                        return Ok(());
                    }
                    let old: BTreeSet<_> = record
                        .specification
                        .expected_start
                        .membership()
                        .voter_ids()
                        .collect();
                    if membership.voter_ids().collect::<BTreeSet<_>>() != old
                        || record
                            .specification
                            .target_voters
                            .iter()
                            .any(|id| membership.get_node(id).is_none())
                    {
                        return Err(ControlledError::Rejected(
                            "joint step requires the opened learner and original voters",
                        ));
                    }
                    if guard.committed != guard.effective {
                        return Err(ControlledError::Rejected(
                            "learner configuration is not committed",
                        ));
                    }
                    let target = target.as_ref().ok_or(ControlledError::Rejected(
                        "target application witness missing",
                    ))?;
                    target.validate_source(&node, group, &record.specification)?;
                    target.receipt_at(&guard.committed).await?;
                    raft.change_membership_guarded(
                        record.specification.target_voters,
                        false,
                        guard,
                    )
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                }
                Ok(())
            })
            .await
    }

    /// Verify durable target application, commit terminal proof, then reconcile startup handoff.
    pub async fn complete_with_port(
        &self,
        executor: &ControlledExecutor,
        target: &SourceTargetPort,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledTerminal, ControlledError> {
        let this = self.clone();
        let executor = executor.clone();
        let target = target.with_source_stop(self.node()?.group_owner(self.group).stopping());
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, sm) = node.controlled_parts(this.group)?;
                // A terminal retry reads the original immutable proof; it never reopens a target.
                raft.ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                let ledger = sm.controlled_ledger().await?;
                let mut record = ledger
                    .current
                    .ok_or(ControlledError::Rejected("no native operation"))?;
                if record.specification.operation != executor.operation
                    || record.executor_epoch != executor.epoch
                {
                    return Err(ControlledError::Rejected("stale executor"));
                }
                if record.terminal.is_none() {
                    checked_executor(&raft, &sm, &executor, node.node_id).await?;
                    let guard = membership_guard(&raft, &executor).await?;
                    let membership = guard.committed.membership();
                    if guard.committed != guard.effective
                        || membership.get_joint_config().len() != 1
                        || membership.voter_ids().collect::<BTreeSet<_>>()
                            != record.specification.target_voters
                        || membership.learner_ids().next().is_some()
                    {
                        return Err(ControlledError::Rejected(
                            "exact final configuration is not committed",
                        ));
                    }
                    target.validate_source(&node, this.group, &record.specification)?;
                    let target_open = target.receipt_at(&guard.committed).await?;
                    let ledger = write_control(
                        &raft,
                        &sm,
                        executor.leader,
                        NativeControl::Complete {
                            operation: executor.operation,
                            executor_epoch: executor.epoch,
                            target_open,
                        },
                    )
                    .await?;
                    record = ledger
                        .current
                        .ok_or(ControlledError::Rejected("native terminal record missing"))?;
                }
                super::terminal::handoff(&this, record).await
            })
            .await
    }
    pub(super) fn checked_active(&self) -> Result<(), ControlledError> {
        if self.checked_receipt()?.stage != ControlledGroupStage::Opened {
            return Err(ControlledError::Rejected("local generation is not active"));
        }
        Ok(())
    }
    pub(super) fn checked_receipt(
        &self,
    ) -> Result<multiraft_store::ControlledGroupReceipt, ControlledError> {
        let node = self.node()?;
        let receipt = node.controlled_db()?.lifecycle_receipt(self.group)?.ok_or(
            ControlledError::Rejected("durable generation receipt missing"),
        )?;
        if receipt.generation != self.generation || receipt.identity != self.identity {
            return Err(ControlledError::Rejected("stale local generation"));
        }
        Ok(receipt)
    }
}

async fn current_leader<S: StateMachine>(
    raft: &Raft<S>,
    local: NodeId,
) -> Result<openraft::alias::CommittedLeaderIdOf<TypeConfig>, ControlledError> {
    raft.ensure_linearizable(ReadPolicy::ReadIndex)
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))?;
    let metrics = raft.metrics();
    let metrics = metrics.borrow_watched();
    if metrics.current_leader != Some(local) || !metrics.vote.committed {
        return Err(ControlledError::Rejected(
            "local node is not the committed leader",
        ));
    }
    Ok(metrics.vote.leader_id().to_committed())
}
pub(super) async fn checked_executor<S: StateMachine>(
    raft: &Raft<S>,
    sm: &StateMachineStore<S>,
    executor: &ControlledExecutor,
    local: NodeId,
) -> Result<multiraft_core::OperationRecord, ControlledError> {
    if current_leader(raft, local).await? != executor.leader {
        return Err(ControlledError::Rejected("leader incarnation changed"));
    }
    let record = sm
        .controlled_ledger()
        .await?
        .current
        .ok_or(ControlledError::Rejected("no native operation"))?;
    if record.specification.operation != executor.operation
        || record.executor_epoch != executor.epoch
        || record.terminal.is_some()
    {
        return Err(ControlledError::Rejected("stale or terminal executor"));
    }
    Ok(record)
}
async fn membership_guard<S: StateMachine>(
    raft: &Raft<S>,
    executor: &ControlledExecutor,
) -> Result<MembershipChangeGuard<TypeConfig>, ControlledError> {
    let leader = executor.leader;
    raft.with_raft_state(move |state| MembershipChangeGuard {
        expected_leader: leader,
        committed: state.membership_state.committed().as_ref().clone(),
        effective: state.membership_state.effective().as_ref().clone(),
    })
    .await
    .map_err(|error| ControlledError::Native(error.to_string()))
}
async fn write_control<S: StateMachine>(
    raft: &Raft<S>,
    sm: &StateMachineStore<S>,
    leader: openraft::alias::CommittedLeaderIdOf<TypeConfig>,
    command: NativeControl,
) -> Result<NativeLedger, ControlledError> {
    let (responder, receiver) = openraft::impls::ProgressResponder::complete_only();
    raft.write(Request::native_control(command))
        .with_leader(leader)
        .responder(responder)
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))?;
    let response = receiver
        .await
        .map_err(|error| ControlledError::Native(error.to_string()))?
        .map_err(|error| ControlledError::Native(error.to_string()))?;
    let result: ControlReply = serde_json::from_slice(&response.data.effects)
        .map_err(|error| ControlledError::Native(error.to_string()))?;
    match result {
        ControlReply::Applied(_) => Ok(sm.controlled_ledger().await?),
        ControlReply::Rejected(_) => Err(ControlledError::Rejected(
            "native ledger rejected operation or executor",
        )),
    }
}
