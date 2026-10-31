//! Proof-driven terminal recovery never needs an executor token or a new native mutation.
use super::*;
use crate::controlled_startup::{
    StartupAuthorityResult, StartupCatalog, StartupTargetGeneration, StartupTerminalProof,
};
use multiraft_core::OperationRecord;

impl<S: StateMachine> ControlledGroup<S> {
    /// Resolve local retirement permission from this data space's retained completion authority.
    ///
    /// This is read-only: it opens no core, performs no ReadIndex, and changes neither catalog
    /// nor lifecycle state. The configured authority must authenticate and bind its retained
    /// facts to this runtime's data space. Decoded RPC catalogs are never accepted here.
    /// The returned token is permission only; [`Self::retire`] establishes actual local stop.
    pub async fn retirement_from_authority(
        &self,
        operation: OperationId,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledTerminal, ControlledError> {
        let catalog = tokio::time::timeout_at(deadline, self.authority.read(self.group))
            .await
            .map_err(|_| ControlledError::Deadline {
                outcome_unknown: false,
            })??;
        let terminal = retirement_permit(self, operation, &catalog)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(ControlledError::Deadline {
                outcome_unknown: false,
            });
        }
        Ok(terminal)
    }
    /// Reconcile an immutable native terminal or retained completed handoff after a crash.
    /// A new ReadIndex and durable native high-water mark precede sealing cleanup evidence.
    /// The configured startup authority must bind catalog and completed facts to this
    /// runtime's data space; the high-water mark does not authenticate another catalog.
    pub async fn reconcile_terminal(
        &self,
        operation: OperationId,
        deadline: tokio::time::Instant,
    ) -> Result<ControlledTerminal, ControlledError> {
        let this = self.clone();
        self.node()?
            .controlled_run(self.group, deadline, move |node| async move {
                this.checked_active()?;
                let (raft, sm) = node.controlled_parts(this.group)?;
                raft.ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map_err(|error| ControlledError::Native(error.to_string()))?;
                let ledger = sm.controlled_ledger().await?;
                if ledger.high_water < operation.sequence {
                    return Err(ControlledError::Rejected(
                        "native high-water mark does not cover the terminal",
                    ));
                }
                let record = if let Some(record) = ledger
                    .current
                    .filter(|record| record.specification.operation == operation)
                {
                    record
                } else {
                    this.authority
                        .read(this.group)
                        .await?
                        .completed
                        .into_iter()
                        .find(|proof| proof.native_record.specification.operation == operation)
                        .map(|proof| proof.native_record)
                        .ok_or(ControlledError::Rejected(
                            "retained terminal proof is unavailable",
                        ))?
                };
                handoff(&this, record).await
            })
            .await
    }
}

pub(super) async fn handoff<S: StateMachine>(
    this: &ControlledGroup<S>,
    record: OperationRecord,
) -> Result<ControlledTerminal, ControlledError> {
    let (source, proof) = validate_native_terminal(&record)?;
    let catalog = this.authority.read(this.group).await?;
    let source_generation = catalog
        .active_operation
        .as_ref()
        .filter(|binding| binding.specification == record.specification)
        .map(|binding| binding.source_generation)
        .or_else(|| {
            catalog
                .completed
                .iter()
                .find(|completed| completed.native_record == record)
                .map(|completed| completed.source_generation)
        })
        .ok_or(ControlledError::Rejected("source generation proof missing"))?;
    let handoff = StartupTerminalProof {
        source_generation,
        group_id: this.group,
        native_record: record.clone(),
        committed_membership: proof.configuration.clone(),
        applied_membership: proof.configuration.clone(),
        applied_log: proof.barrier,
        target_opened: StartupTargetGeneration {
            node_id: proof.target_open.target_node,
            generation: proof.target_open.generation,
            consumption_id: proof.target_open.receipt.clone(),
        },
    };
    match this.authority.terminal_handoff(handoff.clone()).await? {
        StartupAuthorityResult::Applied(actual) if actual == handoff => {}
        StartupAuthorityResult::Unknown
            if this
                .authority
                .read(this.group)
                .await?
                .completed
                .contains(&handoff) => {}
        _ => {
            return Err(ControlledError::Rejected(
                "terminal startup handoff is unconfirmed",
            ))
        }
    }
    Ok(ControlledTerminal {
        group: this.group,
        source,
        source_generation,
        fingerprint: this.node()?.config.raft_transport_fingerprint,
        proof,
        specification: record.specification,
    })
}

/// Validate one complete native replacement record without consulting a local core or catalog.
fn validate_native_terminal(
    record: &OperationRecord,
) -> Result<(NodeId, TerminalProof), ControlledError> {
    let proof = record
        .terminal
        .clone()
        .ok_or(ControlledError::Rejected("native terminal proof missing"))?;
    let specification = &record.specification;
    let starting = specification.expected_start.membership();
    let old: BTreeSet<_> = starting.voter_ids().collect();
    let source: Vec<_> = old
        .difference(&specification.target_voters)
        .copied()
        .collect();
    let target: Vec<_> = specification
        .target_voters
        .difference(&old)
        .copied()
        .collect();
    let members = proof.configuration.membership();
    let start_log = specification
        .expected_start
        .log_id()
        .ok_or(ControlledError::Rejected(
            "native starting configuration identity missing",
        ))?;
    let configuration_log = proof
        .configuration
        .log_id()
        .ok_or(ControlledError::Rejected(
            "native terminal configuration identity missing",
        ))?;
    if specification.operation.sequence == 0
        || record.executor_epoch == 0
        || specification.target_generation == 0
        || old.len() != 3
        || starting.get_joint_config().len() != 1
        || starting.learner_ids().next().is_some()
        || old.iter().any(|node| starting.get_node(node).is_none())
        || source.len() != 1
        || target != [proof.target_open.target_node]
        || specification.target_voters.len() != 3
        || members.get_joint_config().len() != 1
        || members.voter_ids().collect::<BTreeSet<_>>() != specification.target_voters
        || members.learner_ids().next().is_some()
        || members.get_node(&source[0]).is_some()
        || specification
            .target_voters
            .iter()
            .any(|node| members.get_node(node).is_none())
        || proof.target_open.operation != specification.operation
        || proof.target_open.generation != specification.target_generation
        || proof.target_open.receipt.is_empty()
        || !super::seed::covers(configuration_log, start_log)
        || configuration_log.index <= start_log.index
        || !super::seed::covers(proof.target_open.applied, configuration_log)
        || !super::seed::covers(proof.barrier, proof.target_open.applied)
        || proof.barrier.index <= proof.target_open.applied.index
    {
        return Err(ControlledError::Rejected(
            "native terminal identity or application proof mismatch",
        ));
    }
    Ok((source[0], proof))
}

fn checked_completed(
    catalog: &StartupCatalog,
    group: GroupId,
    operation: OperationId,
) -> Result<(&StartupTerminalProof, NodeId, TerminalProof), ControlledError> {
    if catalog.group_id != group {
        return Err(ControlledError::Rejected(
            "retirement catalog group mismatch",
        ));
    }
    let mut matches = catalog
        .completed
        .iter()
        .filter(|completed| completed.native_record.specification.operation == operation);
    let completed = matches.next().ok_or(ControlledError::Rejected(
        "retained completed operation missing",
    ))?;
    if matches.next().is_some() {
        return Err(ControlledError::Rejected(
            "ambiguous retained completed operation",
        ));
    }
    let (source, proof) = validate_native_terminal(&completed.native_record)?;
    let final_log = proof
        .configuration
        .log_id()
        .ok_or(ControlledError::Rejected(
            "completed configuration identity missing",
        ))?;
    let current_log = catalog
        .membership
        .log_id()
        .ok_or(ControlledError::Rejected(
            "catalog configuration identity missing",
        ))?;
    if completed.group_id != group
        || completed.source_generation == 0
        || completed.committed_membership != proof.configuration
        || completed.applied_membership != proof.configuration
        || completed.applied_log != proof.barrier
        || completed.target_opened
            != (StartupTargetGeneration {
                node_id: proof.target_open.target_node,
                generation: proof.target_open.generation,
                consumption_id: proof.target_open.receipt.clone(),
            })
        || !super::seed::covers(current_log, final_log)
        || (current_log == final_log && catalog.membership != proof.configuration)
    {
        return Err(ControlledError::Rejected(
            "retained completed native facts are inconsistent",
        ));
    }
    Ok((completed, source, proof))
}

fn retirement_permit<S: StateMachine>(
    this: &ControlledGroup<S>,
    operation: OperationId,
    catalog: &StartupCatalog,
) -> Result<ControlledTerminal, ControlledError> {
    let node = this.node()?;
    let receipt = this.checked_receipt()?;
    this.storage_owner()?;
    let (completed, source, proof) = checked_completed(catalog, this.group, operation)?;
    let proof_bytes = serde_json::to_vec(&proof).map_err(std::io::Error::other)?;
    let receipt_matches = match receipt.stage {
        multiraft_store::ControlledGroupStage::Opened => receipt.terminal_proof.is_none(),
        multiraft_store::ControlledGroupStage::Retired
        | multiraft_store::ControlledGroupStage::LogsCleaned
        | multiraft_store::ControlledGroupStage::Cleaned => {
            receipt.terminal_proof.as_ref() == Some(&proof_bytes)
        }
        _ => false,
    };
    if source != node.node_id
        || completed.source_generation != this.generation
        || !receipt_matches
        || catalog.authorized.get(&node.node_id) == Some(&this.generation)
        || catalog.active_operation.as_ref().is_some_and(|binding| {
            binding.group_id != this.group
                || (binding.source == node.node_id && binding.source_generation == this.generation)
                || (binding.target.node_id == node.node_id
                    && binding.target.generation == this.generation)
        })
    {
        return Err(ControlledError::Rejected(
            "completed authority does not retire this local generation",
        ));
    }
    Ok(ControlledTerminal {
        group: this.group,
        source,
        source_generation: this.generation,
        fingerprint: node.config.raft_transport_fingerprint,
        proof,
        specification: completed.native_record.specification.clone(),
    })
}

pub(super) async fn recheck_retirement<S: StateMachine>(
    this: &ControlledGroup<S>,
    terminal: &ControlledTerminal,
) -> Result<(), ControlledError> {
    let catalog = this.authority.read(this.group).await?;
    let actual = retirement_permit(this, terminal.specification.operation, &catalog)?;
    if actual.group != terminal.group
        || actual.source != terminal.source
        || actual.source_generation != terminal.source_generation
        || actual.fingerprint != terminal.fingerprint
        || actual.proof != terminal.proof
        || actual.specification != terminal.specification
    {
        return Err(ControlledError::Rejected(
            "retirement permission differs from retained authority",
        ));
    }
    Ok(())
}
