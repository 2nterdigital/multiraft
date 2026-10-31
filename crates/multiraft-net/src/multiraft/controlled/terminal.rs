//! Proof-driven terminal recovery never needs an executor token or a new native mutation.
use super::*;
use crate::controlled_startup::{
    StartupAuthorityResult, StartupTargetGeneration, StartupTerminalProof,
};
use multiraft_core::OperationRecord;

impl<S: StateMachine> ControlledGroup<S> {
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
    let proof = record
        .terminal
        .clone()
        .ok_or(ControlledError::Rejected("native terminal proof missing"))?;
    let specification = &record.specification;
    let old: BTreeSet<_> = specification
        .expected_start
        .membership()
        .voter_ids()
        .collect();
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
    let configuration_log = proof
        .configuration
        .log_id()
        .ok_or(ControlledError::Rejected(
            "native terminal configuration identity missing",
        ))?;
    if source.len() != 1
        || target != [proof.target_open.target_node]
        || specification.target_voters.len() != 3
        || members.get_joint_config().len() != 1
        || members.voter_ids().collect::<BTreeSet<_>>() != specification.target_voters
        || members.learner_ids().next().is_some()
        || proof.target_open.operation != specification.operation
        || proof.target_open.generation != specification.target_generation
        || proof.target_open.receipt.is_empty()
        || proof.target_open.applied < configuration_log
        || proof.target_open.applied.index < configuration_log.index
        || proof.target_open.applied >= proof.barrier
        || proof.target_open.applied.index >= proof.barrier.index
    {
        return Err(ControlledError::Rejected(
            "native terminal identity or application proof mismatch",
        ));
    }
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
        source: source[0],
        source_generation,
        fingerprint: this.node()?.config.raft_transport_fingerprint,
        proof,
        specification: record.specification,
    })
}
