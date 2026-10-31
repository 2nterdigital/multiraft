//! Native ledger transitions commit in the same durable write as applied position.
use super::*;
use multiraft_core::{
    ControlRejection, ControlReply, NativeControl, NativeLedger, OperationRecord, TerminalProof,
};

impl<S: StateMachine> StateMachineStore<S> {
    /// Read the local durable-applied operation ledger; callers establish ReadIndex separately.
    pub async fn controlled_ledger(&self) -> io::Result<NativeLedger> {
        if !self.disk {
            return Err(unsupported("native operation ledger"));
        }
        let mut inner = self.inner.lock().await;
        inner.load_disk_state()?;
        Ok(inner.control.clone().unwrap_or_default())
    }
}

impl<S: StateMachine> StateMachineStoreInner<S> {
    pub(super) fn control_transition(
        &self,
        command: &NativeControl,
        barrier: LogIdOf<TypeConfig>,
    ) -> (NativeLedger, ControlReply) {
        let mut ledger = self.control.clone().unwrap_or_default();
        let rejection = match command {
            NativeControl::Begin {
                specification,
                executor_epoch,
            } => {
                if let Some(current) = ledger
                    .current
                    .as_ref()
                    .filter(|current| current.specification.operation == specification.operation)
                {
                    if current.specification != *specification {
                        Some(ControlRejection::OperationConflict)
                    } else if current.executor_epoch != *executor_epoch {
                        Some(ControlRejection::StaleExecutor)
                    } else {
                        None
                    }
                } else if ledger
                    .current
                    .as_ref()
                    .is_some_and(|current| current.terminal.is_none())
                {
                    Some(ControlRejection::OperationConflict)
                } else if specification.operation.sequence <= ledger.high_water {
                    Some(ControlRejection::StaleOperation)
                } else if *executor_epoch == 0
                    || specification.target_generation == 0
                    || specification.target_voters.len() != 3
                    || specification
                        .expected_start
                        .membership()
                        .get_joint_config()
                        .len()
                        != 1
                    || specification
                        .expected_start
                        .membership()
                        .voter_ids()
                        .count()
                        != 3
                    || specification
                        .expected_start
                        .membership()
                        .learner_ids()
                        .next()
                        .is_some()
                    || specification
                        .expected_start
                        .membership()
                        .voter_ids()
                        .filter(|id| !specification.target_voters.contains(id))
                        .count()
                        != 1
                {
                    Some(ControlRejection::InvalidArguments)
                } else if specification.expected_start != self.last_membership {
                    Some(ControlRejection::ConfigurationChanged)
                } else {
                    ledger.high_water = specification.operation.sequence;
                    ledger.current = Some(OperationRecord {
                        specification: specification.clone(),
                        executor_epoch: *executor_epoch,
                        terminal: None,
                    });
                    None
                }
            }
            NativeControl::Claim {
                operation,
                executor_epoch,
            } => match ledger.current.as_mut() {
                None => Some(ControlRejection::NoOperation),
                Some(record) if record.specification.operation != *operation => {
                    Some(ControlRejection::OperationConflict)
                }
                Some(record) if record.terminal.is_some() => {
                    Some(ControlRejection::TerminalOperation)
                }
                Some(record) if *executor_epoch < record.executor_epoch || *executor_epoch == 0 => {
                    Some(ControlRejection::StaleExecutor)
                }
                Some(record) => {
                    record.executor_epoch = *executor_epoch;
                    None
                }
            },
            NativeControl::Complete {
                operation,
                executor_epoch,
                target_open,
            } => match ledger.current.as_mut() {
                None => Some(ControlRejection::NoOperation),
                Some(record) if record.specification.operation != *operation => {
                    Some(ControlRejection::OperationConflict)
                }
                Some(record) if record.executor_epoch != *executor_epoch => {
                    Some(ControlRejection::StaleExecutor)
                }
                Some(record) if record.terminal.is_some() => {
                    if record
                        .terminal
                        .as_ref()
                        .is_some_and(|proof| proof.target_open == *target_open)
                    {
                        None
                    } else {
                        Some(ControlRejection::OperationConflict)
                    }
                }
                Some(record) => {
                    let membership = self.last_membership.membership();
                    let actual: std::collections::BTreeSet<_> = membership.voter_ids().collect();
                    let target = record.specification.target_voters.iter().find(|id| {
                        !record
                            .specification
                            .expected_start
                            .membership()
                            .voter_ids()
                            .any(|old| old == **id)
                    });
                    if membership.get_joint_config().len() != 1
                        || actual != record.specification.target_voters
                        || membership.learner_ids().next().is_some()
                    {
                        Some(ControlRejection::IncompleteConfiguration)
                    } else if self.last_membership.log_id().is_none_or(|required| {
                        target_open.applied < required || target_open.applied.index >= barrier.index
                    }) || target_open.operation != *operation
                        || target != Some(&target_open.target_node)
                        || target_open.generation != record.specification.target_generation
                        || target_open.receipt.is_empty()
                    {
                        Some(ControlRejection::TargetNotApplied)
                    } else {
                        record.terminal = Some(TerminalProof {
                            configuration: self.last_membership.clone(),
                            barrier,
                            target_open: target_open.clone(),
                        });
                        None
                    }
                }
            },
        };
        let reply = rejection.map_or_else(
            || ControlReply::Applied(ledger.clone()),
            ControlReply::Rejected,
        );
        (ledger, reply)
    }
}
