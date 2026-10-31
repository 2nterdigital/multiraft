//! Native ledger transitions commit in the same durable write as applied position.
use super::*;
use multiraft_core::{
    ControlRejection, ControlReply, NativeControl, NativeLedger, OperationRecord,
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
    ) -> (NativeLedger, ControlReply) {
        let mut ledger = self.control.clone().unwrap_or_default();
        let rejection = match command {
            NativeControl::Begin {
                specification,
                executor_epoch,
            } => {
                if let Some(current) = &ledger.current {
                    if current.specification.operation == specification.operation {
                        if current.specification != *specification {
                            Some(ControlRejection::OperationConflict)
                        } else if current.executor_epoch != *executor_epoch {
                            Some(ControlRejection::StaleExecutor)
                        } else {
                            None
                        }
                    } else {
                        Some(ControlRejection::OperationConflict)
                    }
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
                Some(record) if *executor_epoch < record.executor_epoch || *executor_epoch == 0 => {
                    Some(ControlRejection::StaleExecutor)
                }
                Some(record) => {
                    record.executor_epoch = *executor_epoch;
                    None
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
