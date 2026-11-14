//! A creation fact is complete only after every owned Group has applied its committed genesis.
use crate::{ClusterLayout, ObservedLogId, RuntimeError, SlotStateMachine};
use multiraft_net::{RuntimeHandle, StartupReport};
use std::{collections::BTreeSet, convert::Infallible, time::Duration};
use tokio::time::Instant;

/// Local samples are cheap and bounded; this interval yields while replication catches up.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
/// Keep the same operational cause limit as other Slot startup failures.
const MAX_CAUSE_BYTES: usize = 2048;

pub(super) async fn wait_for_basis<S: SlotStateMachine>(
    native: &RuntimeHandle<S>,
    layout: &ClusterLayout,
    startup: &StartupReport,
    deadline: Instant,
) -> Result<(), RuntimeError> {
    for group in &startup.groups {
        let slot = layout
            .slot_for_group(group.group_id)
            .map_err(|_| RuntimeError::InvalidConfig)?;
        let expected: BTreeSet<_> = layout
            .voters(slot)
            .map_err(|_| RuntimeError::InvalidConfig)?
            .iter()
            .copied()
            .collect();
        let bound = group.deadline.unwrap_or(deadline).min(deadline);
        let mut basis_failure = None;
        loop {
            let failure = |cause: &str| RuntimeError::StartupFailed {
                slot: Some(slot),
                phase: "genesis_basis".into(),
                outcome_unknown: true,
                cause: cause.to_owned(),
            };
            if Instant::now() >= bound {
                return Err(failure(basis_failure.as_deref().unwrap_or("local committed and applied genesis membership was not observed before the startup deadline")));
            }
            let (sample, _) = native
                .observe_group(group.group_id, bound)
                .await
                .map_err(|_| failure("native genesis membership source unavailable"))?;
            if let Some(membership) = sample.committed_membership.log_id {
                if sample.committed_membership.voter_configs != [expected.clone()]
                    || !sample.committed_membership.learner_ids.is_empty()
                {
                    return Err(failure(
                        "native committed membership disagrees with the authorized genesis",
                    ));
                }
                let view = native
                    .try_read_applied_with_watermark(group.group_id, bound, |_| {
                        Ok::<(), Infallible>(())
                    })
                    .await
                    .map_err(|_| failure("native genesis applied source unavailable"))?;
                if view
                    .applied
                    .is_some_and(|applied| covers(applied, membership))
                {
                    match native
                        .validate_local_recovery_basis(group.group_id, bound)
                        .await
                    {
                        Ok(()) if Instant::now() < bound => break,
                        Ok(()) => {}
                        Err(error) => {
                            let mut cause = format!("local durable genesis recovery basis unavailable before startup deadline: {error}");
                            if cause.len() > MAX_CAUSE_BYTES {
                                let mut end = MAX_CAUSE_BYTES;
                                while !cause.is_char_boundary(end) {
                                    end -= 1;
                                }
                                cause.truncate(end);
                            }
                            basis_failure = Some(cause);
                        }
                    }
                }
            }
            tokio::time::sleep_until(bound.min(Instant::now() + SAMPLE_INTERVAL)).await;
        }
    }
    Ok(())
}
fn covers(applied: ObservedLogId, membership: ObservedLogId) -> bool {
    applied.index > membership.index || applied == membership
}
