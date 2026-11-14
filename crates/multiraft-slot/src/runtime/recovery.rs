//! Source recovery never admits the preference bootstrap path.
use crate::{ClusterLayout, RuntimeError, SlotStateMachine};
use multiraft_core::StartupProvenance;
use multiraft_net::{
    GroupStartupReport, InitializeDisposition, RuntimeHandle, StartupGroup, StartupPhase,
    StartupReport,
};
use std::time::Duration;
use tokio::time::Instant;

/// Same operational cause bound as the native preference startup adapter.
const MAX_CAUSE_BYTES: usize = 2048;

pub(super) async fn recover_sources<S: SlotStateMachine>(
    native: &RuntimeHandle<S>,
    layout: &ClusterLayout,
    groups: Vec<StartupGroup>,
    timeout: Duration,
    deadline: Instant,
) -> Result<StartupReport, RuntimeError> {
    let mut report = StartupReport {
        input_digest: Some(layout.digest().0),
        node_id: native.node_id(),
        configuration_verified: false,
        groups: Vec::new(),
    };
    for input in groups {
        let group_id = input.group.group_id;
        let bound = deadline.min(Instant::now() + timeout);
        if let Err(error) = native.recover_existing_group(input.group, bound).await {
            let mut cause = error.to_string();
            // Keep operational errors bounded, including errors originating in application callbacks.
            if cause.len() > MAX_CAUSE_BYTES {
                let mut end = MAX_CAUSE_BYTES;
                while !cause.is_char_boundary(end) {
                    end -= 1;
                }
                cause.truncate(end);
            }
            return Err(RuntimeError::StartupFailed {
                slot: layout.slot_for_group(group_id).ok(),
                phase: "persisted_recovery".into(),
                outcome_unknown: true,
                cause,
            });
        }
        report.groups.push(GroupStartupReport {
            group_id,
            node_id: native.node_id(),
            preferred_initializer: None,
            // Persisted-only admission established this before any factory/native construction.
            provenance: Some(StartupProvenance::Persisted),
            registered_at: None,
            deadline: Some(bound),
            phase: StartupPhase::Ready,
            initialization: InitializeDisposition::NotDispatched,
            fallback: false,
        });
    }
    Ok(report)
}
