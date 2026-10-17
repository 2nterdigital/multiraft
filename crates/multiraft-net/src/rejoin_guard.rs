//! Vote guard for a voter that rejoins an existing Group from wiped storage.
//!
//! A pristine voter that is forbidden to initialize has forgotten its log and its
//! vote. Until it has applied the state the cluster had committed when it
//! rejoined, granting a vote could let a lagging candidate win with entries the
//! wiped voter once acknowledged. While armed, inbound `Vote` requests are dropped
//! (handled like packet loss: no reply, never a grant).
//!
//! Anchor: the `leader_commit` of the first AppendEntries received after startup.
//! An InstallSnapshot raises it to at least the snapshot's last log index. Before
//! any leader message the anchor is unset and every vote is dropped. The guard
//! releases, permanently, once Raft metrics report `applied >= anchor`.
//!
//! The state is three atomics per Group; release is evaluated lazily when a vote
//! arrives (or when owned startup recovery completes), with no background task.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use multiraft_core::typ::LogId;
use multiraft_core::{GroupId, NodeId};
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::async_runtime::WatchReceiver;

/// `anchor` encoding: 0 = unset, otherwise `index + 1`.
const UNSET: u64 = 0;

/// Per-Group inbound vote guard. `Default` is disarmed (never drops votes); only
/// owned startup or the Forbidden facade arm it, before the Group is published.
#[derive(Debug, Default)]
pub struct RejoinGuard {
    armed: AtomicBool,
    anchor: AtomicU64,
}

impl RejoinGuard {
    /// Guard for a Group that never drops votes (Allowed or persisted startup).
    pub(crate) fn disarmed() -> Self {
        Self::default()
    }

    /// Guard for a Forbidden-started pristine voter: drop votes until caught up.
    pub(crate) fn armed() -> Self {
        Self {
            armed: AtomicBool::new(true),
            anchor: AtomicU64::new(UNSET),
        }
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    /// Current anchor index, if a leader message has been observed.
    pub(crate) fn anchor(&self) -> Option<u64> {
        decode(self.anchor.load(Ordering::Acquire))
    }

    /// Record the first observed AppendEntries `leader_commit` (set once).
    pub(crate) fn observe_append(
        &self,
        node: NodeId,
        group: GroupId,
        leader_commit: Option<&LogId>,
    ) {
        if !self.is_armed() {
            return;
        }
        // A leader without a commit carries no recovery target; keep dropping.
        let Some(commit) = leader_commit else {
            return;
        };
        let encoded = commit.index.saturating_add(1);
        if self
            .anchor
            .compare_exchange(UNSET, encoded, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
                phase = "anchor", source = "append_entries", anchor_index = commit.index,
                "rejoin vote guard anchored on first observed leader commit");
        }
    }

    /// An InstallSnapshot is a leader message: its last log index is a lower bound.
    pub(crate) fn observe_snapshot(
        &self,
        node: NodeId,
        group: GroupId,
        last_log_id: Option<&LogId>,
    ) {
        if !self.is_armed() {
            return;
        }
        let Some(last) = last_log_id else {
            return;
        };
        let encoded = last.index.saturating_add(1);
        let previous = self.anchor.fetch_max(encoded, Ordering::AcqRel);
        if previous < encoded {
            tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
                phase = "anchor", source = "install_snapshot", anchor_index = last.index,
                previous_index = decode(previous), "rejoin vote guard anchor raised by snapshot");
        }
    }

    /// Release the guard when `applied >= anchor`; returns whether it is released.
    pub(crate) fn try_release(&self, node: NodeId, group: GroupId, applied: Option<u64>) -> bool {
        if !self.is_armed() {
            return true;
        }
        let Some(anchor) = self.anchor() else {
            return false;
        };
        if applied.is_some_and(|applied| applied >= anchor) {
            if self.armed.swap(false, Ordering::AcqRel) {
                tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
                    phase = "release", anchor_index = anchor, applied_index = applied,
                    "rejoin vote guard released; local state covers the observed leader commit");
            }
            return true;
        }
        false
    }

    /// Whether an inbound vote may reach native Raft. Dropped votes are logged.
    pub(crate) fn admit_vote<S: StateMachine>(&self, group: GroupId, raft: &Raft<S>) -> bool {
        if !self.is_armed() {
            return true;
        }
        let (node, applied) = {
            let metrics = raft.metrics();
            let metrics = metrics.borrow_watched();
            (metrics.id, metrics.last_applied.as_ref().map(|id| id.index))
        };
        if self.try_release(node, group, applied) {
            return true;
        }
        tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
            phase = "drop_vote", anchor_index = self.anchor(), applied_index = applied,
            "rejoin vote guard dropped an inbound vote request");
        false
    }
}

fn decode(value: u64) -> Option<u64> {
    value.checked_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use multiraft_core::TypeConfig;
    use openraft::alias::LeaderIdOf;
    use openraft::vote::RaftLeaderIdExt;

    fn log(index: u64) -> LogId {
        LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index)
    }

    #[test]
    fn disarmed_guard_admits_everything() {
        let guard = RejoinGuard::disarmed();
        assert!(guard.try_release(1, 7, None));
        guard.observe_append(1, 7, Some(&log(5)));
        assert_eq!(guard.anchor(), None);
    }

    #[test]
    fn armed_guard_drops_until_a_leader_message_and_applied_catch_up() {
        let guard = RejoinGuard::armed();
        assert!(!guard.try_release(1, 7, Some(100)), "no leader message yet");
        guard.observe_append(1, 7, None);
        assert!(!guard.try_release(1, 7, Some(100)), "no commit carried");
        guard.observe_append(1, 7, Some(&log(10)));
        guard.observe_append(1, 7, Some(&log(20)));
        assert_eq!(
            guard.anchor(),
            Some(10),
            "first leader commit is the anchor"
        );
        assert!(!guard.try_release(1, 7, None));
        assert!(!guard.try_release(1, 7, Some(9)));
        assert!(guard.try_release(1, 7, Some(10)));
        assert!(!guard.is_armed());
        assert!(guard.try_release(1, 7, None), "release is permanent");
    }

    #[test]
    fn snapshot_raises_anchor_as_a_lower_bound() {
        let guard = RejoinGuard::armed();
        guard.observe_snapshot(1, 7, Some(&log(30)));
        assert_eq!(guard.anchor(), Some(30));
        guard.observe_append(1, 7, Some(&log(12)));
        assert_eq!(guard.anchor(), Some(30), "append never lowers a set anchor");
        guard.observe_snapshot(1, 7, Some(&log(25)));
        assert_eq!(guard.anchor(), Some(30));
        guard.observe_snapshot(1, 7, Some(&log(40)));
        assert_eq!(guard.anchor(), Some(40));
        assert!(!guard.try_release(1, 7, Some(39)));
        assert!(guard.try_release(1, 7, Some(40)));
    }

    #[test]
    fn index_zero_commit_is_distinct_from_unset() {
        let guard = RejoinGuard::armed();
        guard.observe_append(1, 7, Some(&log(0)));
        assert_eq!(guard.anchor(), Some(0));
        assert!(!guard.try_release(1, 7, None));
        assert!(guard.try_release(1, 7, Some(0)));
    }
}
