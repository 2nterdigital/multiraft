//! Vote and election guard for a voter that rejoins a Group from wiped storage.
//!
//! A pristine voter that is forbidden to initialize has forgotten its log and its
//! vote. Until it has applied the state the cluster had committed when it
//! rejoined, two things are unsafe:
//! - granting a vote could let a lagging candidate win with entries the wiped
//!   voter once acknowledged, so inbound `Vote` requests are dropped (packet
//!   loss: no reply, never a grant);
//! - campaigning itself, e.g. after a snapshot gave it membership but before it
//!   has the committed tail, so native elections are disabled
//!   (`RuntimeConfig::elect(false)`) from installation until release.
//!
//! Anchor (per-term freeze): from inbound AppendEntries, the first non-`None`
//! `leader_commit` seen in a leader term fixes the anchor for that term; later
//! AppendEntries of the same or a lower term never move it, so it does not chase
//! the commit under load. An AppendEntries of a strictly higher term raises it to
//! that term's first `leader_commit` (taking the maximum with the previous value:
//! both are committed indexes, so the bound never drops). A deposed leader's lower
//! term therefore cannot fix a stale target once the current leader is heard. An
//! InstallSnapshot's last log index is a lower bound. Before any leader message
//! the anchor is unset and every vote is dropped.
//!
//! Release is permanent and happens exactly once, when Raft metrics report
//! `applied >= anchor` (anchor read at check time); it re-enables elections. It
//! is evaluated by the Group's watcher task (applied changes and anchor raises),
//! by owned startup, and lazily when a vote arrives. Cost while armed: an atomic
//! flag plus a small mutex on the AppendEntries path; after release, one load.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use multiraft_core::typ::LogId;
use multiraft_core::{GroupId, NodeId};
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::async_runtime::WatchReceiver;
use tokio::sync::{watch, Notify};

type ElectToggle = Box<dyn Fn(bool) + Send + Sync>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Anchor {
    /// Leader term whose first `leader_commit` fixed `commit`.
    term: Option<u64>,
    commit: Option<u64>,
    /// Snapshot lower bound.
    floor: Option<u64>,
}

impl Anchor {
    fn index(&self) -> Option<u64> {
        self.commit.max(self.floor)
    }
}

/// Per-Group inbound vote/election guard. `Default` is disarmed (never drops
/// votes); only owned startup or the Forbidden facade arm it, before the Group
/// is published.
pub struct RejoinGuard {
    armed: AtomicBool,
    anchor: Mutex<Anchor>,
    /// Wakes the release watcher when the anchor moves.
    raised: Notify,
    released: watch::Sender<bool>,
    elect: Mutex<Option<ElectToggle>>,
}

impl Default for RejoinGuard {
    fn default() -> Self {
        Self::disarmed()
    }
}

impl std::fmt::Debug for RejoinGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RejoinGuard")
            .field("armed", &self.is_armed())
            .field("anchor", &self.anchor())
            .finish()
    }
}

impl RejoinGuard {
    fn with_armed(armed: bool) -> Self {
        Self {
            armed: AtomicBool::new(armed),
            anchor: Mutex::new(Anchor::default()),
            raised: Notify::new(),
            released: watch::Sender::new(!armed),
            elect: Mutex::new(None),
        }
    }

    /// Guard for a Group that never drops votes (Allowed or persisted startup).
    pub(crate) fn disarmed() -> Self {
        Self::with_armed(false)
    }

    /// Guard for a Forbidden-started pristine voter: drop votes until caught up.
    pub(crate) fn armed() -> Self {
        Self::with_armed(true)
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    /// Install the election toggle: disables elections now when armed and
    /// re-enables them exactly once on release. No-op when disarmed.
    pub(crate) fn install_elect(&self, toggle: impl Fn(bool) + Send + Sync + 'static) {
        let mut slot = self.elect.lock().unwrap();
        if self.is_armed() {
            toggle(false);
            *slot = Some(Box::new(toggle));
        }
    }

    /// Install native election control for `raft` (see [`Self::install_elect`]).
    pub(crate) fn install_raft<S: StateMachine>(&self, raft: &Raft<S>) {
        let raft = raft.clone();
        self.install_elect(move |enabled| raft.runtime_config().elect(enabled));
    }

    /// Current anchor index, if a leader message has been observed.
    pub(crate) fn anchor(&self) -> Option<u64> {
        self.anchor.lock().unwrap().index()
    }

    pub(crate) fn released(&self) -> watch::Receiver<bool> {
        self.released.subscribe()
    }

    /// AppendEntries from a leader of `term`: per-term freeze of `leader_commit`.
    pub(crate) fn observe_append(
        &self,
        node: NodeId,
        group: GroupId,
        term: u64,
        leader_commit: Option<&LogId>,
    ) {
        if !self.is_armed() {
            return;
        }
        // A leader without a commit carries no recovery target; keep dropping.
        let Some(commit) = leader_commit else {
            return;
        };
        let raised = {
            let mut anchor = self.anchor.lock().unwrap();
            if anchor.term.is_some_and(|fixed| term <= fixed) {
                return;
            }
            let before = *anchor;
            anchor.term = Some(term);
            anchor.commit = anchor.commit.max(Some(commit.index));
            (before.index() != anchor.index()).then_some((before, *anchor))
        };
        tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
            phase = "anchor_term", leader_term = term, leader_commit_index = commit.index,
            "rejoin vote guard fixed the anchor for a new leader term");
        if let Some((before, after)) = raised {
            self.log_raise(node, group, "append_entries", before, after);
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
        let raised = {
            let mut anchor = self.anchor.lock().unwrap();
            let before = *anchor;
            anchor.floor = anchor.floor.max(Some(last.index));
            (before.index() != anchor.index()).then_some((before, *anchor))
        };
        if let Some((before, after)) = raised {
            self.log_raise(node, group, "install_snapshot", before, after);
        }
    }

    fn log_raise(&self, node: NodeId, group: GroupId, source: &str, before: Anchor, after: Anchor) {
        tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
            phase = "anchor", source, anchor_index = after.index(), anchor_term = after.term,
            previous_index = before.index(), "rejoin vote guard anchor raised");
        self.raised.notify_one();
    }

    /// Release when `applied >= anchor` (anchor read now). Returns whether the
    /// guard is released. The transition happens once: it re-enables elections,
    /// publishes `released` and drops the election toggle.
    pub(crate) fn try_release(&self, node: NodeId, group: GroupId, applied: Option<u64>) -> bool {
        if !self.is_armed() {
            return true;
        }
        let Some(anchor) = self.anchor() else {
            return false;
        };
        if !applied.is_some_and(|applied| applied >= anchor) {
            return false;
        }
        if self.armed.swap(false, Ordering::AcqRel) {
            if let Some(toggle) = self.elect.lock().unwrap().take() {
                toggle(true);
            }
            self.released.send_replace(true);
            tracing::debug!(target: "multiraft::rejoin_guard", node_id = node, group_id = group,
                phase = "release", anchor_index = anchor, applied_index = applied,
                "rejoin vote guard released; elections re-enabled");
        }
        true
    }

    /// [`Self::try_release`] against this Group's current Raft metrics.
    pub(crate) fn try_release_raft<S: StateMachine>(&self, group: GroupId, raft: &Raft<S>) -> bool {
        if !self.is_armed() {
            return true;
        }
        let (node, applied) = {
            let metrics = raft.metrics();
            let metrics = metrics.borrow_watched();
            (metrics.id, metrics.last_applied.as_ref().map(|id| id.index))
        };
        self.try_release(node, group, applied)
    }

    /// Bounded release driver owned by the Group: ends on release or when the
    /// native metrics channel closes (Group shutdown).
    pub(crate) async fn watch_release<S: StateMachine>(&self, group: GroupId, raft: Raft<S>) {
        let mut metrics = raft.metrics();
        loop {
            if self.try_release_raft(group, &raft) {
                return;
            }
            tokio::select! {
                changed = metrics.changed() => if changed.is_err() { return; },
                _ = self.raised.notified() => {},
            }
        }
    }

    /// Whether an inbound vote may reach native Raft. Dropped votes are logged.
    pub(crate) fn admit_vote<S: StateMachine>(&self, group: GroupId, raft: &Raft<S>) -> bool {
        if self.try_release_raft(group, raft) {
            return true;
        }
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched();
        tracing::debug!(target: "multiraft::rejoin_guard", node_id = metrics.id, group_id = group,
            phase = "drop_vote", anchor_index = self.anchor(),
            applied_index = metrics.last_applied.as_ref().map(|id| id.index),
            "rejoin vote guard dropped an inbound vote request");
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multiraft_core::TypeConfig;
    use openraft::alias::LeaderIdOf;
    use openraft::vote::RaftLeaderIdExt;
    use std::sync::Arc;

    fn log(index: u64) -> LogId {
        LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index)
    }

    fn recorded(guard: &RejoinGuard) -> Arc<Mutex<Vec<bool>>> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sink = calls.clone();
        guard.install_elect(move |enabled| sink.lock().unwrap().push(enabled));
        calls
    }

    #[test]
    fn disarmed_guard_admits_everything_and_never_touches_elect() {
        let guard = RejoinGuard::disarmed();
        let calls = recorded(&guard);
        assert!(guard.try_release(1, 7, None));
        guard.observe_append(1, 7, 3, Some(&log(5)));
        assert_eq!(guard.anchor(), None);
        assert!(*guard.released().borrow());
        assert!(calls.lock().unwrap().is_empty());
    }

    #[test]
    fn armed_guard_drops_until_a_leader_message_and_applied_catch_up() {
        let guard = RejoinGuard::armed();
        let calls = recorded(&guard);
        assert_eq!(*calls.lock().unwrap(), vec![false], "elections disabled");
        assert!(!guard.try_release(1, 7, Some(100)), "no leader message yet");
        guard.observe_append(1, 7, 3, None);
        assert!(!guard.try_release(1, 7, Some(100)), "no commit carried");
        guard.observe_append(1, 7, 3, Some(&log(10)));
        assert_eq!(guard.anchor(), Some(10));
        assert!(!guard.try_release(1, 7, None));
        assert!(!guard.try_release(1, 7, Some(9)));
        assert!(!*guard.released().borrow());
        assert!(guard.try_release(1, 7, Some(10)));
        assert!(!guard.is_armed());
        assert!(*guard.released().borrow());
        assert!(guard.try_release(1, 7, Some(10)), "release is permanent");
        assert_eq!(
            *calls.lock().unwrap(),
            vec![false, true],
            "elections re-enabled exactly once"
        );
    }

    #[test]
    fn anchor_is_frozen_within_a_term_so_it_does_not_chase_the_commit() {
        let guard = RejoinGuard::armed();
        guard.observe_append(1, 7, 3, Some(&log(10)));
        guard.observe_append(1, 7, 3, Some(&log(25)));
        guard.observe_append(1, 7, 3, Some(&log(40)));
        assert_eq!(guard.anchor(), Some(10), "same term: first commit is fixed");
        assert!(guard.try_release(1, 7, Some(10)));
    }

    #[test]
    fn higher_term_raises_anchor_and_lower_term_never_moves_it() {
        let guard = RejoinGuard::armed();
        // A deposed leader (term 3) is heard first with a stale commit.
        guard.observe_append(1, 7, 3, Some(&log(10)));
        assert_eq!(guard.anchor(), Some(10));
        // The current leader (term 5): its first commit fixes the new target.
        guard.observe_append(1, 7, 5, Some(&log(25)));
        guard.observe_append(1, 7, 5, Some(&log(30)));
        assert_eq!(guard.anchor(), Some(25));
        assert!(
            !guard.try_release(1, 7, Some(10)),
            "applied at the stale commit must not release"
        );
        // Lower-term messages never move it.
        guard.observe_append(1, 7, 3, Some(&log(99)));
        guard.observe_append(1, 7, 4, Some(&log(12)));
        assert_eq!(guard.anchor(), Some(25));
        // A newer term whose first commit is lower never lowers the bound.
        guard.observe_append(1, 7, 6, Some(&log(20)));
        assert_eq!(guard.anchor(), Some(25));
        assert!(!guard.try_release(1, 7, Some(24)));
        assert!(guard.try_release(1, 7, Some(25)));
        guard.observe_append(1, 7, 9, Some(&log(99)));
        assert_eq!(guard.anchor(), Some(25), "ignored once released");
    }

    #[test]
    fn snapshot_is_a_lower_bound() {
        let guard = RejoinGuard::armed();
        guard.observe_snapshot(1, 7, Some(&log(30)));
        assert_eq!(guard.anchor(), Some(30));
        guard.observe_append(1, 7, 3, Some(&log(12)));
        assert_eq!(guard.anchor(), Some(30), "a lower commit never lowers it");
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
        guard.observe_append(1, 7, 1, Some(&log(0)));
        assert_eq!(guard.anchor(), Some(0));
        assert!(!guard.try_release(1, 7, None));
        assert!(guard.try_release(1, 7, Some(0)));
    }
}
