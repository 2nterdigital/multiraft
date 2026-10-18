//! Disk-backed FSM mode: the applied position lives in the application's store.
use super::*;
use multiraft_fsm::AppliedState;
use openraft::storage::{RaftLogReader, RaftLogStorage};
use serde::{Deserialize, Serialize};

const RECORD_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppliedRecord {
    version: u32,
    last_applied: LogIdOf<TypeConfig>,
    membership: StoredMembershipOf<TypeConfig>,
}

pub(super) fn unsupported(operation: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("disk-backed state machine has no whole-image {operation}"),
    )
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}

impl<S: StateMachine> StateMachineStoreInner<S> {
    /// Seed the applied position and membership from the FSM's store, once.
    pub(super) fn load_disk_state(&mut self) -> io::Result<()> {
        if self.disk_loaded {
            return Ok(());
        }
        let disk = self
            .fsm
            .disk_state()
            .ok_or_else(|| invalid("state machine left disk-backed mode"))?;
        if let Some(bytes) = disk.durable_applied()? {
            let record: AppliedRecord =
                serde_json::from_slice(&bytes).map_err(|error| invalid(&error.to_string()))?;
            if record.version != RECORD_VERSION {
                return Err(invalid("unknown disk applied record version"));
            }
            if record.membership.log_id().as_ref() > Some(&record.last_applied) {
                return Err(invalid(
                    "disk membership is newer than its applied position",
                ));
            }
            self.last_applied_log = Some(record.last_applied);
            self.last_membership = record.membership;
        }
        self.disk_loaded = true;
        Ok(())
    }

    /// Apply one entry and its applied record in one FSM write, then advance.
    pub(super) fn apply_disk_entry(
        &mut self,
        entry: &DefaultEntryOf<TypeConfig>,
    ) -> io::Result<Response> {
        self.load_disk_state()?;
        let expected = self
            .last_applied_log
            .map_or(Some(0), |id| id.index.checked_add(1));
        if expected != Some(entry.log_id.index) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "disk application log gap: expected {expected:?}, got {}",
                    entry.log_id.index
                ),
            ));
        }
        let (data, membership) = match &entry.payload {
            EntryPayload::Blank => (None, self.last_membership.clone()),
            EntryPayload::Normal(req) if is_standby_snapshot_trigger(&req.data) => {
                (None, self.last_membership.clone())
            }
            EntryPayload::Normal(req) => (Some(req.data.as_slice()), self.last_membership.clone()),
            EntryPayload::Membership(mem) => (
                None,
                StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id), mem.clone()),
            ),
        };
        let record = AppliedRecord {
            version: RECORD_VERSION,
            last_applied: entry.log_id,
            membership,
        };
        let applied = AppliedState::new(
            entry.log_id.index,
            entry.log_id.committed_leader_id().term,
            serde_json::to_vec(&record).map_err(io::Error::other)?,
        );
        let group = self.group_id;
        let disk = self
            .fsm
            .disk_state()
            .ok_or_else(|| invalid("state machine left disk-backed mode"))?;
        let out = disk.apply_durable(group, entry.log_id.index, data, &applied)?;
        // Durable now: only then advance the in-memory view and answer.
        self.last_applied_log = Some(record.last_applied);
        self.last_membership = record.membership;
        Ok(if data.is_some() {
            Response::new(out.effects)
        } else {
            Response::none()
        })
    }
}

impl<S: StateMachine> StateMachineStore<S> {
    /// Whether the FSM selected disk-backed mode at construction.
    pub fn is_disk_backed(&self) -> bool {
        self.disk
    }

    /// Disk mode only: load the FSM's applied position and fail closed when
    /// it exists but `log` has no persisted basis (the directories differ).
    pub async fn check_disk_log_basis<L: RaftLogStorage<TypeConfig>>(
        &self,
        log: &mut L,
    ) -> io::Result<()> {
        if !self.disk {
            return Ok(());
        }
        let applied = {
            let mut inner = self.inner.lock().await;
            inner.load_disk_state()?;
            inner.last_applied_log
        };
        let Some(applied) = applied else {
            return Ok(());
        };
        let state = log.get_log_state().await?;
        let vote = log.get_log_reader().await.read_vote().await?;
        if state.last_log_id.is_none() && state.last_purged_log_id.is_none() && vote.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "disk FSM applied index {} has no persisted Raft log basis",
                    applied.index
                ),
            ));
        }
        Ok(())
    }
}
