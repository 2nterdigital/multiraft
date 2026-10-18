//! Disk-backed state machine mode (`docs/specs/2026-10-18-disk-backed-fsm-mode.md`)
//! and its streaming snapshot hooks (`docs/specs/2026-10-19-disk-fsm-streaming-snapshot.md`).
use crate::{ApplyOut, GroupId};
use std::io;
use std::path::{Path, PathBuf};

/// Raft applied position for one entry, handed to a disk-backed state machine.
///
/// `record` is the bridge's versioned encoding of the applied log id and
/// membership. Persist it verbatim, in the same atomic write as the entry's
/// rows, and return it from [`DiskState::durable_applied`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedState {
    index: u64,
    term: u64,
    record: Vec<u8>,
}

impl AppliedState {
    /// Constructed by the storage bridge; applications only persist it.
    pub fn new(index: u64, term: u64, record: Vec<u8>) -> Self {
        Self {
            index,
            term,
            record,
        }
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn record(&self) -> &[u8] {
        &self.record
    }
}

fn unsupported(operation: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("disk-backed state machine does not support {operation}"),
    )
}

/// Disk-backed mode: the state machine owns its durable store and commits each
/// entry's rows together with the applied position. Restart recovers both from
/// that store; no whole-image restore is involved.
///
/// In this mode the bridge never calls [`crate::StateMachine::apply`],
/// [`crate::StateMachine::snapshot`], [`crate::StateMachine::restore`] or the
/// capture hooks.
///
/// Snapshots (log compaction and catch-up of a lagging replica) use the four
/// directory hooks below. Their defaults refuse, which keeps compaction refused
/// and the log unpurged. `multiraft_store::DiskFsmDb` implements all of them.
pub trait DiskState {
    /// The last record committed by [`Self::apply_durable`], or `None` for a
    /// new store. Called before the first entry is applied.
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>>;

    /// Apply one entry exactly once. `data` is `None` for entries without an
    /// application payload (blank, membership, internal). Before returning,
    /// make the entry's rows and `applied.record()` durable in ONE atomic
    /// write; an error stops the Group and the entry is replayed on restart.
    fn apply_durable(
        &mut self,
        group: GroupId,
        index: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut>;

    /// A directory owned by this Group's store, on the same filesystem as the
    /// live store (checkpoints are hard links and installs are renames). The
    /// bridge keeps published checkpoints, sender leases and received staging
    /// directories there.
    fn snapshot_workspace(&self) -> io::Result<PathBuf> {
        Err(unsupported("snapshots"))
    }

    /// Write a consistent, self-contained copy of the store (rows and applied
    /// record as of the last [`Self::apply_durable`]) into the new directory
    /// `dir`. Called under the bridge's FSM lock, so it must be bounded (a
    /// RocksDB checkpoint is a memtable flush plus hard links). The live store
    /// must be durable up to that position when this returns: the log may be
    /// purged up to it.
    fn checkpoint(&self, dir: &Path) -> io::Result<()> {
        let _ = dir;
        Err(unsupported("snapshots"))
    }

    /// Check that the received checkpoint at `dir` has this store's layout
    /// (format markers, Group, column families) without modifying it or the
    /// live store, and return its applied record.
    fn inspect_checkpoint(&self, dir: &Path) -> io::Result<Option<Vec<u8>>> {
        let _ = dir;
        Err(unsupported("snapshot install"))
    }

    /// Replace the live store with the verified checkpoint at `dir` (consumed)
    /// and reopen it. The replacement must be atomic across crashes: after any
    /// error or crash the store reopens either entirely old or entirely new,
    /// and an error return must leave the old store live (or refuse all later
    /// use).
    fn install_checkpoint(&mut self, dir: &Path) -> io::Result<()> {
        let _ = dir;
        Err(unsupported("snapshot install"))
    }
}
