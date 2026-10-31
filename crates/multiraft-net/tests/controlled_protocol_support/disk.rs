//! Opaque application bytes persisted atomically with the native applied record.
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, DiskStoragePaths, GroupId, StateMachine};
use multiraft_store::{rocksdb::WriteBatch, DiskFsmDb, DiskFsmOptions};
use std::{
    io,
    path::{Path, PathBuf},
};

pub struct DiskImage {
    db: DiskFsmDb,
    gate_root: PathBuf,
}

impl DiskImage {
    pub fn open(root: &Path, group: GroupId) -> io::Result<Self> {
        Ok(Self {
            db: DiskFsmDb::open(
                DiskFsmDb::group_path(root, group),
                group,
                DiskFsmOptions::new(b"controlled-protocol-opaque-v1".to_vec()),
            )?,
            gate_root: root.join("application-gate"),
        })
    }

    pub fn payloads(&self) -> io::Result<Vec<Vec<u8>>> {
        let cf = self.db.cf("default")?;
        self.db
            .db()
            .get_cf(cf, b"payloads")
            .map_err(io::Error::other)?
            .map_or(Ok(Vec::new()), |bytes| {
                serde_json::from_slice(&bytes).map_err(io::Error::other)
            })
    }
}

impl StateMachine for DiskImage {
    type Error = io::Error;

    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        Err(io::Error::other("disk image requires durable apply"))
    }

    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        Err(io::Error::other("disk image requires checkpoint"))
    }

    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        Err(io::Error::other("disk image requires checkpoint install"))
    }

    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}

impl DiskState for DiskImage {
    fn storage_paths(&self) -> io::Result<DiskStoragePaths> {
        self.db.storage_paths()
    }
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }

    fn apply_durable(
        &mut self,
        _: GroupId,
        _: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        if data == Some(b"pending-opaque-application".as_slice())
            && self.gate_root.join("armed").exists()
        {
            std::fs::write(
                self.gate_root.join("entered"),
                b"application callback entered before atomic durable apply",
            )?;
            let limit = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !self.gate_root.join("release").exists() {
                if std::time::Instant::now() >= limit {
                    return Err(io::Error::other(
                        "bounded application test gate was not released",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        let mut batch = WriteBatch::default();
        if let Some(data) = data {
            let mut payloads = self.payloads()?;
            payloads.push(data.to_vec());
            batch.put_cf(
                self.db.cf("default")?,
                b"payloads",
                serde_json::to_vec(&payloads).map_err(io::Error::other)?,
            );
        }
        self.db.commit(batch, applied)?;
        Ok(ApplyOut::default())
    }

    fn snapshot_workspace(&self) -> io::Result<PathBuf> {
        self.db.snapshot_workspace()
    }
    fn checkpoint(&self, dir: &Path) -> io::Result<()> {
        self.db.checkpoint(dir)
    }
    fn inspect_checkpoint(&self, dir: &Path) -> io::Result<Option<Vec<u8>>> {
        self.db.inspect_checkpoint(dir)
    }
    fn install_checkpoint(&mut self, dir: &Path) -> io::Result<()> {
        self.db.install_checkpoint(dir)
    }
}
