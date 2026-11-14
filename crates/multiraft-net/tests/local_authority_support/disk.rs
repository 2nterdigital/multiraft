use multiraft_fsm::{AppliedState, ApplyOut, DiskState, StateMachine};
use multiraft_store::{rocksdb::WriteBatch, DiskFsmDb, DiskFsmOptions};
use std::{io, path::Path};

pub struct Disk {
    db: DiskFsmDb,
}
impl Disk {
    pub fn open(root: &Path, group: u64) -> io::Result<Self> {
        Ok(Self {
            db: DiskFsmDb::open(
                DiskFsmDb::group_path(root, group),
                group,
                DiskFsmOptions::new(b"local-authority-v1".to_vec()),
            )?,
        })
    }
}
impl StateMachine for Disk {
    type Error = io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        unreachable!("disk apply")
    }
    fn snapshot(&self, _: u64) -> io::Result<Vec<u8>> {
        unreachable!("disk snapshot")
    }
    fn restore(&mut self, _: u64, _: &[u8]) -> io::Result<()> {
        unreachable!("disk restore")
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}
impl DiskState for Disk {
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }
    fn apply_durable(
        &mut self,
        _: u64,
        _: u64,
        _: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        self.db.commit(WriteBatch::default(), applied)?;
        Ok(ApplyOut::default())
    }
}
