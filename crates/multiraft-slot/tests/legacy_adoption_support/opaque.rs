//! Opaque compatibility factory preserves actual OLD business CFs without interpreting commands.
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, DiskStoragePaths, StateMachine};
use multiraft_slot::{
    adoption::{AdoptionEvidence, UpgradeAuthorization},
    SlotLocalIdentity, SlotStartupDisposition, StrictSlotStartup, TrustedSlotDirectory,
};
use multiraft_store::{
    rocksdb::{IteratorMode, Options, WriteBatch, DB},
    DiskFsmDb, DiskFsmOptions,
};
use sha2::{Digest, Sha256};
use std::{
    io,
    path::{Path, PathBuf},
};
pub struct OpaqueLegacyFsm {
    db: DiskFsmDb,
}
impl OpaqueLegacyFsm {
    pub fn open(root: &Path, group: u64) -> io::Result<Self> {
        let path = DiskFsmDb::group_path(root, group);
        if !path.join("CURRENT").is_file() {
            return Err(io::Error::other(
                "test factory refuses missing original FSM",
            ));
        }
        let mut options = DiskFsmOptions::new(b"ech0-slot-state-v1".to_vec());
        options.column_families = vec!["slot_state".into()];
        Ok(Self {
            db: DiskFsmDb::open(path, group, options)?,
        })
    }
}
impl StateMachine for OpaqueLegacyFsm {
    type Error = io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        Err(io::Error::other("disk mode only"))
    }
    fn snapshot(&self, _: u64) -> io::Result<Vec<u8>> {
        Err(io::Error::other("checkpoint mode only"))
    }
    fn restore(&mut self, _: u64, _: &[u8]) -> io::Result<()> {
        Err(io::Error::other("no whole-image restore"))
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}
impl DiskState for OpaqueLegacyFsm {
    fn storage_paths(&self) -> io::Result<DiskStoragePaths> {
        self.db.storage_paths()
    }
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }
    fn apply_durable(
        &mut self,
        _: u64,
        _: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        if data.is_some() {
            return Err(io::Error::other(
                "opaque compatibility test refuses every business command",
            ));
        }
        self.db.commit(WriteBatch::default(), applied)?;
        Ok(ApplyOut::default())
    }
    fn snapshot_workspace(&self) -> io::Result<PathBuf> {
        self.db.snapshot_workspace()
    }
    fn checkpoint(&self, path: &Path) -> io::Result<()> {
        self.db.checkpoint(path)
    }
    fn inspect_checkpoint(&self, path: &Path) -> io::Result<Option<Vec<u8>>> {
        self.db.inspect_checkpoint(path)
    }
    fn install_checkpoint(&mut self, path: &Path) -> io::Result<()> {
        self.db.install_checkpoint(path)
    }
}
pub fn authorization(e: &AdoptionEvidence) -> UpgradeAuthorization {
    let identity = SlotLocalIdentity {
        data_space_id: e.data_space_id(),
        data_dir_uuid: [23; 16],
        protocol_version: 3,
        compatibility_digest: [42; 32],
    };
    let startup = StrictSlotStartup {
        disposition: SlotStartupDisposition::RecoverExisting,
        identity: identity.clone(),
        directory: TrustedSlotDirectory {
            version: 1,
            data_space_id: e.data_space_id(),
            genesis_digest: e.layout().digest(),
            nodes: e.layout().config().nodes.clone(),
        },
    };
    UpgradeAuthorization {
        session_id: [1; 16],
        authorization_id: [2; 16],
        cold_manifest_sha256: [3; 32],
        physical_domain: [4; 32],
        root: e.root().to_owned(),
        node_id: e.node_id(),
        data_space_id: e.data_space_id(),
        groups: e.replicas().iter().map(|r| r.group).collect(),
        old_layout_digest: e.layout().digest().0,
        inventory_digest: e.inventory_digest(),
        per_directory_identity_digest: e.per_directory_identity_digest(&identity).unwrap(),
        startup,
    }
}
/// Independent raw-row oracle includes live stores and all original published checkpoints.
pub fn business_payloads(root: &Path) -> (String, u64) {
    fn databases(path: &Path, output: &mut Vec<PathBuf>) {
        if path.join("CURRENT").is_file() {
            output.push(path.to_owned());
            return;
        }
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                databases(&entry.path(), output);
            }
        }
    }
    let mut paths = Vec::new();
    databases(&root.join("fsm-rocksdb"), &mut paths);
    paths.sort();
    let mut hash = Sha256::new();
    let mut count = 0;
    for path in paths {
        let options = Options::default();
        let names = DB::list_cf(&options, &path).unwrap();
        let db = DB::open_cf_for_read_only(&options, &path, names, false).unwrap();
        let cf = db.cf_handle("slot_state").unwrap();
        let relative = path
            .strip_prefix(root)
            .unwrap()
            .to_str()
            .unwrap()
            .as_bytes();
        for row in db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = row.unwrap();
            for bytes in [relative, key.as_ref(), value.as_ref()] {
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
            }
            count += 1;
        }
    }
    (format!("{:x}", hash.finalize()), count)
}
