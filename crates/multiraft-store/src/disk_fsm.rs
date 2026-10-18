//! Per-Group RocksDB instance for disk-backed state machines.
//!
//! Layout is frozen in `docs/specs/2026-10-18-disk-backed-fsm-mode.md`: one
//! instance per Group at `{data_dir}/fsm-rocksdb/group-{id}/`, separate from the
//! shared Raft-log engine. The reserved `multiraft_meta` column family holds the
//! schema, Group and application-format markers and the applied record; every
//! other column family belongs to the application.
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use multiraft_fsm::{AppliedState, GroupId};
use rocksdb::{ColumnFamily, IteratorMode, Options, WriteBatch, WriteOptions, DB};

/// Reserved column family; applications cannot open or name it.
pub const DISK_FSM_META_CF: &str = "multiraft_meta";
const SCHEMA_KEY: &[u8] = b"schema";
const SCHEMA: &[u8] = b"multiraft-disk-fsm-v1";
const GROUP_KEY: &[u8] = b"group";
const APP_FORMAT_KEY: &[u8] = b"app_format";
const APPLIED_KEY: &[u8] = b"applied";

/// Options for [`DiskFsmDb::open`].
#[derive(Clone)]
pub struct DiskFsmOptions {
    /// Application column families besides `default`; the full set is part of
    /// the on-disk format and must match an existing database exactly.
    pub column_families: Vec<String>,
    /// Application format tag; an existing database with another tag fails.
    pub app_format: Vec<u8>,
    /// Synchronous WAL for every commit (default). `false` still survives a
    /// process kill; after power loss the applied position may move back,
    /// which is safe only while the Raft log keeps the entries after it.
    pub sync_writes: bool,
    /// Base RocksDB tuning (shared cache, write-buffer manager, file limits).
    /// Creation flags are set by `open`.
    pub db_options: Options,
}

impl DiskFsmOptions {
    pub fn new(app_format: impl Into<Vec<u8>>) -> Self {
        Self {
            column_families: Vec::new(),
            app_format: app_format.into(),
            sync_writes: true,
            db_options: Options::default(),
        }
    }
}

/// One Group's FSM database. Dropping it closes the database.
pub struct DiskFsmDb {
    db: DB,
    sync_writes: bool,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn db_error(error: rocksdb::Error) -> io::Error {
    io::Error::other(error)
}

impl DiskFsmDb {
    /// Canonical location of `group`'s FSM database under a node data directory.
    pub fn group_path(data_dir: impl AsRef<Path>, group: GroupId) -> PathBuf {
        data_dir
            .as_ref()
            .join("fsm-rocksdb")
            .join(format!("group-{group}"))
    }

    /// Open or create the database, failing closed on any marker or column
    /// family mismatch, or on a nonempty directory that is not a database.
    pub fn open(
        path: impl AsRef<Path>,
        group: GroupId,
        options: DiskFsmOptions,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let mut names = BTreeSet::from(["default".to_owned(), DISK_FSM_META_CF.to_owned()]);
        for name in &options.column_families {
            if name.is_empty() || name == DISK_FSM_META_CF {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid application column family",
                ));
            }
            names.insert(name.clone());
        }
        let existing = path.join("CURRENT").try_exists()?;
        if !existing {
            match std::fs::read_dir(path) {
                Ok(mut entries) => {
                    if entries.next().transpose()?.is_some() {
                        return Err(invalid("nonempty FSM directory without a database"));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        let mut db_options = options.db_options;
        db_options.create_if_missing(!existing);
        db_options.create_missing_column_families(!existing);
        if existing {
            let found: BTreeSet<String> = DB::list_cf(&db_options, path)
                .map_err(db_error)?
                .into_iter()
                .collect();
            if found != names {
                return Err(invalid(
                    "FSM column families differ from the declared format",
                ));
            }
        }
        let db = DB::open_cf(&db_options, path, &names).map_err(db_error)?;
        let store = Self {
            db,
            sync_writes: options.sync_writes,
        };
        store.check_markers(group, &options.app_format, &names)?;
        Ok(store)
    }

    fn check_markers(
        &self,
        group: GroupId,
        app_format: &[u8],
        names: &BTreeSet<String>,
    ) -> io::Result<()> {
        let meta = self.meta()?;
        let group_bytes = group.to_be_bytes();
        match self.db.get_cf(meta, SCHEMA_KEY).map_err(db_error)? {
            Some(schema) if schema == SCHEMA => {
                let stored_group = self.db.get_cf(meta, GROUP_KEY).map_err(db_error)?;
                if stored_group.as_deref() != Some(&group_bytes[..]) {
                    return Err(invalid("FSM database belongs to another group"));
                }
                let stored_format = self.db.get_cf(meta, APP_FORMAT_KEY).map_err(db_error)?;
                if stored_format.as_deref() != Some(app_format) {
                    return Err(invalid("FSM database has another application format"));
                }
                Ok(())
            }
            Some(_) => Err(invalid("unknown disk FSM schema")),
            None => {
                // Only a database left empty by an interrupted creation may be initialized.
                for name in names {
                    let cf = self
                        .db
                        .cf_handle(name)
                        .ok_or_else(|| invalid("missing FSM column family"))?;
                    if let Some(row) = self.db.iterator_cf(cf, IteratorMode::Start).next() {
                        row.map_err(db_error)?;
                        return Err(invalid("missing disk FSM schema in nonempty database"));
                    }
                }
                let mut batch = WriteBatch::default();
                batch.put_cf(meta, SCHEMA_KEY, SCHEMA);
                batch.put_cf(meta, GROUP_KEY, group_bytes);
                batch.put_cf(meta, APP_FORMAT_KEY, app_format);
                let mut write = WriteOptions::default();
                write.set_sync(true);
                self.db.write_opt(batch, &write).map_err(db_error)
            }
        }
    }

    fn meta(&self) -> io::Result<&ColumnFamily> {
        self.db
            .cf_handle(DISK_FSM_META_CF)
            .ok_or_else(|| invalid("missing FSM meta column family"))
    }

    /// Read access. All writes must go through [`Self::commit`].
    pub fn db(&self) -> &DB {
        &self.db
    }

    /// An application column family declared at open (or `default`).
    pub fn cf(&self, name: &str) -> io::Result<&ColumnFamily> {
        if name == DISK_FSM_META_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reserved FSM column family",
            ));
        }
        self.db
            .cf_handle(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown FSM column family"))
    }

    /// The record stored by the last [`Self::commit`], for [`multiraft_fsm::DiskState::durable_applied`].
    pub fn applied_record(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.get_cf(self.meta()?, APPLIED_KEY).map_err(db_error)
    }

    /// Write `batch` and `applied` in one atomic RocksDB write.
    pub fn commit(&self, mut batch: WriteBatch, applied: &AppliedState) -> io::Result<()> {
        batch.put_cf(self.meta()?, APPLIED_KEY, applied.record());
        let mut write = WriteOptions::default();
        write.set_sync(self.sync_writes);
        self.db.write_opt(batch, &write).map_err(db_error)
    }
}
