//! [`multiraft_fsm::DiskState`] snapshot hooks for [`DiskFsmDb`]: checkpoint,
//! read-only inspection of a received checkpoint, and crash-safe install.
use super::{check_markers, column_families, db_error, invalid, meta_cf, DiskFsmDb, APPLIED_KEY};
use crate::disk_snapshot::install_dir;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::DB;
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

impl DiskFsmDb {
    /// Snapshot workspace of the store at `path`: the sibling `{name}.snapshots`
    /// directory, on the same filesystem as the store.
    pub fn workspace_path(path: &Path) -> io::Result<PathBuf> {
        let name = path
            .file_name()
            .ok_or_else(|| invalid("FSM path has no directory name"))?
            .to_string_lossy()
            .into_owned();
        Ok(path.with_file_name(format!("{name}.snapshots")))
    }

    /// For [`multiraft_fsm::DiskState::snapshot_workspace`].
    pub fn snapshot_workspace(&self) -> io::Result<PathBuf> {
        Self::workspace_path(&self.path)
    }

    /// For [`multiraft_fsm::DiskState::checkpoint`]: sync the WAL (the log may
    /// be purged up to this position), then write a RocksDB checkpoint (a
    /// memtable flush plus hard links) into the new directory `dir`.
    pub fn checkpoint(&self, dir: &Path) -> io::Result<()> {
        let db = self.live()?;
        db.flush_wal(true).map_err(db_error)?;
        Checkpoint::new(db)
            .map_err(db_error)?
            .create_checkpoint(dir)
            .map_err(db_error)
    }

    /// For [`multiraft_fsm::DiskState::inspect_checkpoint`]: open `dir`
    /// read-only, require this store's column families and markers, and
    /// return its applied record.
    pub fn inspect_checkpoint(&self, dir: &Path) -> io::Result<Option<Vec<u8>>> {
        let names = column_families(&self.options)?;
        let mut options = self.options.db_options.clone();
        options.create_if_missing(false);
        options.create_missing_column_families(false);
        if !dir.join("CURRENT").try_exists()? {
            return Err(invalid("received checkpoint is not a database"));
        }
        let found: BTreeSet<String> = DB::list_cf(&options, dir)
            .map_err(db_error)?
            .into_iter()
            .collect();
        if found != names {
            return Err(invalid(
                "received checkpoint column families differ from the declared format",
            ));
        }
        let db = DB::open_cf_for_read_only(&options, dir, &names, false).map_err(db_error)?;
        check_markers(&db, self.group, &self.options.app_format, &names, true)?;
        db.get_cf(meta_cf(&db)?, APPLIED_KEY).map_err(db_error)
    }

    /// For [`multiraft_fsm::DiskState::install_checkpoint`]: close the live
    /// store, swap `dir` in (see [`crate::disk_snapshot::install_dir`]) and
    /// reopen. Any failure puts the old store back and reopens it.
    pub fn install_checkpoint(&mut self, dir: &Path) -> io::Result<()> {
        self.inspect_checkpoint(dir)?;
        let workspace = self.snapshot_workspace()?;
        drop(self.db.take());
        let swapped = match install_dir(&self.path, &workspace, dir) {
            Ok(swapped) => swapped,
            Err(error) => {
                self.db = Some(Self::open_db(&self.path, self.group, &self.options)?);
                return Err(error);
            }
        };
        match Self::open_db(&self.path, self.group, &self.options) {
            Ok(db) => {
                self.db = Some(db);
                if let Err(error) = swapped.commit() {
                    // Only garbage remains; the next open removes it.
                    tracing::warn!(error = %error, "failed to remove the retired FSM store");
                }
                Ok(())
            }
            Err(error) => {
                swapped.rollback()?;
                self.db = Some(Self::open_db(&self.path, self.group, &self.options)?);
                Err(error)
            }
        }
    }
}
