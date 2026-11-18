//! Fixed RocksDB property collection with explicit CF and DB scopes.
use multiraft_fsm::{ColumnFamilyMemoryStats, StorageMemoryStats};
use rocksdb::DB;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn collect(db: &DB, db_id: String, names: &[String]) -> StorageMemoryStats {
    let mut errors = Vec::new();
    // `None` reads the DB-scoped property, which RocksDB answers from the default CF.
    let mut read = |name: &str, cf: Option<&rocksdb::ColumnFamily>| {
        let result = match cf {
            Some(cf) => db.property_int_value_cf(cf, name),
            None => db.property_int_value(name),
        };
        match result {
            Ok(value) => value,
            Err(error) => {
                errors.push(format!("{name}: {error}"));
                None
            }
        }
    };
    let column_families = names
        .iter()
        .map(|name| {
            // A DB opened without CF descriptors exposes no handle for "default".
            let cf = db.cf_handle(name);
            let mut stats = ColumnFamilyMemoryStats {
                name: name.clone(),
                ..Default::default()
            };
            if cf.is_some() || name == rocksdb::DEFAULT_COLUMN_FAMILY_NAME {
                let active = read("rocksdb.cur-size-active-mem-table", cf);
                let current = read("rocksdb.cur-size-all-mem-tables", cf);
                let all = read("rocksdb.size-all-mem-tables", cf);
                stats.active_memtable_bytes = active;
                stats.all_memtable_bytes = all;
                stats.pinned_memtable_bytes = all
                    .zip(current)
                    .map(|(all, current)| all.saturating_sub(current));
                stats.table_readers_bytes = read("rocksdb.estimate-table-readers-mem", cf);
                stats.live_versions = read("rocksdb.num-live-versions", cf);
                stats.live_sst_bytes = read("rocksdb.live-sst-files-size", cf);
                stats.total_sst_bytes = read("rocksdb.total-sst-files-size", cf);
            }
            stats
        })
        .collect();
    StorageMemoryStats {
        db_id,
        captured_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64,
        column_families,
        num_snapshots: read("rocksdb.num-snapshots", None),
        oldest_snapshot_time: read("rocksdb.oldest-snapshot-time", None),
        num_running_flushes: read("rocksdb.num-running-flushes", None),
        num_running_compactions: read("rocksdb.num-running-compactions", None),
        shared: None,
        errors,
        io_latency: Default::default(),
    }
}
