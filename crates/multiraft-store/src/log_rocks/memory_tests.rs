//! Engine resource ownership and effective options, independent of group count.
use super::*;
use multiraft_core::TypeConfig;

#[test]
fn group_handles_share_engine_resources_and_effective_options() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open_with_sync_all(root.path(), true).unwrap();
    let first = db.group(1).unwrap();
    let second = db.group(2).unwrap();
    assert!(Arc::ptr_eq(&first.owner.engine, &second.owner.engine));
    let before = db.storage_memory_stats().unwrap();
    let after = first.owner.storage_memory_stats().unwrap();
    assert_eq!(before.db_id, after.db_id);
    // Empty errors: capacity was read back from RocksDB, not the fallback.
    assert!(before.errors.is_empty(), "{:?}", before.errors);
    let shared = before.shared.unwrap();
    let other = after.shared.unwrap();
    assert_eq!(shared.cache_id, other.cache_id);
    assert_eq!(
        shared.write_buffer_manager_id,
        other.write_buffer_manager_id
    );
    assert_eq!(shared.cache_capacity_bytes, 32 * 1024 * 1024);
    assert_eq!(shared.write_buffer_manager_limit_bytes, 64 * 1024 * 1024);
    // The persisted OPTIONS file records effective DB and default-CF settings.
    let options = std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("OPTIONS-"))
        .unwrap();
    let text = std::fs::read_to_string(options.path()).unwrap();
    for expected in [
        "write_buffer_size=16777216",
        "max_write_buffer_number=2",
        "max_open_files=64",
        "bytes_per_sync=1048576",
        "use_fsync=true",
    ] {
        assert!(
            text.contains(expected),
            "missing effective option {expected}"
        );
    }
    drop(db);
    assert!(
        first.owner.storage_memory_stats().unwrap().column_families[0]
            .active_memtable_bytes
            .is_some()
    );
}

#[test]
fn old_default_options_schema_reopens_with_bounded_resources() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = DB::open_default(root.path()).unwrap();
        db.put(SCHEMA_KEY, SCHEMA).unwrap();
    }
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    assert!(db.group(7).is_ok());
    assert_eq!(
        db.storage_memory_stats().unwrap().column_families[0].name,
        "default"
    );
}
