//! Provider facts are validated before open/migration can change a namespace.
use multiraft_core::{typ, StartupProvenance};
use multiraft_store::{FileLogStoreOf, SnapshotCatalog};
use openraft::storage::RaftLogStorage;
#[tokio::test]
async fn absence_and_recognized_empty_records_are_pristine_but_any_old_vote_is_persisted() {
    let root = tempfile::tempdir().unwrap();
    let native = root.path().join("native");
    assert_eq!(
        FileLogStoreOf::startup_provenance(&native).unwrap(),
        StartupProvenance::Pristine
    );
    assert!(!native.exists());
    std::fs::create_dir(&native).unwrap();
    std::fs::write(
        native.join("hard_state.json"),
        br#"{"last_purged_log_id":null,"committed":null,"vote":null}"#,
    )
    .unwrap();
    std::fs::write(native.join("log.json"), b"[]").unwrap();
    std::fs::write(native.join("log.ndjson"), b"\n").unwrap();
    std::fs::write(native.join("log.bin"), b"").unwrap();
    assert_eq!(
        FileLogStoreOf::startup_provenance(&native).unwrap(),
        StartupProvenance::Pristine
    );
    let mut store = FileLogStoreOf::open(&native).unwrap();
    store.save_vote(&typ::Vote::new(4, 1)).await.unwrap();
    drop(store);
    assert_eq!(
        FileLogStoreOf::startup_provenance(&native).unwrap(),
        StartupProvenance::Persisted
    );
}
#[test]
fn unknown_or_corrupt_ignored_legacy_records_cannot_be_overwritten_as_pristine() {
    for (name, data) in [
        ("mystery", b"".as_slice()),
        ("hard_state.json", b"broken".as_slice()),
        ("log.json", b"broken".as_slice()),
        ("log.ndjson", b"broken".as_slice()),
        ("log.bin", b"broken".as_slice()),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(name), data).unwrap();
        assert!(
            FileLogStoreOf::startup_provenance(root.path()).is_err(),
            "{name}"
        );
        assert_eq!(std::fs::read(root.path().join(name)).unwrap(), data);
    }
}
#[test]
fn every_group_snapshot_namespace_is_checked_even_without_active_manifest() {
    let root = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(root.path(), 1);
    assert_eq!(
        catalog.startup_provenance(7, 1024).unwrap(),
        StartupProvenance::Pristine
    );
    std::fs::create_dir(root.path().join("7")).unwrap();
    std::fs::write(root.path().join("7").join("mystery"), b"unknown").unwrap();
    assert!(catalog.startup_provenance(7, 1024).is_err());
}
#[cfg(unix)]
#[test]
fn symlinks_cannot_alias_pristine_native_namespaces() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let linked = root.path().join("linked");
    std::os::unix::fs::symlink(real, &linked).unwrap();
    assert!(FileLogStoreOf::startup_provenance(&linked).is_err());
}

#[test]
fn fully_validated_inactive_generation_is_inert_and_never_promoted_to_active() {
    let root = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(root.path(), 1);
    let meta = openraft::alias::SnapshotMetaOf::<multiraft_core::TypeConfig> {
        last_log_id: None,
        last_membership: Default::default(),
        snapshot_id: "inert".to_owned(),
    };
    let staged = catalog.stage_native(7, &meta, b"inert-data", 1024).unwrap();
    drop(staged);
    assert_eq!(
        catalog.startup_provenance(7, 1024).unwrap(),
        StartupProvenance::Pristine
    );
    assert!(catalog.load_native(7, 1024).unwrap().is_none());
    let generation = std::fs::read_dir(root.path().join("7/native-v1"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(generation.join("data.bin"), b"corrupt").unwrap();
    assert!(catalog.startup_provenance(7, 1024).is_err());
}
