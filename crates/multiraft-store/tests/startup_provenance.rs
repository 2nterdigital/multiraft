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

#[test]
fn unsupported_hard_state_fields_and_structures_are_refused_without_changing_bytes() {
    for bytes in [
        br#"{"last_purged_log_id":null,"committed":null,"vote":null,"future_vote":{"term":4}}"#.as_slice(),
        br#"{"last_purged_log_id":null,"committed":null,"vote":{"leader_id":{"term":4,"node_id":1,"future":true},"committed":false}}"#.as_slice(),
        br#"{"last_purged_log_id":null,"committed":null,"vote":{"leader_id":{"term":4,"node_id":1},"committed":false,"future":true}}"#.as_slice(),
        br#"{"last_purged_log_id":null,"committed":{"leader_id":{"term":4,"node_id":1},"index":2,"future":true},"vote":null}"#.as_slice(),
        br#"{"last_purged_log_id":null,"committed":null,"vote":null,"vote":null}"#.as_slice(),
        br#"{"last_purged_log_id":null,"committed":null,"vote":{"leader_id":{"term":4,"term":5,"node_id":1},"committed":false}}"#.as_slice(),
        b"[null,null,null]".as_slice(),
    ] {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("hard_state.json");
        std::fs::write(&file, bytes).unwrap();
        let result = FileLogStoreOf::startup_provenance(root.path());
        assert!(result.is_err(), "unsupported hard state accepted: {result:?}");
        assert_eq!(std::fs::read(file).unwrap(), bytes);
    }
}

#[test]
fn strict_provenance_preserves_optional_defaults_and_legacy_open_unknown_field_compatibility() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("hard_state.json");
    std::fs::write(&file, b"{}").unwrap();
    assert_eq!(
        FileLogStoreOf::startup_provenance(root.path()).unwrap(),
        StartupProvenance::Pristine
    );
    let bytes =
        br#"{"last_purged_log_id":null,"committed":null,"vote":null,"future_vote":{"term":4}}"#;
    std::fs::write(&file, bytes).unwrap();
    let legacy = FileLogStoreOf::open(root.path()).unwrap();
    drop(legacy);
    assert_eq!(std::fs::read(file).unwrap(), bytes);
}
