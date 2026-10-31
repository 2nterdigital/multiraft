//! Durable generation fencing through the public shared RocksDB engine.
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::TypeConfig;
use multiraft_store::RocksLogDb;
use openraft::alias::{LeaderIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::RaftLeaderIdExt;
use openraft::RaftLogReader;

#[tokio::test]
async fn retirement_fences_existing_handles_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut legacy = db.group(7).unwrap();
    let mut sibling = db.group(8).unwrap();
    let vote = VoteOf::<TypeConfig>::new(4, 1);
    sibling.save_vote(&vote).await.unwrap();
    let log_id = |index| LogId::new(LeaderIdOf::<TypeConfig>::new_committed(4, 1), index);
    sibling
        .blocking_append((0..3).map(|index| Entry::new_blank(log_id(index))))
        .await
        .unwrap();
    sibling.save_committed(Some(log_id(1))).await.unwrap();
    sibling.purge(log_id(0)).await.unwrap();
    db.enroll_controlled(7, 1, b"cutover").unwrap();
    assert!(legacy.read_vote().await.is_err());
    assert!(db.group(7).is_err());
    let mut old = db.group_generation(7, 1).unwrap();
    old.save_vote(&vote).await.unwrap();
    db.retire(7, 1, b"terminal-config").unwrap();
    assert!(old.read_vote().await.is_err());
    assert!(old.try_get_log_entries(..).await.is_err());
    assert!(old.blocking_append(Vec::new()).await.is_err());
    assert!(old.save_vote(&vote).await.is_err());
    assert!(old.read_committed().await.is_err());
    assert!(old.get_log_state().await.is_err());
    assert!(old.save_committed(None).await.is_err());
    assert!(old.truncate_after(None).await.is_err());
    assert!(old.purge(log_id(0)).await.is_err());
    assert!(db.cleanup_group(7, 1, b"wrong-config").is_err());
    db.cleanup_group(7, 1, b"terminal-config").unwrap();
    db.cleanup_group(7, 1, b"terminal-config").unwrap();
    drop((old, legacy, sibling, db));
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    assert!(db.group(7).is_err());
    assert!(db.group_generation(7, 1).is_err());
    let mut sibling = db.group(8).unwrap();
    assert_eq!(sibling.read_vote().await.unwrap(), Some(vote));
    assert_eq!(sibling.read_committed().await.unwrap(), Some(log_id(1)));
    assert_eq!(
        sibling.get_log_state().await.unwrap().last_purged_log_id,
        Some(log_id(0))
    );
    assert_eq!(
        sibling
            .try_get_log_entries(..)
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.log_id)
            .collect::<Vec<_>>(),
        vec![log_id(1), log_id(2)]
    );
}

#[tokio::test]
async fn rejoin_waits_for_native_cleanup_and_old_cleanup_cannot_delete_new_logs() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    db.enroll_controlled(7, 1, b"cutover").unwrap();
    let mut old = db.group_generation(7, 1).unwrap();
    db.retire(7, 1, b"terminal").unwrap();
    db.cleanup_group(7, 1, b"terminal").unwrap();
    assert!(db.begin_rejoin(7, 1, 2, b"new-op").is_err());
    db.finish_cleanup(7, 1, b"terminal").unwrap();
    db.finish_cleanup(7, 1, b"terminal").unwrap();
    db.begin_rejoin(7, 1, 2, b"new-op").unwrap();
    assert!(db.group_generation(7, 2).is_err());
    db.advance_controlled(
        7,
        2,
        b"new-op",
        serde_json::from_str("\"Reserved\"").unwrap(),
        serde_json::from_str("\"Opening\"").unwrap(),
    )
    .unwrap();
    let mut new = db.group_generation(7, 2).unwrap();
    let vote = VoteOf::<TypeConfig>::new(9, 1);
    new.save_vote(&vote).await.unwrap();
    assert!(db.cleanup_group(7, 1, b"terminal").is_err());
    assert!(old.save_vote(&vote).await.is_err());
    assert_eq!(new.read_vote().await.unwrap(), Some(vote));
    assert!(db.begin_rejoin(7, 1, 2, b"another-op").is_err());
}

#[test]
fn admission_receipts_replay_across_restart_without_reopening_reserved_groups() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let reserved = db.reserve_controlled(9, 1, b"open-op").unwrap();
    assert!(db.group_generation(9, 1).is_err());
    assert!(db
        .advance_controlled(
            9,
            1,
            b"wrong-op",
            reserved.stage,
            serde_json::from_str("\"Opening\"").unwrap()
        )
        .is_err());
    let opening = db
        .advance_controlled(
            9,
            1,
            b"open-op",
            reserved.stage,
            serde_json::from_str("\"Opening\"").unwrap(),
        )
        .unwrap();
    let opened = db
        .advance_controlled(
            9,
            1,
            b"open-op",
            opening.stage,
            serde_json::from_str("\"Opened\"").unwrap(),
        )
        .unwrap();
    drop(db);
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    assert_eq!(db.reserve_controlled(9, 1, b"open-op").unwrap(), opened);
    assert_eq!(db.lifecycle_receipt(9).unwrap(), Some(opened));
    assert!(db.reserve_controlled(9, 1, b"different-op").is_err());
}

#[test]
fn old_engine_schema_refuses_controlled_databases_even_after_log_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    db.enroll_controlled(3, 1, b"cutover").unwrap();
    db.retire(3, 1, b"terminal").unwrap();
    db.cleanup_group(3, 1, b"terminal").unwrap();
    drop(db);
    // This is the exact schema acceptance condition of the released v1 opener.
    let old_engine = multiraft_store::rocksdb::DB::open_default(root.path()).unwrap();
    assert_ne!(
        old_engine.get(b"schema").unwrap().unwrap(),
        b"multiraft-rocks-log-v1"
    );
    drop(old_engine);
    assert!(RocksLogDb::<TypeConfig>::open(root.path())
        .unwrap()
        .group(3)
        .is_err());
}

fn group_key(tag: u8, group: u64) -> Vec<u8> {
    let mut key = vec![tag];
    key.extend_from_slice(&group.to_be_bytes());
    key
}

#[test]
fn missing_registry_or_metadata_never_recreates_a_controlled_generation() {
    for missing in *b"xcm" {
        let root = tempfile::tempdir().unwrap();
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        db.enroll_controlled(7, 1, b"cutover").unwrap();
        drop(db);
        let raw = multiraft_store::rocksdb::DB::open_default(root.path()).unwrap();
        let key = if missing == b'm' {
            let mut key = group_key(b'g', 7);
            key.push(b'm');
            key
        } else {
            group_key(missing, 7)
        };
        raw.delete(key).unwrap();
        raw.flush_wal(true).unwrap();
        drop(raw);
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        assert!(db.group(7).is_err(), "missing marker {missing}");
        assert!(
            db.group_generation(7, 1).is_err(),
            "missing marker {missing}"
        );
        assert!(
            db.startup_provenance(7).is_err(),
            "missing marker {missing}"
        );
    }
}

#[tokio::test]
async fn partial_retired_namespace_deletion_can_resume_without_touching_sibling() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    db.enroll_controlled(7, 1, b"cutover").unwrap();
    let mut sibling = db.group(u64::MAX).unwrap();
    let vote = VoteOf::<TypeConfig>::new(10, 1);
    sibling.save_vote(&vote).await.unwrap();
    db.retire(7, 1, b"terminal").unwrap();
    drop((sibling, db));
    let raw = multiraft_store::rocksdb::DB::open_default(root.path()).unwrap();
    let mut metadata_key = group_key(b'g', 7);
    metadata_key.push(b'm');
    raw.delete(metadata_key).unwrap();
    raw.flush_wal(true).unwrap();
    drop(raw);
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    assert!(db.group_generation(7, 1).is_err());
    db.cleanup_group(7, 1, b"terminal").unwrap();
    db.finish_cleanup(7, 1, b"terminal").unwrap();
    drop(db);
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    assert!(db.group(7).is_err());
    assert_eq!(
        db.group(u64::MAX).unwrap().read_vote().await.unwrap(),
        Some(vote)
    );
    db.begin_rejoin(7, 1, 2, b"new-op").unwrap();
}
