//! Shared-engine durability, namespace isolation, and recovery corruption boundaries.
use std::io::ErrorKind;

use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{StartupProvenance, TypeConfig};
use multiraft_store::RocksLogDb;
use openraft::alias::{LeaderIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::RaftLeaderIdExt;
use openraft::RaftLogReader;

fn id(term: u64, index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(term, 1), index)
}
fn entries(term: u64, range: std::ops::Range<u64>) -> Vec<Entry> {
    range
        .map(|index| Entry::new_blank(id(term, index)))
        .collect()
}
fn key(group: u64, tag: u8, index: Option<u64>) -> Vec<u8> {
    let mut key = vec![b'g'];
    key.extend_from_slice(&group.to_be_bytes());
    key.push(tag);
    if let Some(index) = index {
        key.extend_from_slice(&index.to_be_bytes());
    }
    key
}
fn mutate(path: &std::path::Path, operation: impl FnOnce(&rocksdb::DB)) {
    let db = rocksdb::DB::open_default(path).unwrap();
    operation(&db);
    db.flush_wal(true).unwrap();
}

#[tokio::test]
async fn three_groups_share_one_engine_and_survive_clone_drop_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let clone = db.clone();
    let mut a = db.group(7).unwrap();
    let mut b = db.group(8).unwrap();
    let mut c = db.group(u64::MAX).unwrap();
    assert_eq!(
        db.startup_provenance(7).unwrap(),
        StartupProvenance::Pristine
    );
    a.blocking_append(entries(1, 0..4)).await.unwrap();
    b.blocking_append(entries(2, 0..3)).await.unwrap();
    c.blocking_append(entries(3, 0..2)).await.unwrap();
    let c_bytes = db.retained_log_bytes(u64::MAX).unwrap();
    let vote = VoteOf::<TypeConfig>::new(4, 1);
    a.save_vote(&vote).await.unwrap();
    a.save_committed(Some(id(1, 2))).await.unwrap();
    b.truncate_after(Some(id(2, 0))).await.unwrap();
    b.blocking_append(entries(5, 1..3)).await.unwrap();
    a.purge(id(1, 1)).await.unwrap();
    assert_eq!(clone.retained_log_bytes(u64::MAX).unwrap(), c_bytes);
    drop(a);
    drop(db);
    drop(clone);
    assert_eq!(c.try_get_log_entries(..).await.unwrap().len(), 2);
    drop(b);
    drop(c);
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut a = db.group(7).unwrap();
    assert_eq!(a.read_vote().await.unwrap(), Some(vote));
    assert_eq!(a.read_committed().await.unwrap(), Some(id(1, 2)));
    assert_eq!(
        a.get_log_state().await.unwrap().last_purged_log_id,
        Some(id(1, 1))
    );
    assert_eq!(
        a.try_get_log_entries(0..100)
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.log_id.index())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let mut b = db.group(8).unwrap();
    assert_eq!(
        b.try_get_log_entries(1..3).await.unwrap()[0].log_id,
        id(5, 1)
    );
    assert_eq!(
        db.startup_provenance(7).unwrap(),
        StartupProvenance::Persisted
    );
    assert_eq!(
        db.startup_provenance(99).unwrap(),
        StartupProvenance::Pristine
    );
}

#[tokio::test]
async fn committed_and_purged_basis_cannot_be_replaced_or_truncated() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(1).unwrap();
    assert!(store.save_committed(Some(id(1, 0))).await.is_err());
    store.blocking_append(entries(1, 0..4)).await.unwrap();
    store.save_committed(Some(id(1, 1))).await.unwrap();
    assert!(store.truncate_after(None).await.is_err());
    assert!(store.truncate_after(Some(id(1, 0))).await.is_err());
    assert!(store.truncate_after(Some(id(2, 1))).await.is_err());
    assert!(store.blocking_append(entries(2, 1..3)).await.is_err());
    assert!(store.save_committed(Some(id(1, 4))).await.is_err());
    assert!(store.save_committed(Some(id(2, 1))).await.is_err());
    assert!(store.save_committed(None).await.is_err());
    store.truncate_after(Some(id(1, 1))).await.unwrap();
    store.blocking_append(entries(2, 2..4)).await.unwrap();
    store.purge(id(1, 1)).await.unwrap();
    assert!(store.truncate_after(None).await.is_err());
    assert!(store.purge(id(2, 1)).await.is_err());
    assert!(store.blocking_append(entries(3, 1..2)).await.is_err());
    assert_eq!(store.try_get_log_entries(0..100).await.unwrap().len(), 2);
}

#[tokio::test]
async fn retained_bytes_follow_atomic_append_suffix_and_prefix_changes() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(1).unwrap();
    let batch = entries(1, 0..4);
    let sizes: Vec<_> = batch
        .iter()
        .map(|entry| serde_json::to_vec(entry).unwrap().len() as u64)
        .collect();
    store.blocking_append(batch).await.unwrap();
    assert_eq!(db.retained_log_bytes(1).unwrap(), sizes.iter().sum::<u64>());
    store.truncate_after(Some(id(1, 2))).await.unwrap();
    assert_eq!(
        db.retained_log_bytes(1).unwrap(),
        sizes[..3].iter().sum::<u64>()
    );
    store.purge(id(1, 1)).await.unwrap();
    assert_eq!(db.retained_log_bytes(1).unwrap(), sizes[2]);
    store.purge(id(1, 2)).await.unwrap();
    assert_eq!(db.retained_log_bytes(1).unwrap(), 0);
    assert_eq!(
        db.startup_provenance(1).unwrap(),
        StartupProvenance::Persisted
    );
    assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    store.blocking_append(entries(2, 3..5)).await.unwrap();
}

#[tokio::test]
async fn empty_append_completes_and_invalid_append_does_not_mutate() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(1).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.blocking_append(Vec::<Entry>::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        db.startup_provenance(1).unwrap(),
        StartupProvenance::Pristine
    );
    assert!(store
        .blocking_append(vec![Entry::new_blank(id(1, 0)), Entry::new_blank(id(1, 2))])
        .await
        .is_err());
    assert_eq!(db.retained_log_bytes(1).unwrap(), 0);
    store.blocking_append(entries(1, 0..3)).await.unwrap();
    assert!(store.try_get_log_entries(10..20).await.unwrap().is_empty());
    assert!(store.try_get_log_entries(0..0).await.unwrap().is_empty());
    assert_eq!(
        store
            .try_get_log_entries((std::ops::Bound::Excluded(0), std::ops::Bound::Included(2)))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn gaps_mismatched_log_ids_and_missing_markers_fail_recovery() {
    for corruption in 0..5 {
        let root = tempfile::tempdir().unwrap();
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        let mut store = db.group(1).unwrap();
        store.blocking_append(entries(1, 0..4)).await.unwrap();
        drop(store);
        drop(db);
        mutate(root.path(), |db| match corruption {
            0 => db.delete(key(1, b'e', Some(1))).unwrap(),
            1 => db
                .put(
                    key(1, b'e', Some(1)),
                    serde_json::to_vec(&Entry::new_blank(id(1, 2))).unwrap(),
                )
                .unwrap(),
            2 => db.delete(key(1, b'm', None)).unwrap(),
            3 => db.put(key(1, b'e', Some(1)), b"corrupt").unwrap(),
            _ => db.delete(key(1, b'e', Some(3))).unwrap(),
        });
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        assert_eq!(db.group(1).err().unwrap().kind(), ErrorKind::InvalidData);
        assert_eq!(
            db.startup_provenance(1).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}

#[tokio::test]
async fn schema_and_required_metadata_fields_are_not_silently_recreated() {
    for corruption in 0..3 {
        let root = tempfile::tempdir().unwrap();
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        let mut store = db.group(1).unwrap();
        store
            .save_vote(&VoteOf::<TypeConfig>::new(2, 1))
            .await
            .unwrap();
        drop(store);
        drop(db);
        mutate(root.path(), |db| match corruption {
            0 => db.delete(b"schema").unwrap(),
            1 => db.put(b"schema", b"unknown").unwrap(),
            _ => {
                let meta_key = key(1, b'm', None);
                let mut value: serde_json::Value =
                    serde_json::from_slice(&db.get(&meta_key).unwrap().unwrap()).unwrap();
                value.as_object_mut().unwrap().remove("vote");
                db.put(meta_key, serde_json::to_vec(&value).unwrap())
                    .unwrap();
            }
        });
        if corruption < 2 {
            assert_eq!(
                RocksLogDb::<TypeConfig>::open(root.path())
                    .err()
                    .unwrap()
                    .kind(),
                ErrorKind::InvalidData
            );
        } else {
            let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
            assert_eq!(db.group(1).err().unwrap().kind(), ErrorKind::InvalidData);
        }
    }
}

#[tokio::test]
async fn snapshot_purge_can_advance_past_tail_then_accept_only_contiguous_suffix() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(1).unwrap();
    store.blocking_append(entries(1, 0..3)).await.unwrap();
    store.save_committed(Some(id(1, 1))).await.unwrap();
    store.purge(id(2, 9)).await.unwrap();
    store.save_committed(Some(id(2, 9))).await.unwrap();
    assert!(store.blocking_append(entries(2, 11..12)).await.is_err());
    store.blocking_append(entries(2, 10..12)).await.unwrap();
    drop(store);
    drop(db);
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    assert_eq!(
        db.group(1)
            .unwrap()
            .try_get_log_entries(..)
            .await
            .unwrap()
            .len(),
        2
    );
}
