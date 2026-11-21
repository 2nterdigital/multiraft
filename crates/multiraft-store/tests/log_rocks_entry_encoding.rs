//! Entry values are tagged bincode, control entries stay JSON, and legacy JSON still reads.
use std::collections::{BTreeMap, BTreeSet};

use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{NativeControl, OperationId, Request, TypeConfig};
use multiraft_store::RocksLogDb;
use openraft::alias::LeaderIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::RaftLeaderIdExt;
use openraft::{EntryPayload, Membership, RaftLogReader};

const GROUP: u64 = 7;

fn id(index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index)
}
fn key(tag: u8, index: Option<u64>) -> Vec<u8> {
    let mut key = vec![b'g'];
    key.extend_from_slice(&GROUP.to_be_bytes());
    key.push(tag);
    if let Some(index) = index {
        key.extend_from_slice(&index.to_be_bytes());
    }
    key
}
fn claim() -> NativeControl {
    NativeControl::Claim {
        operation: OperationId {
            sequence: 7,
            nonce: [3; 16],
        },
        executor_epoch: 4,
    }
}
fn sample() -> Vec<Entry> {
    let nodes = BTreeMap::from_iter([1, 2, 3].map(|id| (id, openraft::BasicNode::default())));
    vec![
        Entry::new_normal(id(0), Request::new(vec![90u8; 256])),
        Entry::new_membership(
            id(1),
            Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes).unwrap(),
        ),
        Entry::new_normal(id(2), Request::native_control(claim())),
        Entry::new_blank(id(3)),
    ]
}
fn assert_sample(read: &[Entry]) {
    assert_eq!(read.len(), 4);
    for (got, want) in read.iter().zip(sample()) {
        assert_eq!(got.log_id, want.log_id);
        assert_eq!(
            serde_json::to_vec(got).unwrap(),
            serde_json::to_vec(&want).unwrap()
        );
    }
    match &read[2].payload {
        EntryPayload::Normal(request) => assert_eq!(request.control, Some(claim())),
        _ => panic!("control entry payload changed"),
    }
}

#[tokio::test]
async fn normal_entries_are_bincode_and_control_entries_stay_json() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        let mut store = db.group(GROUP).unwrap();
        store.blocking_append(sample()).await.unwrap();
        let json_size = serde_json::to_vec(&sample()[0]).unwrap().len() as u64;
        assert!(db.retained_log_bytes(GROUP).unwrap() < json_size);
    }
    {
        let raw = rocksdb::DB::open_default(root.path()).unwrap();
        let tags: Vec<u8> = (0..4)
            .map(|index| raw.get(key(b'e', Some(index))).unwrap().unwrap()[0])
            .collect();
        assert_eq!(tags, vec![0xB1, 0xB1, b'{', 0xB1]);
    }
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(GROUP).unwrap();
    assert_sample(&store.try_get_log_entries(..).await.unwrap());
}

#[tokio::test]
async fn legacy_json_values_read_back_and_accept_an_identical_reappend() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        db.group(GROUP)
            .unwrap()
            .blocking_append(sample())
            .await
            .unwrap();
    }
    {
        // Rewrite every entry in the pre-bincode JSON layout and fix the retained byte count.
        let raw = rocksdb::DB::open_default(root.path()).unwrap();
        let mut bytes = 0u64;
        for (index, entry) in sample().iter().enumerate() {
            let legacy = serde_json::to_vec(entry).unwrap();
            bytes += legacy.len() as u64;
            raw.put(key(b'e', Some(index as u64)), legacy).unwrap();
        }
        let mut meta: serde_json::Value =
            serde_json::from_slice(&raw.get(key(b'm', None)).unwrap().unwrap()).unwrap();
        meta["bytes"] = bytes.into();
        raw.put(key(b'm', None), serde_json::to_vec(&meta).unwrap())
            .unwrap();
        raw.flush_wal(true).unwrap();
    }
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(GROUP).unwrap();
    assert_sample(&store.try_get_log_entries(..).await.unwrap());
    store.blocking_append(sample()).await.unwrap();
    let changed = Entry::new_normal(id(0), Request::new(vec![91u8; 256]));
    assert!(store.blocking_append(vec![changed]).await.is_err());
    assert_sample(&store.try_get_log_entries(..).await.unwrap());

    // New bincode entries past the legacy tail, then truncate and purge across the boundary.
    let suffix: Vec<Entry> = (4..7)
        .map(|index| Entry::new_normal(id(index), Request::new(vec![index as u8; 64])))
        .collect();
    store.blocking_append(suffix).await.unwrap();
    store.truncate_after(Some(id(5))).await.unwrap();
    store.purge(id(1)).await.unwrap();
    drop(store);
    drop(db);
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(GROUP).unwrap();
    let read = store.try_get_log_entries(..).await.unwrap();
    let indexes: Vec<u64> = read.iter().map(|entry| entry.log_id.index()).collect();
    assert_eq!(indexes, vec![2, 3, 4, 5]);
    assert_eq!(
        serde_json::to_vec(&read[0]).unwrap(),
        serde_json::to_vec(&sample()[2]).unwrap()
    );
    match &read[3].payload {
        EntryPayload::Normal(request) => assert_eq!(request.data, vec![5u8; 64]),
        _ => panic!("bincode entry payload changed"),
    }
}
