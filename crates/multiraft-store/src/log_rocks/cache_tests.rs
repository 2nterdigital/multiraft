//! Group cache coherence across lifecycle operations, write failures, the
//! append tail rule, and point versus iterator range reads.
use super::*;
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{Request, TypeConfig};
use openraft::alias::LeaderIdOf;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::vote::{RaftLeaderIdExt, RaftVote};
use openraft::{EntryPayload, RaftLogReader};

fn id(term: u64, index: u64) -> LogId {
    LogId::new(LeaderIdOf::<TypeConfig>::new_committed(term, 1), index)
}
fn blanks(term: u64, range: std::ops::Range<u64>) -> Vec<Entry> {
    range
        .map(|index| Entry::new_blank(id(term, index)))
        .collect()
}
fn vote(term: u64) -> VoteOf<TypeConfig> {
    VoteOf::<TypeConfig>::from_leader_id(LeaderIdOf::<TypeConfig>::new_committed(term, 1), true)
}
fn is_unloaded(store: &RocksLogStore<TypeConfig>) -> bool {
    let cache = store.gate.lock().unwrap();
    cache.receipt.is_none() && cache.metadata.is_none()
}
/// A handle on a group that may have no namespace, without opening it.
fn raw_handle(
    db: &RocksLogDb<TypeConfig>,
    group: u64,
    generation: Option<u64>,
) -> RocksLogStore<TypeConfig> {
    RocksLogStore {
        owner: db.clone(),
        group,
        gate: db.engine.gate(group).unwrap(),
        generation,
    }
}
/// Every hot-path operation must fail on a fenced handle.
async fn assert_fenced(store: &mut RocksLogStore<TypeConfig>) {
    assert!(store.read_vote().await.is_err());
    assert!(store.read_committed().await.is_err());
    assert!(store.get_log_state().await.is_err());
    assert!(store.try_get_log_entries(0..1).await.is_err());
    assert!(store.save_vote(&vote(99)).await.is_err());
    assert!(store.save_committed(None).await.is_err());
    assert!(store.blocking_append(blanks(9, 0..1)).await.is_err());
    assert!(store.truncate_after(None).await.is_err());
    assert!(store.purge(id(9, 0)).await.is_err());
}

#[tokio::test]
async fn cached_absent_receipt_and_metadata_follow_every_lifecycle_step() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();

    // enroll: a legacy handle has cached "no receipt" and its metadata.
    let mut legacy = db.group(7).unwrap();
    legacy.save_vote(&vote(3)).await.unwrap();
    legacy.blocking_append(blanks(3, 0..2)).await.unwrap();
    assert_eq!(legacy.read_vote().await.unwrap(), Some(vote(3)));
    assert!(legacy.gate.lock().unwrap().receipt == Some(None));
    db.enroll_controlled(7, 1, b"cutover").unwrap();
    assert!(is_unloaded(&legacy));
    assert_fenced(&mut legacy).await;
    let mut enrolled = db.group_generation(7, 1).unwrap();
    assert_eq!(enrolled.read_vote().await.unwrap(), Some(vote(3)));
    enrolled.save_vote(&vote(4)).await.unwrap();
    assert_eq!(enrolled.read_vote().await.unwrap(), Some(vote(4)));

    // retire, cleanup_group, finish_cleanup: the enrolled handle stays fenced.
    db.retire(7, 1, b"proof").unwrap();
    assert!(is_unloaded(&enrolled));
    assert_fenced(&mut enrolled).await;
    db.cleanup_group(7, 1, b"proof").unwrap();
    assert!(is_unloaded(&enrolled));
    assert_fenced(&mut enrolled).await;
    db.finish_cleanup(7, 1, b"proof").unwrap();
    assert!(is_unloaded(&enrolled));
    assert_fenced(&mut enrolled).await;

    // begin_rejoin and both advance steps: the cached Reserved receipt must
    // refresh, and the new generation sees fresh metadata, not generation 1's.
    db.begin_rejoin(7, 1, 2, b"rejoin").unwrap();
    assert!(is_unloaded(&enrolled));
    assert_fenced(&mut enrolled).await;
    let mut next = raw_handle(&db, 7, Some(2));
    assert!(next.read_vote().await.is_err(), "Reserved is not active");
    assert!(next.gate.lock().unwrap().receipt.is_some());
    db.advance_controlled(
        7,
        2,
        b"rejoin",
        ControlledGroupStage::Reserved,
        ControlledGroupStage::Opening,
    )
    .unwrap();
    assert!(is_unloaded(&next));
    assert_eq!(next.read_vote().await.unwrap(), None);
    next.blocking_append(blanks(5, 0..1)).await.unwrap();
    db.advance_controlled(
        7,
        2,
        b"rejoin",
        ControlledGroupStage::Opening,
        ControlledGroupStage::Opened,
    )
    .unwrap();
    assert!(is_unloaded(&next));
    assert_eq!(
        next.get_log_state().await.unwrap().last_log_id,
        Some(id(5, 0))
    );
    assert_fenced(&mut enrolled).await;
    assert_fenced(&mut legacy).await;
}

#[tokio::test]
async fn reservation_replaces_a_cached_absent_receipt() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut legacy = raw_handle(&db, 9, None);
    // No namespace yet: the receipt check passes and caches its absence.
    assert!(legacy.read_vote().await.is_err());
    assert!(legacy.gate.lock().unwrap().receipt == Some(None));
    db.reserve_controlled(9, 1, b"reserve").unwrap();
    assert!(is_unloaded(&legacy));
    assert_fenced(&mut legacy).await;
    let mut reserved = raw_handle(&db, 9, Some(1));
    assert_fenced(&mut reserved).await;
}

#[tokio::test]
async fn failed_write_unloads_the_cache_and_keeps_durable_state() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
        let mut store = db.group(3).unwrap();
        store.save_vote(&vote(2)).await.unwrap();
    }
    // A read-only engine rejects every write, which a writable one cannot be made to do.
    let db = RocksLogDb::<TypeConfig> {
        engine: Arc::new(Engine {
            db: DB::open_for_read_only(&Options::default(), root.path(), false).unwrap(),
            gates: Mutex::new(HashMap::new()),
            cache: Cache::new_lru_cache(1024),
            write_buffer_manager: WriteBufferManager::new_write_buffer_manager(1024, false),
            db_id: "read-only".to_owned(),
            io_latency: IoLatency::default(),
        }),
    };
    let mut store = db.group(3).unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(vote(2)));
    assert!(!is_unloaded(&store));
    assert!(store.save_vote(&vote(5)).await.is_err());
    assert!(is_unloaded(&store));
    assert_eq!(store.read_vote().await.unwrap(), Some(vote(2)));
    assert!(store.blocking_append(blanks(2, 0..1)).await.is_err());
    assert!(is_unloaded(&store));
    assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);
}

#[tokio::test]
async fn tail_rule_keeps_duplicate_appends_idempotent_and_conflicts_rejected() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(1).unwrap();
    store.blocking_append(blanks(1, 0..3)).await.unwrap();
    let bytes = db.retained_log_bytes(1).unwrap();
    // Same entries again, alone and straddling the tail.
    store.blocking_append(blanks(1, 1..3)).await.unwrap();
    assert_eq!(db.retained_log_bytes(1).unwrap(), bytes);
    store.blocking_append(blanks(1, 2..5)).await.unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(id(1, 4))
    );
    // Different identity, and same identity with different bytes, both conflict.
    assert!(store.blocking_append(blanks(2, 4..5)).await.is_err());
    let changed = Entry {
        log_id: id(1, 4),
        payload: EntryPayload::Normal(Request::new(b"other".to_vec())),
    };
    assert!(store.blocking_append(vec![changed]).await.is_err());
    // A gap past the tail is still refused; the next index still appends.
    assert!(store.blocking_append(blanks(1, 6..7)).await.is_err());
    store.blocking_append(blanks(1, 5..6)).await.unwrap();
    // After truncation the old tail index is next again and appends anew.
    store.truncate_after(Some(id(1, 3))).await.unwrap();
    store.blocking_append(blanks(2, 4..5)).await.unwrap();
    let logs = store.try_get_log_entries(3..).await.unwrap();
    let ids: Vec<_> = logs.iter().map(RaftEntry::log_id).collect();
    assert_eq!(ids, vec![id(1, 3), id(2, 4)]);
    drop(store);
    db.group(1).unwrap();
}

#[tokio::test]
async fn point_reads_match_the_iterator_at_every_boundary() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(2).unwrap();
    store.blocking_append(blanks(1, 0..20)).await.unwrap();
    store.purge(id(1, 4)).await.unwrap();
    let ids = |entries: Vec<Entry>| entries.iter().map(RaftEntry::log_id).collect::<Vec<_>>();
    // (requested range, first and last index after clipping to 5..=19, or empty)
    let cases = [
        (5..5, None),
        (7..7, None),
        (0..3, None),
        (20..30, None),
        (2..10, Some((5, 9))),
        (0..13, Some((5, 12))),   // 8 entries after clipping: points
        (0..14, Some((5, 13))),   // 9 entries after clipping: iterator
        (10..18, Some((10, 17))), // exactly 8
        (10..19, Some((10, 18))), // exactly 9
        (12..40, Some((12, 19))),
        (19..20, Some((19, 19))),
    ];
    for (range, clipped) in cases {
        let read = ids(store.try_get_log_entries(range.clone()).await.unwrap());
        let Some((lower, upper)) = clipped else {
            assert!(read.is_empty(), "{range:?}");
            continue;
        };
        let expected: Vec<_> = (lower..=upper).map(|index| id(1, index)).collect();
        assert_eq!(read, expected, "{range:?}");
        assert_eq!(ids(store.read_points(lower, upper).unwrap()), expected);
        assert_eq!(ids(store.read_iterated(lower, upper).unwrap()), expected);
    }
    // A missing entry: both report a gap before later entries, a miss at the end.
    db.engine.db.delete(entry_key(2, 12)).unwrap();
    for (lower, upper) in [(10, 15), (10, 12), (12, 12), (12, 13), (5, 12)] {
        let points = store.read_points(lower, upper).unwrap_err().to_string();
        let iterated = store.read_iterated(lower, upper).unwrap_err().to_string();
        assert_eq!(points, iterated, "{lower}..={upper}");
    }
    assert!(store.try_get_log_entries(10..13).await.is_err());
    assert!(store.try_get_log_entries(10..20).await.is_err());
}

#[tokio::test]
async fn append_after_purge_past_the_tail_starts_at_the_purge_boundary() {
    let root = tempfile::tempdir().unwrap();
    let db = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let mut store = db.group(4).unwrap();
    store.blocking_append(blanks(1, 0..3)).await.unwrap();
    // Snapshot installation: the purge marker moves beyond the tail.
    store.purge(id(2, 10)).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(id(2, 10)));
    assert_eq!(state.last_log_id, Some(id(2, 10)));
    assert_eq!(store.required_metadata().unwrap().tail, None);
    assert!(
        store.blocking_append(blanks(2, 12..13)).await.is_err(),
        "gap"
    );
    store.blocking_append(blanks(2, 11..13)).await.unwrap();
    let ids: Vec<_> = store
        .try_get_log_entries(..)
        .await
        .unwrap()
        .iter()
        .map(RaftEntry::log_id)
        .collect();
    assert_eq!(ids, vec![id(2, 11), id(2, 12)]);
    drop(store);
    // The durable state validates on reopen as well.
    db.group(4).unwrap();
}
