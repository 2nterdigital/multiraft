//! Per-batch FSM sync (ech0 ADR 0022): with `DiskState::defers_sync`, the bridge
//! answers an entry only after a `sync_applied` covering it, syncs every 256
//! entries and after a failed entry, and a failed sync answers nothing.
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use futures::{stream, StreamExt};
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{Request, TypeConfig};
use multiraft_fsm::{AppliedState, ApplyOut, DiskState, GroupId, StateMachine};
use multiraft_store::{Raft, RocksLogDb, StateMachineStore, StubNetworkFactory};
use openraft::alias::LeaderIdOf;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use openraft::{BasicNode, Config, EntryPayload, SnapshotPolicy};

const GROUP: GroupId = 7;

/// What survives power loss: written per entry, or by `sync_applied` when deferred.
#[derive(Clone, Default)]
struct Durable {
    record: Option<Vec<u8>>,
    index: Option<u64>,
    sum: u64,
}

/// State shared across restarts of one node.
#[derive(Default)]
struct Shared {
    durable: Mutex<Durable>,
    /// Last written index covered by each successful `sync_applied`.
    syncs: Mutex<Vec<u64>>,
    sync_calls: AtomicUsize,
    fail_sync: AtomicBool,
    gate_closed: Mutex<bool>,
    gate: Condvar,
}

impl Shared {
    fn set_gate(&self, closed: bool) {
        *self.gate_closed.lock().unwrap() = closed;
        self.gate.notify_all();
    }
    fn durable_sum(&self) -> u64 {
        self.durable.lock().unwrap().sum
    }
    fn syncs(&self) -> Vec<u64> {
        self.syncs.lock().unwrap().clone()
    }
}

/// Sums 8-byte payloads; any other payload fails its entry.
struct Fsm {
    defers: bool,
    shared: Arc<Shared>,
    /// Written but, when deferred, not yet synced.
    written: Durable,
}

impl Fsm {
    /// Reopen from what survived.
    fn open(defers: bool, shared: &Arc<Shared>) -> Self {
        let written = shared.durable.lock().unwrap().clone();
        Self {
            defers,
            shared: shared.clone(),
            written,
        }
    }
}

impl StateMachine for Fsm {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        unreachable!("disk mode never calls whole-image apply")
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        unreachable!("disk mode never calls whole-image snapshot")
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        unreachable!("disk mode never calls whole-image restore")
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}

impl DiskState for Fsm {
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.shared.durable.lock().unwrap().record.clone())
    }
    fn apply_durable(
        &mut self,
        _: GroupId,
        index: u64,
        data: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        let delta = match data {
            None => 0,
            Some(data) => u64::from_be_bytes(
                data.try_into()
                    .map_err(|_| io::Error::other("bad payload"))?,
            ),
        };
        self.written.sum += delta;
        self.written.record = Some(applied.record().to_vec());
        self.written.index = Some(index);
        if !self.defers {
            *self.shared.durable.lock().unwrap() = self.written.clone();
        }
        Ok(ApplyOut {
            effects: self.written.sum.to_be_bytes().to_vec(),
        })
    }
    fn defers_sync(&self) -> bool {
        self.defers
    }
    fn sync_applied(&mut self) -> io::Result<()> {
        self.shared.sync_calls.fetch_add(1, Ordering::SeqCst);
        let mut closed = self.shared.gate_closed.lock().unwrap();
        while *closed {
            closed = self.shared.gate.wait(closed).unwrap();
        }
        drop(closed);
        if self.shared.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected sync failure"));
        }
        self.shared
            .syncs
            .lock()
            .unwrap()
            .push(self.written.index.unwrap());
        *self.shared.durable.lock().unwrap() = self.written.clone();
        Ok(())
    }
}

fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/fsm-batch-sync");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

fn add(delta: u64) -> Request {
    Request::new(delta.to_be_bytes().to_vec())
}

/// Membership is entry 0 and the leader's blank entry 1; client writes start at 2.
async fn start(log: &RocksLogDb<TypeConfig>, fsm: Fsm, initialize: bool) -> Raft<Fsm> {
    let config = Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        snapshot_policy: SnapshotPolicy::Never,
        ..Default::default()
    };
    let raft = openraft::Raft::new(
        1,
        Arc::new(config.validate().unwrap()),
        StubNetworkFactory,
        log.group(GROUP).unwrap(),
        StateMachineStore::new(GROUP, fsm),
    )
    .await
    .unwrap();
    if initialize {
        raft.initialize(BTreeMap::from([(1, BasicNode::new("local"))]))
            .await
            .unwrap();
    }
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    raft
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_sync_answers_only_after_sync_applied_returns() {
    let root = scratch();
    let log = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let shared = Arc::new(Shared::default());
    let raft = start(&log, Fsm::open(true, &shared), true).await;
    raft.wait(Some(Duration::from_secs(5)))
        .applied_index(Some(1), "blank applied")
        .await
        .unwrap();

    shared.set_gate(true);
    let calls = shared.sync_calls.load(Ordering::SeqCst);
    let writer = tokio::spawn({
        let raft = raft.clone();
        async move { raft.client_write(add(5)).await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while shared.sync_calls.load(Ordering::SeqCst) == calls {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the entry reaches sync_applied");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !writer.is_finished(),
        "answered before sync_applied returned"
    );
    assert_eq!(shared.durable_sum(), 0);

    shared.set_gate(false);
    let response = writer.await.unwrap().unwrap();
    assert_eq!(response.data.effects, 5u64.to_be_bytes());
    assert_eq!(shared.durable_sum(), 5);
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_entry_mode_never_calls_sync_applied() {
    let root = scratch();
    let log = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let shared = Arc::new(Shared::default());
    // A closed gate would hang any sync_applied call.
    shared.set_gate(true);
    let raft = start(&log, Fsm::open(false, &shared), true).await;
    let response = raft.client_write(add(5)).await.unwrap();
    assert_eq!(response.data.effects, 5u64.to_be_bytes());
    assert_eq!(shared.durable_sum(), 5);
    assert_eq!(shared.sync_calls.load(Ordering::SeqCst), 0);
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_sync_answers_nothing_stops_the_group_and_replays_on_restart() {
    let root = scratch();
    let log = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let shared = Arc::new(Shared::default());
    let raft = start(&log, Fsm::open(true, &shared), true).await;
    raft.wait(Some(Duration::from_secs(5)))
        .applied_index(Some(1), "blank applied")
        .await
        .unwrap();

    shared.fail_sync.store(true, Ordering::SeqCst);
    assert!(raft.client_write(add(5)).await.is_err());
    assert_eq!(shared.durable_sum(), 0, "nothing was synced");
    assert!(
        raft.client_write(add(1)).await.is_err(),
        "the group stopped"
    );
    let _ = raft.shutdown().await;
    drop(raft);

    shared.fail_sync.store(false, Ordering::SeqCst);
    let raft = start(&log, Fsm::open(true, &shared), false).await;
    raft.wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(2), "unsynced entry replayed")
        .await
        .unwrap();
    assert_eq!(shared.durable_sum(), 5, "the unsynced entry was replayed");
    assert!(shared.syncs().contains(&2));
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_entry_syncs_and_answers_the_entries_before_it() {
    let root = scratch();
    let log = RocksLogDb::<TypeConfig>::open(root.path()).unwrap();
    let shared = Arc::new(Shared::default());
    let raft = start(&log, Fsm::open(true, &shared), true).await;
    raft.wait(Some(Duration::from_secs(5)))
        .applied_index(Some(1), "blank applied")
        .await
        .unwrap();

    // One message to the core: entries 2..=5 are committed and applied as one batch.
    let bad = Request::new(b"bad".to_vec());
    let results: Vec<_> = raft
        .client_write_many([add(5), add(7), bad, add(9)])
        .await
        .unwrap()
        .collect()
        .await;
    let answered: Vec<_> = results
        .iter()
        .map_while(|result| match result {
            Ok(Ok(response)) => Some(response.response.effects.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        answered,
        [5u64.to_be_bytes().to_vec(), 12u64.to_be_bytes().to_vec()]
    );
    assert_eq!(shared.durable_sum(), 12);
    let syncs = shared.syncs();
    assert_eq!(syncs.last(), Some(&3), "entries 2 and 3 synced once");
    assert!(!syncs.contains(&2), "entries 2 and 3 were in one batch");
    let _ = raft.shutdown().await;
}

fn entry(index: u64, delta: u64) -> Entry {
    Entry {
        log_id: LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index),
        payload: EntryPayload::Normal(add(delta)),
    }
}

#[tokio::test]
async fn long_batch_syncs_every_256_entries() {
    let shared = Arc::new(Shared::default());
    let mut store = StateMachineStore::new(GROUP, Fsm::open(true, &shared));
    store
        .apply(stream::iter(
            (0..600).map(|index| Ok((entry(index, 1), None))),
        ))
        .await
        .unwrap();
    assert_eq!(shared.syncs(), [255, 511, 599]);
    assert_eq!(shared.durable_sum(), 600);
}
