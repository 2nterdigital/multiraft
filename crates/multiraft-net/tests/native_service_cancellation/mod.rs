//! The native install owner must retain admission after the RPC waiter is cancelled.
use super::*;
use multiraft_core::{FileLogSyncLevel, TypeConfig};
use multiraft_fsm::{ApplyOut, GroupId};
use multiraft_store::{
    FileLogStoreOf, NativeSmOptions, SnapshotCatalog, StateMachineStore, StubNetworkFactory,
};
use openraft::alias::{LeaderIdOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf};
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

struct GateRestore {
    entered: mpsc::Sender<()>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl StateMachine for GateRestore {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> Result<(), Self::Error> {
        self.entered.send(()).unwrap();
        let mut released = self.gate.0.lock().unwrap();
        while !*released {
            released = self.gate.1.wait(released).unwrap();
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_service_waiter_does_not_admit_another_native_install() {
    let root = tempfile::tempdir().unwrap();
    let (entered, receiver) = mpsc::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let mut sm = StateMachineStore::with_native_options(
        7,
        GateRestore {
            entered,
            gate: gate.clone(),
        },
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.path().join("snapshots"), 1)),
            max_snapshot_bytes: 1024,
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap();
    let log = FileLogStoreOf::open_with_options(root.path().join("log"), 0, FileLogSyncLevel::Data)
        .unwrap();
    let raft = openraft::Raft::new(
        2,
        Arc::new(openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }),
        StubNetworkFactory,
        log,
        sm.clone(),
    )
    .await
    .unwrap();
    let groups = Arc::new(Mutex::new(BTreeMap::from([(
        7,
        crate::GroupApp {
            node_id: 2,
            group_id: 7,
            raft: raft.clone(),
            state_machine: sm.clone(),
        },
    )])));
    let service = Arc::new(RaftServiceImpl {
        groups,
        snapshot_slots: Arc::new(tokio::sync::Semaphore::new(1)),
    });
    let id = LogIdOf::<TypeConfig>::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 3);
    let membership = openraft::Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from([
            (1, openraft::BasicNode::new("one")),
            (2, openraft::BasicNode::new("two")),
            (3, openraft::BasicNode::new("three")),
        ]),
    )
    .unwrap();
    let meta = SnapshotMetaOf::<TypeConfig> {
        snapshot_id: "cancelled-rpc".into(),
        last_log_id: Some(id),
        last_membership: StoredMembershipOf::<TypeConfig>::new(None, membership),
    };
    let payload = encode((
        multiraft_core::typ::Vote::new_committed(1, 1),
        meta,
        vec![7_u8; 4],
    ));
    let request = || {
        Request::new(RaftRequest {
            group_id: 7,
            path: "/raft/snapshot".into(),
            payload: payload.clone(),
        })
    };
    let owned_service = service.clone();
    let first_request = request();
    let waiter = tokio::spawn(async move { owned_service.call(first_request).await });
    tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    waiter.abort();
    let _ = waiter.await;
    let second = tokio::time::timeout(Duration::from_millis(200), service.call(request())).await;
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if sm.get_current_snapshot().await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    sm.close_native_intake();
    raft.shutdown().await.unwrap();
    sm.wait_native_quiescent().await;
    assert!(
        matches!(second, Ok(Err(status)) if status.code() == tonic::Code::ResourceExhausted),
        "cancelled RPC released native install admission"
    );
}
