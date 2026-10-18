//! Real gRPC snapshot payload beyond tonic's old implicit 4-MiB decode limit.
use multiraft_core::typ::{LogId, Vote};
use multiraft_core::{FileLogSyncLevel, TypeConfig};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{GroupApp, GrpcRouter, GrpcServer};
use multiraft_store::{
    FileLogStoreOf, NativeSmOptions, SnapshotCatalog, StateMachineStore, StubNetworkFactory,
};
use openraft::alias::{LeaderIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::network::RPCOption;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use openraft::{BasicNode, Config, Membership, SnapshotPolicy};
use openraft_multi::GroupRouter;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Image {
    bytes: usize,
}
impl StateMachine for Image {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![7; self.bytes])
    }
    fn restore(&mut self, _: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        if bytes.iter().any(|byte| *byte != 7) {
            return Err(std::io::Error::other("invalid image"));
        }
        self.bytes = bytes.len();
        Ok(())
    }
}

#[tokio::test]
async fn native_grpc_transfers_five_mib_and_receiver_reopens_its_local_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Arc::new(SnapshotCatalog::new(root.path().join("snapshots"), 1));
    let sm = StateMachineStore::with_native_options(
        7,
        Image::default(),
        NativeSmOptions {
            catalog: catalog.clone(),
            max_snapshot_bytes: 8 * 1024 * 1024,
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap();
    let log = FileLogStoreOf::open_with_options(root.path().join("log"), 0, FileLogSyncLevel::Data)
        .unwrap();
    let config = Arc::new(
        Config {
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = openraft::Raft::new(2, config.clone(), StubNetworkFactory, log, sm.clone())
        .await
        .unwrap();
    let groups = Arc::new(Mutex::new(BTreeMap::from([(
        7,
        GroupApp {
            node_id: 2,
            group_id: 7,
            raft: raft.clone(),
            state_machine: sm.clone(),
        },
    )])));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        GrpcServer::serve_with_listener(listener, groups)
            .await
            .unwrap();
    });
    let router = GrpcRouter::new(vec![(2, address)], 1);
    let id = LogId::new(LeaderIdOf::<TypeConfig>::new_committed(3, 1), 10);
    let meta = SnapshotMetaOf::<TypeConfig> {
        snapshot_id: "five-mib".into(),
        last_log_id: Some(id),
        last_membership: StoredMembershipOf::<TypeConfig>::new(
            Some(LogId::new(LeaderIdOf::<TypeConfig>::new_committed(3, 1), 1)),
            Membership::new(
                vec![BTreeSet::from([1, 2, 3])],
                BTreeMap::from([
                    (1, BasicNode::new("one")),
                    (2, BasicNode::new("two")),
                    (3, BasicNode::new("three")),
                ]),
            )
            .unwrap(),
        ),
    };
    let snapshot = SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
        meta,
        snapshot: Cursor::new(vec![7; 5 * 1024 * 1024]),
    };
    router
        .full_snapshot(
            2,
            7,
            Vote::new_committed(3, 1),
            snapshot,
            std::future::pending(),
            RPCOption::new(Duration::from_secs(10)),
        )
        .await
        .unwrap();
    assert_eq!(sm.with_fsm(|fsm| fsm.bytes).await, 5 * 1024 * 1024);
    raft.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
    drop(raft);
    drop(sm);
    let mut fresh = StateMachineStore::with_native_options(
        7,
        Image::default(),
        NativeSmOptions {
            catalog,
            max_snapshot_bytes: 8 * 1024 * 1024,
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap();
    let local = fresh.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(local.meta.last_log_id, Some(id));
    fresh
        .install_snapshot(&local.meta, local.snapshot)
        .await
        .unwrap();
    assert_eq!(fresh.with_fsm(|fsm| fsm.bytes).await, 5 * 1024 * 1024);
}
