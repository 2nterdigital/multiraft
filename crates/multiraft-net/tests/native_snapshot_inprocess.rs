//! Oversized typed snapshots must be rejected before killing a native Group.
use multiraft_core::TypeConfig;
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{GroupApp, Node, Router};
use multiraft_store::{
    MemLogStore, NativeSmOptions, SnapshotCatalog, StateMachineStore, StubNetworkFactory,
};
use openraft::alias::{LeaderIdOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf};
use openraft::vote::RaftLeaderIdExt;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

struct Image;
impl StateMachine for Image {
    type Error = std::io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn oversized_inprocess_snapshot_does_not_enter_native_install() {
    let root = tempfile::tempdir().unwrap();
    let sm = StateMachineStore::with_native_options(
        7,
        Image,
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.path(), 1)),
            max_snapshot_bytes: 4,
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap();
    let raft = openraft::Raft::new(
        2,
        Arc::new(openraft::Config::default()),
        StubNetworkFactory,
        MemLogStore::default(),
        sm.clone(),
    )
    .await
    .unwrap();
    let groups = Arc::new(Mutex::new(BTreeMap::from([(
        7,
        GroupApp {
            node_id: 2,
            group_id: 7,
            raft: raft.clone(),
            state_machine: sm,
        },
    )])));
    let router = Router::new();
    let (node, _tx) = Node::with_groups(2, router.clone(), groups);
    let task = tokio::spawn(node.run());
    let meta = SnapshotMetaOf::<TypeConfig> {
        snapshot_id: "oversize".into(),
        last_log_id: Some(LogIdOf::<TypeConfig>::new(
            LeaderIdOf::<TypeConfig>::new_committed(3, 1),
            10,
        )),
        last_membership: StoredMembershipOf::<TypeConfig>::default(),
    };
    assert!(router
        .send_snapshot(
            2,
            7,
            multiraft_core::typ::Vote::new_committed(3, 1),
            meta,
            vec![7; 5]
        )
        .await
        .is_err());
    let live = raft.with_state_machine(|_| Box::pin(async { true })).await;
    assert!(live.unwrap(), "invalid transport input stopped the Group");
    raft.shutdown().await.unwrap();
    task.abort();
    let _ = task.await;
}
