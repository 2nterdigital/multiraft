//! Catch the legacy policy invoking a memory snapshot beyond 5000 entries.
use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, CounterFsm, GroupId, StateMachine};
use multiraft_net::MultiRaft;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

struct Probe {
    inner: CounterFsm,
    builds: Arc<AtomicUsize>,
}
impl StateMachine for Probe {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.inner.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.inner.restore(group, bytes)
    }
}

#[tokio::test]
async fn disabled_processes_5100_entries_without_policy_snapshot() {
    let builds = Arc::new(AtomicUsize::new(0));
    let shared = builds.clone();
    let node = MultiRaft::start_with_factory(ClusterConfig::for_test(1, &[1]), move |_| {
        Ok(Probe {
            inner: CounterFsm::new(),
            builds: shared.clone(),
        })
    })
    .await
    .unwrap();
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for batch in 0..51 {
        let result = node
            .propose_batch(
                7,
                (0..100)
                    .map(|i| CounterFsm::encode_add(1, batch * 100 + i))
                    .collect(),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 100);
    }
    assert_eq!(
        node.read_linearizable(7, |fsm| fsm.inner.value(7))
            .await
            .unwrap(),
        5100
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.shutdown().await.unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        0,
        "Disabled triggered the legacy memory snapshot policy"
    );
}

#[tokio::test]
async fn low_level_constructor_disables_policy_snapshots_too() {
    let builds = Arc::new(AtomicUsize::new(0));
    let shared = builds.clone();
    let node = multiraft_net::create_node(1, &[7], multiraft_net::Router::new(), move |_| Probe {
        inner: CounterFsm::new(),
        builds: shared.clone(),
    })
    .await;
    let raft = node.get_raft(7).unwrap();
    raft.initialize(std::collections::BTreeMap::from([(
        1,
        openraft::BasicNode::new("one"),
    )]))
    .await
    .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "single leader")
        .await
        .unwrap();
    for i in 0..5100 {
        raft.client_write(multiraft_core::Request::new(CounterFsm::encode_add(1, i)))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    raft.shutdown().await.unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        0,
        "low-level constructor retained the policy"
    );
}
