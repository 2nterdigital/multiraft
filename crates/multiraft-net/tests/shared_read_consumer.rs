//! Independent business-neutral FSM consumer, entirely through weak public runtime APIs.
use multiraft_core::{ClusterConfig, MultiRaftError, ReadIndexFailure};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    GroupConfig, NodeOwner, ReadEvent, ReadOutcome, ReadStage, RuntimeConfig, RuntimeError,
    RuntimeHandle, RuntimeTransport, SharedFabric, TryReadError,
};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

struct Counter {
    value: u64,
    releases: Arc<AtomicUsize>,
}
impl Drop for Counter {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateMachine for Counter {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, data: &[u8]) -> Result<ApplyOut, io::Error> {
        self.value = u64::from_be_bytes(data.try_into().map_err(|_| io::Error::other("command"))?);
        Ok(ApplyOut {
            effects: self.value.to_be_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, io::Error> {
        Ok(self.value.to_be_bytes().to_vec())
    }
    fn restore(&mut self, _: GroupId, data: &[u8]) -> Result<(), io::Error> {
        self.value = u64::from_be_bytes(data.try_into().map_err(|_| io::Error::other("snapshot"))?);
        Ok(())
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
struct Cluster {
    owners: Vec<Option<NodeOwner<Counter>>>,
    handles: Vec<RuntimeHandle<Counter>>,
    releases: Arc<AtomicUsize>,
}
async fn cluster() -> Cluster {
    let fabric = SharedFabric::new();
    let releases = Arc::new(AtomicUsize::new(0));
    let owners = futures::future::join_all([1, 2, 3].into_iter().map(|node| {
        let mut config = RuntimeConfig::new(
            ClusterConfig::for_test(node, &[1, 2, 3]),
            vec![GroupConfig {
                group_id: 7,
                voters: vec![1, 2, 3],
            }],
        );
        config.transport = RuntimeTransport::InProcess(fabric.clone());
        let counter_releases = releases.clone();
        async move {
            Some(
                NodeOwner::start(
                    config,
                    move |_| {
                        Ok(Counter {
                            value: 0,
                            releases: counter_releases.clone(),
                        })
                    },
                    Instant::now() + Duration::from_secs(8),
                )
                .await
                .unwrap(),
            )
        }
    }))
    .await;
    let handles = owners
        .iter()
        .map(|o| o.as_ref().unwrap().handle())
        .collect();
    Cluster {
        owners,
        handles,
        releases,
    }
}
async fn leader(handles: &[RuntimeHandle<Counter>], excluded: Option<usize>) -> usize {
    timeout(Duration::from_secs(5), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if Some(index) == excluded {
                    continue;
                }
                if let Ok(hint) = handle.leader_hint(7, deadline()).await {
                    if hint.candidate == Some(handle.node_id())
                        && handle
                            .read_linearizable(7, deadline(), |_| ())
                            .await
                            .is_ok()
                    {
                        return index;
                    }
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("live majority forms authority")
}
async fn stop(cluster: Cluster) {
    for owner in cluster.owners.into_iter().flatten() {
        owner.shutdown(deadline()).await.unwrap();
    }
    assert_eq!(
        cluster.releases.load(Ordering::SeqCst),
        3,
        "successful stop released actual FSMs"
    );
    for handle in cluster.handles {
        assert!(matches!(
            handle.read_linearizable(7, deadline(), |_| ()).await,
            Err(RuntimeError::Closed)
        ));
    }
}
async fn workload(handle: RuntimeHandle<Counter>, start: u64, end: u64) {
    let acknowledged = Arc::new(AtomicU64::new(start));
    let done = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicUsize::new(0));
    let readers: Vec<_> = (0..32)
        .map(|_| {
            let handle = handle.clone();
            let acknowledged = acknowledged.clone();
            let done = done.clone();
            let completed = completed.clone();
            tokio::spawn(async move {
                while !done.load(Ordering::SeqCst) {
                    let floor = acknowledged.load(Ordering::SeqCst);
                    let value = handle
                        .read_linearizable(7, deadline(), |f| f.value)
                        .await
                        .unwrap();
                    assert!(
                        value >= floor,
                        "{value} omitted previously acknowledged {floor}"
                    );
                    completed.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();
    for value in start + 1..=end {
        let receipt = handle
            .propose(7, value.to_be_bytes().to_vec(), deadline())
            .await
            .unwrap();
        assert_eq!(receipt.effects, value.to_be_bytes());
        acknowledged.store(value, Ordering::SeqCst);
        tokio::task::yield_now().await;
    }
    done.store(true, Ordering::SeqCst);
    for reader in readers {
        reader.await.unwrap();
    }
    assert!(completed.load(Ordering::SeqCst) >= 32);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_concurrent_reads_preserve_acknowledged_writes_through_owner_change_and_quorum_loss() {
    timeout(Duration::from_secs(25), async {
        let mut c = cluster().await;
        let old = leader(&c.handles, None).await;
        workload(c.handles[old].clone(), 0, 30).await;
        c.owners[old]
            .take()
            .unwrap()
            .shutdown(deadline())
            .await
            .unwrap();
        let new = leader(&c.handles, Some(old)).await;
        let value = c.handles[new]
            .read_linearizable(7, deadline(), |f| f.value)
            .await
            .unwrap();
        assert_eq!(value, 30);
        workload(c.handles[new].clone(), 30, 50).await;
        let follower = (0..3).find(|&i| i != old && i != new).unwrap();
        c.owners[follower]
            .take()
            .unwrap()
            .shutdown(deadline())
            .await
            .unwrap();
        let began = Instant::now();
        let events = Mutex::new(Vec::<ReadEvent>::new());
        let observer = |e| events.lock().unwrap().push(e);
        let results = futures::future::join_all((0..32).map(|_| {
            c.handles[new].read_linearizable_observed(7, deadline(), |f| f.value, &observer)
        }))
        .await;
        assert!(
            results.iter().all(Result::is_err),
            "no stale success without majority"
        );
        assert!(began.elapsed() < Duration::from_secs(4));
        assert!(results.iter().all(|r| matches!(
            r,
            Err(RuntimeError::Deadline { .. })
                | Err(RuntimeError::Source(MultiRaftError::NotLeader { .. }))
                | Err(RuntimeError::Source(MultiRaftError::ReadIndex(
                    ReadIndexFailure::QuorumUnavailable { .. } | ReadIndexFailure::RoundTimeout
                )))
        )));
        {
            let facts = events.lock().unwrap();
            assert_eq!(
                facts
                    .iter()
                    .filter(|e| e.stage == ReadStage::ReadIndex)
                    .count(),
                32
            );
            assert!(facts
                .iter()
                .filter(|e| e.stage == ReadStage::ReadIndex)
                .all(|e| e.outcome != ReadOutcome::Completed));
        }
        stop(c).await;
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_caller_fsm_rejection_and_observer_panic_do_not_change_source_results() {
    let c = cluster().await;
    let leader = leader(&c.handles, None).await;
    let events = Mutex::new(Vec::<ReadEvent>::new());
    let observer = |e| events.lock().unwrap().push(e);
    let result = c.handles[leader]
        .try_read_linearizable(
            7,
            deadline(),
            |_| Err::<(), _>(io::Error::other("application refused")),
            Some(&observer),
        )
        .await;
    assert!(matches!(result, Err(TryReadError::Application(_))));
    {
        let facts = events.lock().unwrap();
        assert!(facts
            .iter()
            .any(|e| e.stage == ReadStage::ReadIndex && e.outcome == ReadOutcome::Completed));
        assert!(facts
            .iter()
            .any(|e| e.stage == ReadStage::StateMachine
                && e.outcome == ReadOutcome::ApplicationError));
    }
    let callers = Mutex::new(Vec::<(usize, ReadEvent)>::new());
    let results = futures::future::join_all((0..32).map(|id| {
        let callers = &callers;
        let handle = &c.handles[leader];
        async move {
            let observer = |event| callers.lock().unwrap().push((id, event));
            handle
                .read_linearizable_observed(7, deadline(), |f| (id, f.value), &observer)
                .await
                .unwrap()
        }
    }))
    .await;
    for (id, result) in results.iter().enumerate() {
        assert_eq!(*result, (id, 0), "each waiter performs its own FSM query");
        assert_eq!(
            callers
                .lock()
                .unwrap()
                .iter()
                .filter(|(who, e)| *who == id
                    && e.stage == ReadStage::StateMachine
                    && e.outcome == ReadOutcome::Completed)
                .count(),
            1
        );
    }
    let panicking = |_: ReadEvent| panic!("observer panic must be isolated");
    assert_eq!(
        c.handles[leader]
            .read_linearizable_observed(7, deadline(), |f| f.value, &panicking)
            .await
            .unwrap(),
        0
    );
    let follower = (leader + 1) % 3;
    let error = c.handles[follower]
        .read_linearizable_observed(7, deadline(), |f| f.value, &observer)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::Source(MultiRaftError::NotLeader { .. })
    ));
    assert!(events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.outcome == ReadOutcome::NotLeader
            && e.local_node_id == c.handles[follower].node_id()));
    stop(c).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hint_receivers_cannot_retain_owner_and_waiters_wake_on_drop() {
    let mut c = cluster().await;
    let leader = leader(&c.handles, None).await;
    let handle = c.handles[leader].clone();
    let (initial, mut retained) = handle.observe_leader_hint(7, deadline()).await.unwrap();
    assert_eq!(initial.candidate, Some(handle.node_id()));
    let (_group, mut group_receiver) = handle.observe_group(7, deadline()).await.unwrap();
    let waiting = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .wait_leader_hint_other_than(
                    7,
                    initial.candidate.unwrap(),
                    Instant::now() + Duration::from_secs(60),
                )
                .await
        }
    });
    // Wait until first poll; receiver registration precedes returning Pending.
    tokio::task::yield_now().await;
    drop(c.owners[leader].take());
    assert!(matches!(
        timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap(),
        Err(RuntimeError::Closed) | Err(RuntimeError::Source(MultiRaftError::ObservationClosed(_)))
    ));
    assert!(matches!(
        retained.latest(),
        Err(MultiRaftError::ObservationClosed(_))
    ));
    timeout(Duration::from_secs(5), async {
        while group_receiver.changed().await.is_ok() {}
    })
    .await
    .unwrap();
    timeout(Duration::from_secs(5), async {
        while c.releases.load(Ordering::SeqCst) < 1 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop(c).await;
}
