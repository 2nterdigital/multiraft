//! Independent business-neutral FSM consumer, entirely through weak public runtime APIs.
use multiraft_core::{ClusterConfig, MultiRaftError, ReadIndexFailure};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    GroupConfig, NodeOwner, ReadEvent, ReadOutcome, ReadStage, RuntimeConfig, RuntimeError,
    RuntimeHandle, RuntimeTransport, SharedFabric, TryReadError,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
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

#[tokio::test]
async fn per_call_confirmation_sees_prior_writes_and_cancel_does_not_strand_next_reader() {
    let c = cluster().await;
    let leader = leader(&c.handles, None).await;
    let handle = &c.handles[leader];
    for value in 1_u64..=3 {
        handle
            .propose(7, value.to_be_bytes().to_vec(), deadline())
            .await
            .unwrap();
        let results = futures::future::join_all(
            (0..8).map(|_| handle.read_linearizable(7, deadline(), |f| f.value)),
        )
        .await;
        assert!(
            results
                .iter()
                .all(|value_seen| matches!(value_seen, Ok(seen) if *seen==value)),
            "each independent confirmation includes earlier acknowledged write"
        );
    }
    let events = Mutex::new(Vec::<ReadEvent>::new());
    let observer = |event| events.lock().unwrap().push(event);
    let mut canceled =
        Box::pin(handle.read_linearizable_observed(7, deadline(), |f| f.value, &observer));
    assert!(futures::poll!(&mut canceled).is_pending());
    drop(canceled);
    assert_eq!(
        handle
            .read_linearizable(7, deadline(), |f| f.value)
            .await
            .unwrap(),
        3
    );
    assert!(events.lock().unwrap().iter().any(
        |event| event.stage == ReadStage::ReadIndex && event.outcome == ReadOutcome::Cancelled
    ));
    stop(c).await;
}
#[tokio::test]
async fn per_call_deadline_quorum_loss_and_closed_handle_never_return_stale_success() {
    let mut c = cluster().await;
    let live = leader(&c.handles, None).await;
    let expired = c.handles[live]
        .read_linearizable(7, Instant::now(), |_| panic!("expired caller cannot query"))
        .await;
    assert!(matches!(
        expired,
        Err(RuntimeError::Deadline {
            outcome_unknown: false,
            ..
        })
    ));
    for index in 0..3 {
        if index != live {
            c.owners[index]
                .take()
                .unwrap()
                .shutdown(deadline())
                .await
                .unwrap();
        }
    }
    let events = Mutex::new(Vec::<ReadEvent>::new());
    let observer = |event| events.lock().unwrap().push(event);
    let results = futures::future::join_all((0..3).map(|_| {
        c.handles[live].read_linearizable_observed(
            7,
            Instant::now() + Duration::from_millis(150),
            |_| panic!("no majority cannot query"),
            &observer,
        )
    }))
    .await;
    assert!(results.iter().all(Result::is_err));
    assert!(results.iter().all(|r| matches!(
        r,
        Err(RuntimeError::Deadline {
            outcome_unknown: false,
            ..
        }) | Err(RuntimeError::Source(MultiRaftError::NotLeader { .. }))
            | Err(RuntimeError::Source(MultiRaftError::ReadIndex(
                ReadIndexFailure::QuorumUnavailable { .. } | ReadIndexFailure::Deadline
            )))
    )));
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.stage == ReadStage::ReadIndex)
            .count(),
        3
    );
    stop(c).await;
}
#[tokio::test]
async fn per_call_fallible_query_and_observer_panic_preserve_results() {
    let c = cluster().await;
    let live = leader(&c.handles, None).await;
    let fail = c.handles[live]
        .try_read_linearizable(
            7,
            deadline(),
            |_| Err::<(), _>(io::Error::other("application rejected")),
            None,
        )
        .await;
    assert!(matches!(fail, Err(TryReadError::Application(_))));
    assert_eq!(
        c.handles[live]
            .read_linearizable_observed(7, deadline(), |f| f.value, &|_: ReadEvent| panic!(
                "isolated source observer"
            ))
            .await
            .unwrap(),
        0
    );
    stop(c).await;
}
