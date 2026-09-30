use super::*;
use axum::extract::State;
use axum::routing::get;
use axum::Router as AxumRouter;
use futures::FutureExt;

struct UpstreamProbe {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
}

async fn counting_upstream(State(probe): State<Arc<UpstreamProbe>>) -> axum::http::StatusCode {
    probe.calls.fetch_add(1, AtomicOrdering::SeqCst);
    probe.entered.notify_one();
    axum::http::StatusCode::INTERNAL_SERVER_ERROR
}

#[tokio::test]
async fn background_daisy_is_rejected_before_task_creation() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let upstream_entered = Arc::new(Notify::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = AxumRouter::new()
        .route("/snapshots/0/latest", get(counting_upstream))
        .with_state(Arc::new(UpstreamProbe {
            calls: calls.clone(),
            entered: upstream_entered.clone(),
        }));
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let mut config = ClusterConfig::for_test(1, &[1]);
    config.daisy_upstream_base = Some(format!("http://{address}"));
    config.daisy_sync_interval_ms = 1;
    let mut node = MultiRaft::start(config).await?;
    let probe = DaisySpawnProbe::default();
    node.set_daisy_spawn_probe_for_test(probe.clone());
    let result: Result<(), MultiRaftError> = node.spawn_daisy_sync_loop(vec![0]);
    server.abort();
    node.shutdown().await?;
    assert!(matches!(
        result,
        Err(MultiRaftError::LiveSnapshotInstallUnsupported)
    ));
    assert_eq!(probe.spawn_attempts.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(probe.ticks.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_observer_does_not_wake_on_data_progress() -> anyhow::Result<()> {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs).await?;
    let group = 99;

    for node in &nodes {
        node.create_group(group, &peer_ids).await?;
    }

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader elected");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let raft = leader.raft(group).expect("leader raft");
    let mut raw_full_metrics = raft.metrics();
    let _ = raw_full_metrics.borrow_and_update().clone();
    let (_initial, mut observer) = leader.observe_group(group).expect("observe group");

    let proposed = leader.propose(group, CounterFsm::encode_add(1, 1)).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last_applied = None;

    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out waiting for raw full metrics last_applied >= {}; last observed {:?}",
                proposed.index, last_applied
            );
        }

        match tokio::time::timeout(remaining, raw_full_metrics.changed()).await {
            Ok(Ok(())) => {
                let metrics = {
                    let borrowed = raw_full_metrics.borrow_and_update();
                    borrowed.clone()
                };
                last_applied = metrics.last_applied.as_ref().map(|log_id| log_id.index());
                if last_applied >= Some(proposed.index) {
                    break;
                }
            }
            Ok(Err(_)) => panic!("raw full metrics closed before data progress"),
            Err(_) => panic!(
                "timed out waiting for raw full metrics last_applied >= {}; last observed {:?}",
                proposed.index, last_applied
            ),
        }
    }

    assert!(
        observer.changed().now_or_never().is_none(),
        "server-metrics observer must not wake on data-only progress"
    );
    Ok(())
}
