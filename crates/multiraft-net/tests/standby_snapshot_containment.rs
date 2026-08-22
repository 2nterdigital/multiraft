use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use multiraft_core::{ClusterConfig, NodeId, SnapshotAdvertisement, SnapshotMode};
use multiraft_fsm::{CounterFsm, StateMachine};
use multiraft_net::{wait_for_leader, MultiRaft, NodeTx, Router as FabricRouter, SharedFabric};
use multiraft_store::SnapshotCatalog;
use openraft::alias::LeaderIdOf;
use openraft::vote::RaftLeaderIdExt;
use openraft::BasicNode;
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

struct ReRegisterOnDrop {
    router: FabricRouter,
    victim_id: NodeId,
    tx: Option<NodeTx>,
}

impl ReRegisterOnDrop {
    fn unregister(router: FabricRouter, victim_id: NodeId) -> anyhow::Result<Self> {
        let tx = router
            .unregister_node(victim_id)
            .ok_or_else(|| anyhow::anyhow!("victim NodeTx was not registered"))?;
        Ok(Self {
            router,
            victim_id,
            tx: Some(tx),
        })
    }

    fn restore_now(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.router.register_node(self.victim_id, tx);
        }
    }
}

impl Drop for ReRegisterOnDrop {
    fn drop(&mut self) {
        self.restore_now();
    }
}

struct SnapshotPosition {
    index: u64,
    term: u64,
}

struct FrozenSnapshot {
    position: SnapshotPosition,
    data: Vec<u8>,
    sha256_hex: String,
}

struct DelayedSnapshot {
    data: Vec<u8>,
    last_index: u64,
    last_term: u64,
    snapshot_id: String,
    size: u64,
    sha256_hex: String,
    requests: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

fn test_config(
    node_id: NodeId,
    peer_ids: &[NodeId],
    data_dir: std::path::PathBuf,
) -> ClusterConfig {
    let mut config = ClusterConfig::for_test(node_id, peer_ids);
    config.data_dir = data_dir;
    config.snapshot_mode = SnapshotMode::StandbyOffload;
    config.snapshot_keep = 2;
    config
}

async fn start_nodes(
    temp_dir: &tempfile::TempDir,
    group: u64,
) -> anyhow::Result<(SharedFabric, Vec<MultiRaft>)> {
    let peer_ids = [1u64, 2, 3];
    let members = peer_ids.to_vec();
    let fabric = SharedFabric::new();
    let mut nodes = Vec::with_capacity(peer_ids.len());

    for node_id in peer_ids {
        let data_dir = temp_dir.path().join(format!("node-{node_id}"));
        std::fs::create_dir_all(&data_dir)?;
        nodes.push(
            fabric
                .start_node(test_config(node_id, &peer_ids, data_dir))
                .await?,
        );
    }
    for node in &nodes {
        node.create_group(group, &members).await?;
    }
    wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .ok_or_else(|| anyhow::anyhow!("leader was not elected"))?;

    Ok((fabric, nodes))
}

async fn wait_value(node: &MultiRaft, group: u64, expected: i64) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if node
            .with_fsm(group, |fsm| fsm.value(group))
            .await
            .is_some_and(|value| value == expected)
        {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            let observed = node
                .with_fsm(group, |fsm| fsm.value(group))
                .await
                .unwrap_or_default();
            anyhow::bail!(
                "node {} did not reach value {expected}; observed {observed}",
                node.node_id()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn freeze_leader_snapshot(leader: &MultiRaft, group: u64) -> anyhow::Result<FrozenSnapshot> {
    let (index, term) = leader
        .local_applied(group)
        .await
        .ok_or_else(|| anyhow::anyhow!("leader has no local applied position"))?;
    let data = leader
        .with_fsm(group, |fsm| fsm.snapshot(group))
        .await
        .ok_or_else(|| anyhow::anyhow!("leader group disappeared while freezing snapshot"))??;

    Ok(FrozenSnapshot {
        position: SnapshotPosition { index, term },
        sha256_hex: hex_sha256(&data),
        data,
    })
}

async fn shutdown_nodes(nodes: &[MultiRaft]) -> anyhow::Result<()> {
    for node in nodes {
        node.shutdown().await?;
    }
    Ok(())
}

fn hex_sha256(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn snapshot_headers(snapshot: &DelayedSnapshot) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-snapshot-index",
        HeaderValue::from_str(&snapshot.last_index.to_string()).unwrap(),
    );
    headers.insert(
        "x-snapshot-term",
        HeaderValue::from_str(&snapshot.last_term.to_string()).unwrap(),
    );
    headers.insert(
        "x-snapshot-id",
        HeaderValue::from_str(&snapshot.snapshot_id).unwrap(),
    );
    headers.insert(
        "x-snapshot-sha256",
        HeaderValue::from_str(&snapshot.sha256_hex).unwrap(),
    );
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers
}

async fn serve_delayed_snapshot(
    State(snapshot): State<Arc<DelayedSnapshot>>,
    request_headers: HeaderMap,
) -> Response {
    if snapshot.requests.fetch_add(1, Ordering::SeqCst) == 0 {
        snapshot.entered.notify_one();
        snapshot.release.notified().await;
    }

    let mut response_headers = snapshot_headers(&snapshot);
    response_headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&snapshot.size.to_string()).unwrap(),
    );

    if let Some(range) = request_headers
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
    {
        let range = range.strip_prefix("bytes=").unwrap_or_default();
        let (start, end) = range.split_once('-').unwrap_or_default();
        let start = start.parse::<u64>().unwrap_or_default();
        let end = if end.is_empty() {
            snapshot.size.saturating_sub(1)
        } else {
            end.parse::<u64>()
                .unwrap_or_else(|_| snapshot.size.saturating_sub(1))
        }
        .min(snapshot.size.saturating_sub(1));
        if start > end {
            return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
        }
        let body = snapshot.data[start as usize..=end as usize].to_vec();
        response_headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{}", snapshot.size)).unwrap(),
        );
        response_headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&body.len().to_string()).unwrap(),
        );
        return (StatusCode::PARTIAL_CONTENT, response_headers, body).into_response();
    }

    (StatusCode::OK, response_headers, snapshot.data.clone()).into_response()
}

async fn spawn_delayed_server(
    snapshot: DelayedSnapshot,
) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new()
        .route("/snapshots/0/latest", get(serve_delayed_snapshot))
        .with_state(Arc::new(snapshot));
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((address, server))
}

async fn serve_counting_snapshot(State(snapshot): State<Arc<DelayedSnapshot>>) -> Response {
    snapshot.requests.fetch_add(1, Ordering::SeqCst);
    let mut headers = snapshot_headers(&snapshot);
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&snapshot.size.to_string()).unwrap(),
    );
    (StatusCode::OK, headers, snapshot.data.clone()).into_response()
}

async fn spawn_counting_server(
    snapshot: DelayedSnapshot,
) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new()
        .route("/snapshots/0/latest", get(serve_counting_snapshot))
        .with_state(Arc::new(snapshot));
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((address, server))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_primitive_delayed_pull_is_rejected_before_fetch() -> anyhow::Result<()> {
    let group = 0u64;
    let temp_dir = tempfile::tempdir()?;
    let (_fabric, nodes) = start_nodes(&temp_dir, group).await?;
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .ok_or_else(|| anyhow::anyhow!("leader was not elected"))?;
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .ok_or_else(|| anyhow::anyhow!("leader handle was not found"))?;
    let victim = nodes
        .iter()
        .find(|node| node.node_id() != leader_id)
        .ok_or_else(|| anyhow::anyhow!("victim handle was not found"))?;

    leader.propose(group, CounterFsm::encode_add(10, 1)).await?;
    for node in &nodes {
        wait_value(node, group, 10).await?;
    }
    let frozen = freeze_leader_snapshot(leader, group).await?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let request_count = Arc::new(AtomicUsize::new(0));
    let (address, server) = spawn_delayed_server(DelayedSnapshot {
        size: frozen.data.len() as u64,
        data: frozen.data,
        last_index: frozen.position.index,
        last_term: frozen.position.term,
        snapshot_id: "direct-snapshot-10".into(),
        sha256_hex: frozen.sha256_hex,
        requests: request_count.clone(),
        entered: entered.clone(),
        release: release.clone(),
    })
    .await?;
    let fetch_url = format!("http://{address}/snapshots/0/latest");

    leader.propose(group, CounterFsm::encode_add(1, 2)).await?;
    wait_value(victim, group, 11).await?;

    let pull = victim.pull_and_install_snapshot(group, &fetch_url);
    tokio::pin!(pull);
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    let early_result = tokio::select! {
        result = &mut pull => Some(result),
        _ = &mut entered_wait => None,
    };
    if let Some(result) = early_result {
        assert!(
            matches!(
                result,
                Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
            ),
            "desired contract: live Standby installation is rejected before effect"
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        wait_value(victim, group, 11).await?;
        server.abort();
        shutdown_nodes(&nodes).await?;
        drop(temp_dir);
        return Ok(());
    }
    release.notify_one();
    let result = pull.await;
    wait_value(victim, group, 10).await?;
    server.abort();
    shutdown_nodes(&nodes).await?;
    drop(temp_dir);
    eprintln!("DIRECT_PRIMITIVE_OBSERVED_11_THEN_10");
    assert!(
        matches!(
            result,
            Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
        ),
        "desired contract: live Standby installation is rejected before effect"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c42_ad_recovery_is_rejected_before_ad_read_or_fetch() -> anyhow::Result<()> {
    let group = 0u64;
    let temp_dir = tempfile::tempdir()?;
    let (fabric, nodes) = start_nodes(&temp_dir, group).await?;
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .ok_or_else(|| anyhow::anyhow!("leader was not elected"))?;
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .ok_or_else(|| anyhow::anyhow!("leader handle was not found"))?;
    let victim_id = nodes
        .iter()
        .map(MultiRaft::node_id)
        .find(|node_id| *node_id != leader_id)
        .ok_or_else(|| anyhow::anyhow!("victim id was not found"))?;
    let victim = nodes
        .iter()
        .find(|node| node.node_id() == victim_id)
        .ok_or_else(|| anyhow::anyhow!("victim handle was not found"))?;

    let mut gate = ReRegisterOnDrop::unregister(fabric.router().clone(), victim_id)?;
    // Survivors now write/freeze value 10. Victim created Group first and is below ad.
    leader.propose(group, CounterFsm::encode_add(10, 1)).await?;
    for node in nodes.iter().filter(|node| node.node_id() != victim_id) {
        wait_value(node, group, 10).await?;
    }
    let frozen = freeze_leader_snapshot(leader, group).await?;
    let snapshot_pos = frozen.position;
    let snapshot_data = frozen.data;
    let ad_http_entered = Arc::new(Notify::new());
    let release_old_ten = Arc::new(Notify::new());
    let request_count = Arc::new(AtomicUsize::new(0));
    let (address, server) = spawn_delayed_server(DelayedSnapshot {
        size: snapshot_data.len() as u64,
        sha256_hex: hex_sha256(&snapshot_data),
        data: snapshot_data.clone(),
        last_index: snapshot_pos.index,
        last_term: snapshot_pos.term,
        snapshot_id: "c42-snapshot-10".into(),
        requests: request_count.clone(),
        entered: ad_http_entered.clone(),
        release: release_old_ten.clone(),
    })
    .await?;
    let delayed_url = format!("http://{address}/snapshots/0/latest");

    let applied_before = victim.local_applied(group).await;
    let (local_index, local_term) = applied_before.unwrap_or((0, 0));
    let ad = SnapshotAdvertisement {
        group,
        last_index: snapshot_pos.index,
        last_term: snapshot_pos.term,
        snapshot_id: "c42-snapshot-10".into(),
        size: snapshot_data.len() as u64,
        sha256_hex: hex_sha256(&snapshot_data),
        fetch_url: delayed_url.clone(),
    };
    assert!((ad.last_term, ad.last_index) > (local_term, local_index));
    let victim_value_before = victim
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .expect("victim fsm before recovery");
    victim.record_snapshot_ad(ad);
    let recovery = victim.try_recover_from_standby_ads(group);
    tokio::pin!(recovery);
    let entered_wait = ad_http_entered.notified();
    tokio::pin!(entered_wait);
    let early_result = tokio::select! {
        result = &mut recovery => Some(result),
        _ = &mut entered_wait => None,
    };
    if let Some(result) = early_result {
        assert!(
            matches!(
                result,
                Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
            ),
            "desired contract: live Standby installation is rejected before effect"
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        assert_eq!(
            victim.with_fsm(group, |fsm| fsm.value(group)).await,
            Some(victim_value_before)
        );
        assert_eq!(victim.local_applied(group).await, applied_before);
        gate.restore_now();
        server.abort();
        shutdown_nodes(&nodes).await?;
        drop(temp_dir);
        return Ok(());
    }
    gate.restore_now();
    leader.propose(group, CounterFsm::encode_add(1, 2)).await?;
    wait_value(victim, group, 11).await?;
    release_old_ten.notify_one();
    let result = recovery.await;
    wait_value(victim, group, 10).await?;
    server.abort();
    shutdown_nodes(&nodes).await?;
    drop(temp_dir);
    eprintln!("C42_AD_PRECHECK_OBSERVED_11_THEN_10");
    assert!(
        matches!(
            result,
            Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
        ),
        "desired contract: live Standby installation is rejected before effect"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ad_recovery_eligible_ad_is_rejected_before_ad_read_or_fetch() -> anyhow::Result<()> {
    let group = 0;
    let temp_dir = tempfile::tempdir()?;
    let (fabric, nodes) = start_nodes(&temp_dir, group).await?;
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .ok_or_else(|| anyhow::anyhow!("leader was not elected"))?;
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .unwrap();
    let victim_id = nodes
        .iter()
        .map(MultiRaft::node_id)
        .find(|id| *id != leader_id)
        .unwrap();
    let victim = nodes
        .iter()
        .find(|node| node.node_id() == victim_id)
        .unwrap();
    let mut gate = ReRegisterOnDrop::unregister(fabric.router().clone(), victim_id)?;
    leader.propose(group, CounterFsm::encode_add(10, 1)).await?;
    for node in nodes.iter().filter(|node| node.node_id() != victim_id) {
        wait_value(node, group, 10).await?;
    }
    let frozen = freeze_leader_snapshot(leader, group).await?;
    let request_count = Arc::new(AtomicUsize::new(0));
    let (address, server) = spawn_counting_server(DelayedSnapshot {
        size: frozen.data.len() as u64,
        data: frozen.data,
        last_index: frozen.position.index,
        last_term: frozen.position.term,
        snapshot_id: "eligible-ad".into(),
        sha256_hex: frozen.sha256_hex,
        requests: request_count.clone(),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    })
    .await?;
    victim.record_snapshot_ad(SnapshotAdvertisement {
        group,
        last_index: 10,
        last_term: 1,
        snapshot_id: "eligible-ad".into(),
        size: 1,
        sha256_hex: "not-used-by-ad-selection".into(),
        fetch_url: format!("http://{address}/snapshots/0/latest"),
    });
    let result = victim.try_recover_from_standby_ads(group).await;
    gate.restore_now();
    server.abort();
    shutdown_nodes(&nodes).await?;
    drop(temp_dir);
    eprintln!(
        "AD_RECOVERY_BASE_EFFECT_OBSERVED requests={}",
        request_count.load(Ordering::SeqCst)
    );
    assert!(matches!(
        result,
        Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
    ));
    assert_eq!(request_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn catalog_install_is_rejected_before_corrupt_catalog_read() -> anyhow::Result<()> {
    let group = 0;
    let temp_dir = tempfile::tempdir()?;
    let catalog = SnapshotCatalog::new(temp_dir.path().join("corrupt-catalog"), 2);
    catalog.write(group, 10, 1, "10-1", b"ten")?;
    std::fs::write(
        catalog.root().join("0").join("10-1").join("meta.json"),
        b"corrupt",
    )?;
    let node = MultiRaft::start(test_config(1, &[1], temp_dir.path().join("node"))).await?;
    node.create_group(group, &[1]).await?;
    let value_before = node
        .with_fsm(group, |fsm| fsm.value(group))
        .await
        .expect("catalog fixture fsm");
    let applied_before = node.local_applied(group).await;
    let result = node.try_install_from_standby_catalog(group, &catalog).await;
    eprintln!("CATALOG_INSTALL_BASE_EFFECT_OBSERVED");
    assert!(matches!(
        result,
        Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
    ));
    assert_eq!(
        node.with_fsm(group, |fsm| fsm.value(group)).await,
        Some(value_before)
    );
    assert_eq!(node.local_applied(group).await, applied_before);
    node.shutdown().await?;
    drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_durable_install_is_rejected_before_fsm_effect() -> anyhow::Result<()> {
    let group = 0;
    let temp_dir = tempfile::tempdir()?;
    let (_fabric, nodes) = start_nodes(&temp_dir, group).await?;
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .unwrap();
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .unwrap();
    let victim = nodes
        .iter()
        .find(|node| node.node_id() != leader_id)
        .unwrap();
    leader.propose(group, CounterFsm::encode_add(10, 1)).await?;
    for node in &nodes {
        wait_value(node, group, 10).await?;
    }
    let frozen = freeze_leader_snapshot(leader, group).await?;
    leader.propose(group, CounterFsm::encode_add(1, 2)).await?;
    wait_value(victim, group, 11).await?;
    let result = victim
        .install_durable_snapshot(
            group,
            frozen.position.index,
            frozen.position.term,
            frozen.data,
        )
        .await;
    eprintln!("PUBLIC_DURABLE_INSTALL_BASE_EFFECT_OBSERVED");
    assert!(matches!(
        result,
        Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
    ));
    wait_value(victim, group, 11).await?;
    shutdown_nodes(&nodes).await?;
    drop(temp_dir);
    Ok(())
}

#[tokio::test]
async fn manual_daisy_is_rejected_before_config_or_upstream() -> anyhow::Result<()> {
    let group = 0;
    let temp_dir = tempfile::tempdir()?;
    let upstream_calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new().route(
        "/snapshots/0/latest",
        get({
            let upstream_calls = upstream_calls.clone();
            move || {
                let upstream_calls = upstream_calls.clone();
                async move {
                    upstream_calls.fetch_add(1, Ordering::SeqCst);
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }),
    );
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let mut config = test_config(1, &[1], temp_dir.path().join("node"));
    config.daisy_upstream_base = Some(format!("http://{address}"));
    let node = MultiRaft::start(config).await?;
    let result = node.sync_from_daisy_upstream(group).await;
    server.abort();
    node.shutdown().await?;
    drop(temp_dir);
    eprintln!(
        "MANUAL_DAISY_BASE_EFFECT_OBSERVED calls={}",
        upstream_calls.load(Ordering::SeqCst)
    );
    assert!(matches!(
        result,
        Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
    ));
    assert_eq!(upstream_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn snapshot_mode_disabled_native_complete_snapshot_rpc() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = temp_dir.path().join("node");
    config.snapshot_mode = SnapshotMode::Disabled;
    let node = MultiRaft::start(config).await?;
    node.create_group(0, &[1]).await?;
    let leader_id = LeaderIdOf::<multiraft_core::TypeConfig>::new_committed(7, 2);
    let last_log_id = multiraft_core::typ::LogId::new(leader_id, 42);
    let membership = multiraft_core::typ::Membership::new(
        vec![BTreeSet::from([1u64])],
        BTreeMap::from([(
            1u64,
            BasicNode {
                addr: "127.0.0.1:19001".into(),
            },
        )]),
    )?;
    let meta = multiraft_core::typ::SnapshotMeta {
        last_log_id: Some(last_log_id),
        last_membership: multiraft_core::typ::StoredMembership::new(Some(last_log_id), membership),
        snapshot_id: "disabled-native-complete-42".into(),
    };
    let data = CounterFsm::new().snapshot(0)?;
    node.router()
        .send_snapshot(
            1,
            0,
            multiraft_core::typ::Vote::new_committed(7, 2),
            meta,
            data,
        )
        .await?;
    assert_eq!(node.local_applied(0).await, Some((42, 7)));
    assert_eq!(node.with_fsm(0, |fsm| fsm.value(0)).await, Some(0));
    assert!(node.snapshot_catalog().is_none());
    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn snapshot_mode_disabled_startup_ignores_preseeded_lab_catalog() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("node");
    let catalog = SnapshotCatalog::new(data_dir.join("snapshots"), 2);
    catalog.write(0, 10, 3, "10-3", b"preseeded")?;
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = data_dir;
    config.snapshot_mode = SnapshotMode::Disabled;
    let node = MultiRaft::start(config).await?;
    node.create_group(0, &[1]).await?;
    assert!(node.snapshot_catalog().is_none());
    assert_eq!(node.with_fsm(0, |fsm| fsm.value(0)).await, Some(0));
    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn standby_ad_consumer_is_rejected_before_effect() -> anyhow::Result<()> {
    let node = MultiRaft::start(ClusterConfig::for_test(1, &[1])).await?;
    let result = node.try_recover_from_standby_ads(0).await;
    assert!(matches!(
        result,
        Err(multiraft_core::MultiRaftError::LiveSnapshotInstallUnsupported)
    ));
    node.shutdown().await?;
    Ok(())
}
