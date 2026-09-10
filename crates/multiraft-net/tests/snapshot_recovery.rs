//! Positive native snapshot recovery for a lagging voter across a purged prefix.
//!
//! The original W1/W2 load and direct victim oracle are retained. Both survivors
//! explicitly build durable checkpoints and purge beyond the victim's prefix
//! before it restarts. This is an in-process conformance test, not a G1 receipt.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{wait_for_leader, MultiRaft, SharedFabric};
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// Entries the victim observes before it is stopped.
const W1: u64 = 100;
/// Original workload committed while the victim is down.
const W2: u64 = 5_600;

#[derive(Clone, Default)]
struct RecoveryEventCapture {
    events: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

impl RecoveryEventCapture {
    fn matching(&self, operation: &str) -> Vec<BTreeMap<String, String>> {
        self.events
            .lock()
            .expect("event capture lock")
            .iter()
            .filter(|event| event.get("operation").map(String::as_str) == Some(operation))
            .cloned()
            .collect()
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for RecoveryEventCapture
where
    S: Subscriber,
{
    fn enabled(&self, metadata: &Metadata<'_>, _context: Context<'_, S>) -> bool {
        metadata.target() == "multiraft::recovery"
    }

    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if event.metadata().target() != "multiraft::recovery" {
            return;
        }
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.events
            .lock()
            .expect("event capture lock")
            .push(visitor.fields);
    }
}

fn cfg(id: u64, peers: &[u64], dir: std::path::PathBuf) -> ClusterConfig {
    let mut config = ClusterConfig::for_test(id, peers);
    config.data_dir = dir;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.retain_log_entries = 0;
    config
}

/// Propose one deduplicated +1; safe to retry blindly because the
/// `CounterFsm` applies each `idem` exactly once.
async fn propose_inc(nodes: &[&MultiRaft], group: u64, idem: u64) {
    let data = CounterFsm::encode_add(1, idem);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut order: Vec<&MultiRaft> = Vec::with_capacity(nodes.len());
        for n in nodes {
            if n.is_leader(group) {
                order.push(n);
            }
        }
        for n in nodes {
            if !n.is_leader(group) {
                order.push(n);
            }
        }
        for n in order {
            if n.propose(group, data.clone()).await.is_ok() {
                return;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("propose timed out at idem={idem}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Keep one Raft entry per original command, with bounded concurrent submission.
async fn propose_batch_inc(nodes: &[&MultiRaft], group: u64, ids: &[u64]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        for node in nodes.iter().filter(|node| node.is_leader(group)) {
            let data = ids
                .iter()
                .map(|&idem| CounterFsm::encode_add(1, idem))
                .collect();
            if let Ok(results) = node.propose_batch(group, data).await {
                assert_eq!(results.len(), ids.len());
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bounded Counter batch timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn parse_u64_field(event: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    event.get(key).and_then(|raw| {
        raw.trim_matches(|c: char| !c.is_ascii_digit())
            .parse::<u64>()
            .ok()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_voter_recovers_via_native_snapshot_across_purge() {
    let capture = RecoveryEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    // Global (not thread-local) because this test runs on a multi-thread
    // runtime: diagnostics are emitted from tokio worker threads. This test
    // binary contains exactly one test, so the once-per-process limit holds.
    tracing::subscriber::set_global_default(subscriber)
        .expect("install global diagnostics subscriber");

    let peer_ids = [1u64, 2, 3];
    let members = peer_ids.to_vec();
    let group = 1u64;
    let data_root = tempfile::tempdir().expect("create temporary data root");
    let dirs: Vec<std::path::PathBuf> = peer_ids
        .iter()
        .map(|&id| {
            let dir = data_root.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        })
        .collect();

    let fabric = SharedFabric::new();
    let mut nodes = Vec::new();
    for (i, &id) in peer_ids.iter().enumerate() {
        nodes.push(
            fabric
                .start_node(cfg(id, &peer_ids, dirs[i].clone()))
                .await
                .expect("start voter"),
        );
    }
    for n in &nodes {
        n.create_group(group, &members).await.expect("create_group");
    }
    wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("initial leader");

    // Phase 1: W1 entries; every node (victim included) must apply them.
    let all: Vec<&MultiRaft> = nodes.iter().collect();
    for idem in 1..=W1 {
        propose_inc(&all, group, idem).await;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut applied = 0;
        for n in &nodes {
            if n.with_fsm(group, |fsm| fsm.value(group)).await == Some(W1 as i64) {
                applied += 1;
            }
        }
        if applied == nodes.len() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "all voters must apply the W1 prefix before the victim stops"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop a non-leader victim with its W1-long retained prefix.
    let victim_idx = nodes
        .iter()
        .position(|n| !n.is_leader(group))
        .expect("a non-leader victim exists");
    let victim_id = nodes[victim_idx].node_id();
    nodes[victim_idx].shutdown().await.expect("shutdown victim");
    let survivors: Vec<&MultiRaft> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != victim_idx)
        .map(|(_, n)| n)
        .collect();

    // Phase 2: preserve every original W2 command while batching disk sync work.
    let ids: Vec<_> = ((W1 + 1)..=(W1 + W2)).collect();
    for batch in ids.chunks(128) {
        propose_batch_inc(&survivors, group, batch).await;
    }
    for node in &survivors {
        node.request_compaction(group)
            .await
            .expect("explicit durable compaction");
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let status = node.local_storage_status(group).await.unwrap();
            if status.progress == multiraft_net::CompactionProgress::CompletedObserved {
                assert!(status.purged.is_some_and(|id| id.index > W1 + 50));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "survivor compaction unfinished: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Await the snapshot-build and purge diagnostics.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let purge_index = loop {
        let builds = capture.matching("native_snapshot_build");
        let purges = capture.matching("file_log_purge");
        let best_purge = purges
            .iter()
            .filter(|e| parse_u64_field(e, "removed_entries").unwrap_or(0) > 0)
            .filter_map(|e| parse_u64_field(e, "requested_purge_index"))
            .max();
        if !builds.is_empty() {
            if let Some(index) = best_purge {
                break index;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "native snapshot build + log purge must occur after manual durable compaction (builds={}, purges={})",
            builds.len(),
            purges.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        purge_index > W1 + 50,
        "the purged prefix (up to {purge_index}) must reach far beyond the \
         victim's retained prefix (~{W1}) so log replication cannot supply \
         the gap and only the native install-snapshot channel remains"
    );

    // Phase 3: restart the victim on its same root through the shared fabric.
    let restarted = fabric
        .start_node(cfg(victim_id, &peer_ids, dirs[victim_idx].clone()))
        .await
        .expect("restart victim");
    restarted
        .create_group(group, &members)
        .await
        .expect("create_group after victim restart");

    // Direct victim oracle: the victim's OWN local FSM must converge to the
    // full committed value, which is only reachable through the snapshot.
    let expected = (W1 + W2) as i64;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        if restarted.with_fsm(group, |fsm| fsm.value(group)).await == Some(expected) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "restarted victim must recover the full value {expected} across \
             the purged prefix via the native install-snapshot channel"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        restarted
            .local_storage_status(group)
            .await
            .unwrap()
            .durable_snapshot
            .is_some(),
        "victim must durably install its peer snapshot"
    );

    // A ReadIndex-backed read agrees with the recovered value.
    let survivors_after: Vec<&MultiRaft> = survivors.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let read = loop {
        let mut value = None;
        for n in &survivors_after {
            if let Ok(v) = n.read_linearizable(group, |fsm| fsm.value(group)).await {
                value = Some(v);
                break;
            }
        }
        if let Some(v) = value {
            break v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a linearizable read must succeed after victim recovery"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        read, expected,
        "linearizable read must match the recovered value"
    );

    eprintln!(
        "snapshot_recovery: victim={victim_id} purge_index={purge_index} \
         builds={} value={expected}",
        capture.matching("native_snapshot_build").len()
    );

    restarted
        .shutdown()
        .await
        .expect("shutdown restarted victim");
    for n in survivors_after {
        n.shutdown().await.expect("shutdown survivor");
    }
}
