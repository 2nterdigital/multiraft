//! Named backend recovery shapes, with three OS processes and direct victim oracles.
#[path = "native_rf3_harness/mod.rs"]
mod harness;
use harness::{Cluster, SERIAL};
use serde_json::json;

#[test]
#[ignore = "G1 dedicated laboratory only; run the exact named case with --ignored"]
fn isolated_local_post_purge_recovery_has_no_live_peer_source() {
    let _serial = SERIAL.lock().unwrap();
    let mut cluster = Cluster::new("g1-isolated-");
    cluster.start_all();
    for id in 1..=10 {
        cluster.add(1, id);
    }
    cluster.wait_all(10);
    cluster.compact(2);
    cluster.add(7, 11);
    cluster.wait_all(17);
    cluster.stop_all();
    cluster.start(2);
    let state = cluster.wait_value(2, 17);
    assert!(state["purged"].as_u64().is_some());
    assert!(state["durable_snapshot"].as_u64().is_some());
    println!("NATIVE_RECOVERY source=local_snapshot_plus_suffix live_peers=0 node=2 expected=17");
    cluster.stop_all();
}

#[test]
#[ignore = "G1 dedicated laboratory only; run the exact named case with --ignored"]
fn every_voter_cold_reopens_after_purge_then_proposal_and_readindex_succeed() {
    let _serial = SERIAL.lock().unwrap();
    let mut cluster = Cluster::new("g1-all-voter-");
    cluster.start_all();
    for id in 1..=10 {
        cluster.add(1, id);
    }
    cluster.wait_all(10);
    for id in 1..=3 {
        cluster.compact(id);
    }
    cluster.add(7, 11);
    cluster.wait_all(17);
    cluster.stop_all();
    cluster.start_all();
    cluster.wait_all(17);
    cluster.add(3, 12);
    cluster.wait_all(20);
    let leader = cluster.leader();
    let read = cluster
        .nodes
        .get_mut(&leader)
        .unwrap()
        .call(json!({"op":"read"}));
    assert_eq!(read["value"], 20);
    println!("NATIVE_RECOVERY source=local_snapshot_plus_suffix nodes=3 expected=17 subsequent_proposal_and_readindex=20");
    cluster.stop_all();
}

#[test]
#[ignore = "G1 dedicated laboratory only; run the exact named case with --ignored"]
fn native_peer_snapshot_receiver_later_reopens_without_any_live_peer() {
    let _serial = SERIAL.lock().unwrap();
    let mut cluster = Cluster::new("g1-peer-install-");
    cluster.start_all();
    for id in 1..=3 {
        cluster.add(1, id);
    }
    cluster.wait_all(3);
    let old = cluster.status(3)["applied"].as_u64().unwrap();
    cluster.kill(3);
    for id in 4..=13 {
        cluster.add(1, id);
    }
    cluster.wait_all(13);
    cluster.compact(1);
    cluster.compact(2);
    cluster.start(3);
    let installed = cluster.wait_value(3, 13);
    assert!(
        installed["durable_snapshot"]
            .as_u64()
            .is_some_and(|cut| cut > old),
        "receiver must actually install beyond its old prefix: {installed}"
    );
    cluster.add(7, 14);
    cluster.wait_all(20);
    cluster.stop_all();
    cluster.start(3);
    cluster.wait_value(3, 20);
    println!("NATIVE_RECOVERY source=native_grpc_snapshot_then_local_snapshot_plus_suffix node=3 old_applied={old} expected=20 live_peers_at_final_reopen=0");
    cluster.stop_all();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "OS-process fixture launched only by the named RF3 tests"]
async fn rf3_process_node() {
    use harness::ChildConfig;
    use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
    use multiraft_fsm::CounterFsm;
    use multiraft_net::MultiRaft;
    use serde_json::Value;
    use std::io::{BufRead, Write};
    let config: ChildConfig =
        serde_json::from_str(&std::env::var("NATIVE_RF3_NODE_CONFIG").unwrap()).unwrap();
    harness::require_lab(&config.root);
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();
    let mut cfg = ClusterConfig::for_test(config.id, &[1, 2, 3]);
    cfg.peers = config
        .peers
        .iter()
        .map(|(id, addr)| (*id, addr.parse().unwrap()))
        .collect();
    cfg.data_dir = config.root;
    cfg.file_log_sync_level = FileLogSyncLevel::Data;
    cfg.snapshot_mode = SnapshotMode::NativeDurable;
    cfg.retain_log_entries = 0;
    cfg.enable_stale_queries = true;
    let node = MultiRaft::start_grpc(cfg).await.unwrap();
    node.create_group(7, &[1, 2, 3]).await.unwrap();
    println!("NATIVE_REPLY {}", json!({"ready":true,"node":config.id}));
    std::io::stdout().flush().unwrap();
    for line in std::io::stdin().lock().lines() {
        let command: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let op = command["op"].as_str().unwrap();
        let reply = match op {
            "status" => {
                let value = node.read_stale(7, |fsm| fsm.value(7)).await.unwrap();
                let state = node.local_storage_status(7).await.unwrap();
                let memory = std::fs::read_to_string("/proc/self/status").unwrap();
                let kib = |name: &str| {
                    memory
                        .lines()
                        .find(|line| line.starts_with(name))
                        .and_then(|line| line.split_whitespace().nth(1))
                        .and_then(|value| value.parse::<u64>().ok())
                };
                json!({"node":config.id,"value":value.value,"applied":value.applied_index,"is_leader":node.is_leader(7),"durable_snapshot":state.durable_snapshot.and_then(|s| s.last_log_id).map(|p|p.index),"purged":state.purged.map(|p|p.index),"progress":format!("{:?}",state.progress),"retained_log_bytes":state.retained_log_bytes,"pid":std::process::id(),"rss_kib":kib("VmRSS:"),"peak_rss_kib":kib("VmHWM:")})
            }
            "add" => match node
                .propose(
                    7,
                    CounterFsm::encode_add(
                        command["delta"].as_i64().unwrap(),
                        command["idem"].as_u64().unwrap(),
                    ),
                )
                .await
            {
                Ok(result) => json!({"index":result.index}),
                Err(error) => json!({"error":format!("{error:?}")}),
            },
            "compact" => match node.request_compaction(7).await {
                Ok(result) => json!({"target":result.target.map(|p|p.index)}),
                Err(error) => json!({"error":format!("{error:?}")}),
            },
            "read" => match node.read_linearizable(7, |fsm| fsm.value(7)).await {
                Ok(value) => json!({"value":value}),
                Err(error) => json!({"error":format!("{error:?}")}),
            },
            "stop" => match node.shutdown().await {
                Ok(()) => json!({"stopped":true}),
                Err(error) => json!({"error":format!("{error:?}")}),
            },
            _ => panic!("unknown fixture command"),
        };
        println!("NATIVE_REPLY {reply}");
        std::io::stdout().flush().unwrap();
        if op == "stop" {
            break;
        }
    }
}
