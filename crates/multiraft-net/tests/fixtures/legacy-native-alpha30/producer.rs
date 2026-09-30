use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{CompactionProgress, MultiRaft};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let root = std::env::args().nth(1).expect("output root");
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.retain_log_entries = 2;
    let node = MultiRaft::start(config).await.expect("old source start");
    node.create_group(7, &[1]).await.expect("group");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("leader");
    for id in 1..=10 {
        node.propose(7, CounterFsm::encode_add(1, id)).await.expect("write");
    }
    node.request_compaction(7).await.expect("compaction");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = node.local_storage_status(7).await.expect("status");
            if status.progress == CompactionProgress::CompletedObserved {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("durable snapshot/purge");
    node.propose(7, CounterFsm::encode_add(5, 11)).await.expect("tail");
    assert_eq!(node.read_linearizable(7, |fsm| fsm.value(7)).await.unwrap(), 15);
    node.shutdown().await.expect("old source shutdown");
    println!("old source fe832257b0c744faf93f9c7a4ab1da63d37d6a46: group7 value15 snapshot10+tail5");
}
