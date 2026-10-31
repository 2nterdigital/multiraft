//! Node 4 owns its native endpoint, target directory and application FSM exclusively.
use super::{
    catalog::FileAuthority,
    pipe::{self, Reply, Request, Setup},
    source::{self, deadline, GROUP},
};
use multiraft_net::portable::NativeTargetEndpoint;
use std::{
    io::{self, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
pub async fn run() {
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    output.write_all(pipe::MAGIC).unwrap();
    output.flush().unwrap();
    let setup: Setup = serde_json::from_slice(&pipe::read_frame(&mut input).unwrap()).unwrap();
    let mut config = multiraft_core::ClusterConfig::for_test(4, &[1, 2, 3]);
    config.peers = setup.peers;
    config.data_dir = setup.root;
    config.log_storage_backend = multiraft_core::LogStorageBackend::RocksDb;
    config.snapshot_mode = multiraft_core::SnapshotMode::NativeDurable;
    config.retain_log_entries = 0;
    config.enable_stale_queries = true;
    config.raft_transport_fingerprint = Some(source::DOMAIN);
    let count = Arc::new(AtomicUsize::new(0));
    let node = source::start(config, count.clone()).await;
    let authority = Arc::new(FileAuthority::open(&setup.catalog));
    let target = node
        .reserve_controlled_target(
            GROUP,
            setup.specification.clone(),
            vec![4],
            authority,
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "reservation must not construct FSM"
    );
    let endpoint = NativeTargetEndpoint::new(target, setup.specification).unwrap();
    pipe::write_frame(
        &mut output,
        &serde_json::to_vec(&Reply::Identity(endpoint.identity().clone())).unwrap(),
    )
    .unwrap();
    loop {
        let frame = match pipe::read_frame(&mut input) {
            Ok(frame) => frame,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("bounded child frame: {e}"),
        };
        let reply = match serde_json::from_slice(&frame).unwrap() {
            Request::Native(bytes) => Reply::Native(
                endpoint
                    .dispatch(&bytes)
                    .await
                    .expect("native DTO dispatch"),
            ),
            Request::Diagnostics => Reply::Diagnostics {
                rows: node
                    .with_fsm(GROUP, |fsm| fsm.payloads().unwrap())
                    .await
                    .unwrap_or_default(),
                creations: count.load(Ordering::SeqCst),
                pid: std::process::id(),
            },
            Request::ReadBarrier {
                term,
                node: leader,
                index,
                rows,
            } => {
                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                    loop {
                        let view = node
                            .read_applied_with_watermark(GROUP, |fsm| fsm.payloads().unwrap())
                            .await
                            .unwrap();
                        if view.applied.is_some_and(|id| {
                            id.index > index
                                || (id.index == index && id.term == term && id.node_id == leader)
                        }) {
                            assert_eq!(
                                view.value, rows,
                                "target read covers leader quorum-confirmed application barrier"
                            );
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                })
                .await
                .expect("target applies confirmed read barrier");
                Reply::Barrier
            }
            Request::Stop => {
                node.shutdown().await.unwrap();
                pipe::write_frame(&mut output, &serde_json::to_vec(&Reply::Stopped).unwrap())
                    .unwrap();
                return;
            }
        };
        pipe::write_frame(&mut output, &serde_json::to_vec(&reply).unwrap()).unwrap();
    }
    // EOF does not cancel any native work already dispatched; shutdown joins its owners.
    node.shutdown().await.unwrap();
}
