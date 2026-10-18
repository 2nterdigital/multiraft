use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_slot::*;
use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    time::Duration,
};
use tokio::time::Instant;

fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn layout() -> ClusterLayout {
    ClusterLayout::new(LayoutConfig {
        cluster_id: "transport-test".into(),
        format_version: FORMAT_VERSION,
        route_version: RouteVersion::Sha256Bucket256V1,
        bucket_count: BUCKET_COUNT,
        slot_count: 1,
        group_id_base: 100,
        nodes: (1..=3)
            .map(|node_id| NodeAddress {
                node_id,
                raft: address(),
                application_rpc: address(),
            })
            .collect(),
        buckets: contiguous_bucket_table(1).unwrap(),
        slots: vec![SlotAssignment {
            slot: SlotId(0),
            voters: vec![1, 2, 3],
            preferred_initializer: 1,
            soft_preferred_leader: 1,
        }],
    })
    .unwrap()
}
fn transport() -> SlotTransportConfig {
    SlotTransportConfig {
        nodes: (1..=3)
            .map(|node_id| SlotTransportNode {
                node_id,
                raft: format!("localhost:{}", address().port()),
                application_rpc: format!("localhost:{}", address().port()),
            })
            .collect(),
        raft_bind: SocketAddr::from(([0, 0, 0, 0], address().port())),
        application_rpc_bind: SocketAddr::from(([0, 0, 0, 0], address().port())),
    }
}
fn root(label: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp/transport-tests")
        .join(format!("{}-{label}", std::process::id()))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
#[derive(Default)]
struct Fsm;
impl StateMachine for Fsm {
    type Error = std::io::Error;
    fn apply(&mut self, _: u64, _: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        Ok(ApplyOut {
            effects: bytes.to_vec(),
        })
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _: u64, _: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl SlotStateMachine for Fsm {
    fn query(&self, _: u64, _: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
}

#[test]
fn exact_coverage_and_endpoint_validation_are_pure() {
    let layout = layout();
    let valid = transport();
    valid.validate(&layout).unwrap();
    for endpoint in [
        "http://node:80",
        "node:80/path",
        "user@node:80",
        "node :80",
        "node:0",
        "node:65536",
        "0.0.0.0:80",
        "[::]:80",
        "node",
        "node:80?x",
        "node:80#x",
        "-node:80",
        "node_:80",
        "::1:80",
        "2001:db8::1:80",
    ] {
        let mut t = valid.clone();
        t.nodes[0].raft = endpoint.into();
        assert!(t.validate(&layout).is_err(), "accepted {endpoint}");
    }
    for endpoint in [
        "localhost:80",
        "node.ns.svc.cluster.local:80",
        "[::1]:80",
        "127.0.0.1:80",
        "node.example.:80",
    ] {
        let mut t = valid.clone();
        t.nodes[0].application_rpc = endpoint.into();
        t.validate(&layout).unwrap();
    }
    for raft in [true, false] {
        let mut t = valid.clone();
        if raft {
            t.raft_bind.set_port(0);
        } else {
            t.application_rpc_bind.set_port(0);
        }
        assert!(t.validate(&layout).is_err());
    }
    let mut t = valid.clone();
    t.nodes.pop();
    assert!(t.validate(&layout).is_err());
    let mut t = valid.clone();
    t.nodes[0].node_id = 2;
    assert!(t.validate(&layout).is_err());
    let mut t = valid.clone();
    t.nodes[0].node_id = 4;
    assert!(t.validate(&layout).is_err());
    let mut t = valid;
    t.nodes[0].raft = format!("{}:80", "a".repeat(261));
    assert!(t.validate(&layout).is_err());
}
#[tokio::test]
async fn invalid_catalog_leaves_new_and_existing_data_untouched() {
    for exists in [false, true] {
        let dir = root(if exists {
            "invalid-existing"
        } else {
            "invalid-new"
        });
        let _ = std::fs::remove_dir_all(&dir);
        if exists {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("sentinel"), b"unchanged").unwrap();
        }
        let mut config = SlotNodeConfig::new(layout(), 1, &dir);
        let mut t = transport();
        t.nodes.pop();
        config.transport = Some(t);
        assert!(matches!(
            SlotNodeOwner::start(config, |_| Ok(Fsm), deadline()).await,
            Err(RuntimeError::InvalidConfig)
        ));
        if exists {
            assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
            assert_eq!(std::fs::read(dir.join("sentinel")).unwrap(), b"unchanged");
            std::fs::remove_dir_all(dir).unwrap();
        } else {
            assert!(!dir.exists());
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dns_dial_and_wildcard_bind_forward_across_three_nodes() {
    let layout = layout();
    let digest = layout.digest();
    let catalog = transport();
    let start = |id| {
        let mut config = SlotNodeConfig::new(layout.clone(), id, root(&format!("cluster-{id}")));
        let mut t = catalog.clone();
        let n = &t.nodes[(id - 1) as usize];
        t.raft_bind
            .set_port(n.raft.rsplit_once(':').unwrap().1.parse().unwrap());
        t.application_rpc_bind.set_port(
            n.application_rpc
                .rsplit_once(':')
                .unwrap()
                .1
                .parse()
                .unwrap(),
        );
        config.transport = Some(t);
        config.native.heartbeat_interval_ms = 30;
        config.native.election_timeout_min_ms = 120;
        config.native.election_timeout_max_ms = 240;
        config.startup.bootstrap_grace = Duration::from_millis(100);
        SlotNodeOwner::start(config, |_| Ok(Fsm), deadline())
    };
    let (a, b, c) = tokio::join!(start(1), start(2), start(3));
    let mut owners = vec![Some(a.unwrap()), Some(b.unwrap()), Some(c.unwrap())];
    let until = deadline();
    let leader = loop {
        if let Ok(Some(leader)) = owners[0]
            .as_ref()
            .unwrap()
            .handle()
            .leader_hint(SlotId(0), until)
            .await
        {
            break leader;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    for (index, owner) in owners.iter().enumerate() {
        let owner = owner.as_ref().unwrap();
        assert_eq!(owner.handle().layout().digest(), digest);
        if index as u64 + 1 != leader {
            let applied = owner
                .handle()
                .propose(b"key", b"forwarded".to_vec(), deadline())
                .await
                .unwrap();
            assert_eq!(applied.effects, b"forwarded");
        }
    }
    // Reopen one follower on the same bind ports and durable membership, then
    // remove the other follower: this write needs the surviving leader to
    // reconnect its Raft channel to the restarted DNS endpoint.
    let followers: Vec<_> = (0..3).filter(|i| *i as u64 + 1 != leader).collect();
    let restored = followers[0];
    assert!(
        owners[restored]
            .take()
            .unwrap()
            .shutdown(deadline())
            .await
            .complete
    );
    owners[restored] = Some(start(restored as u64 + 1).await.unwrap());
    let until = deadline();
    loop {
        if owners[restored]
            .as_ref()
            .unwrap()
            .handle()
            .leader_hint(SlotId(0), until)
            .await
            .unwrap()
            == Some(leader)
        {
            break;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        owners[followers[1]]
            .take()
            .unwrap()
            .shutdown(deadline())
            .await
            .complete
    );
    let applied = owners[restored]
        .as_ref()
        .unwrap()
        .handle()
        .propose(b"key", b"after-reconnect".to_vec(), deadline())
        .await
        .unwrap();
    assert_eq!(applied.effects, b"after-reconnect");
    for owner in owners.into_iter().flatten() {
        assert!(owner.shutdown(deadline()).await.complete);
    }
    for id in 1..=3 {
        std::fs::remove_dir_all(root(&format!("cluster-{id}"))).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_dns_catalog_does_not_block_router_startup() {
    let original = layout();
    let mut input = original.config().clone();
    input.slots[0].voters = vec![1];
    let layout = ClusterLayout::new(input).unwrap();
    let dir = root("unresolved");
    let mut config = SlotNodeConfig::new(layout, 3, &dir);
    let mut t = transport();
    for node in &mut t.nodes {
        node.raft = "does-not-exist.invalid:12345".into();
        node.application_rpc = "does-not-exist.invalid:12346".into();
    }
    config.transport = Some(t);
    let owner = SlotNodeOwner::start(config, |_| Ok(Fsm), deadline())
        .await
        .unwrap();
    assert!(owner.shutdown(deadline()).await.complete);
    std::fs::remove_dir_all(dir).unwrap();
}
