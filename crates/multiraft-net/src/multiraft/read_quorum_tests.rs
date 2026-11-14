//! ReadIndex quorum retry: late follower acks are retried within the caller's
//! budget, while real leadership loss still fails the read promptly.
use super::*;
use crate::router::{NodeMessage, RaftCall};
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};
use openraft::vote::RaftLeaderId;
use tokio::time::Instant;

/// Fault applied to AppendEntries reaching an interposed node.
#[derive(Default)]
struct Fault {
    /// Replies to appends are held back by `delay` until this instant.
    delay_until: Option<Instant>,
    delay: Duration,
    /// Appends sent by this leader are dropped (reply channel closed).
    drop_from: Option<NodeId>,
}

type SharedFault = Arc<Mutex<Fault>>;

/// Replace `node`'s inbound channel on the shared router with a proxy that
/// forwards in order and applies `fault` to AppendEntries.
fn interpose(router: &Router, node: NodeId, fault: SharedFault) {
    let mut real = router
        .nodes
        .lock()
        .unwrap()
        .get(&node)
        .cloned()
        .expect("registered node");
    let (tx, mut rx) = mpsc::channel::<NodeMessage>(1024);
    router.nodes.lock().unwrap().insert(node, tx);
    tokio::spawn(async move {
        while let Some(message) = rx.next().await {
            let NodeMessage {
                group_id,
                call,
                response_tx,
            } = message;
            let (delay, dropped) = match &call {
                RaftCall::Append(rpc) => {
                    let fault = fault.lock().unwrap();
                    let sender = *rpc.vote.leader_id().node_id();
                    let delay = match fault.delay_until {
                        Some(until) if Instant::now() < until => fault.delay,
                        _ => Duration::ZERO,
                    };
                    (delay, fault.drop_from == Some(sender))
                }
                _ => (Duration::ZERO, false),
            };
            if dropped {
                continue;
            }
            let (inner_tx, inner_rx) = oneshot::channel();
            let forwarded = NodeMessage {
                group_id,
                call,
                response_tx: inner_tx,
            };
            if real.send(forwarded).await.is_err() {
                return;
            }
            tokio::spawn(async move {
                if let Ok(reply) = inner_rx.await {
                    tokio::time::sleep(delay).await;
                    let _ = response_tx.send(reply);
                }
            });
        }
    });
}

async fn three_nodes(group: GroupId) -> (Vec<MultiRaft>, NodeId, SharedFault) {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs).await.expect("cluster");
    for node in &nodes {
        node.create_group(group, &peer_ids).await.expect("group");
    }
    let leader = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let fault = SharedFault::default();
    for node in nodes.iter().filter(|n| n.node_id() != leader) {
        interpose(nodes[0].router(), node.node_id(), fault.clone());
    }
    (nodes, leader, fault)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_follower_acks_are_retried_within_the_read_budget() {
    let group = 7;
    let (nodes, leader_id, fault) = three_nodes(group).await;
    let leader = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();
    leader
        .propose(group, CounterFsm::encode_add(5, 1))
        .await
        .expect("propose");

    // Acks arrive 150 ms late (over the 100 ms ReadIndex wait, under the
    // 300 ms election floor) for a short window; leadership is unchanged.
    {
        let mut fault = fault.lock().unwrap();
        fault.delay = Duration::from_millis(150);
        fault.delay_until = Some(Instant::now() + Duration::from_millis(300));
    }
    let started = Instant::now();
    let value = leader
        .read_linearizable_at(group, started + Duration::from_secs(5), |fsm| {
            fsm.value(group)
        })
        .await
        .expect("read survives late acks");
    let elapsed = started.elapsed();
    assert_eq!(value, 5);
    assert!(
        elapsed >= Duration::from_millis(100),
        "the first ReadIndex round must have missed its quorum; elapsed {elapsed:?}"
    );
    assert!(leader.is_leader(group), "leadership must be unchanged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_leadership_fails_the_read_promptly() {
    let group = 8;
    let (nodes, leader_id, fault) = three_nodes(group).await;
    let leader = nodes.iter().find(|n| n.node_id() == leader_id).unwrap();

    // Followers stop hearing from the leader and elect another one; the old
    // leader learns of it from the new leader's appends.
    fault.lock().unwrap().drop_from = Some(leader_id);
    let started = Instant::now();
    let error = leader
        .read_linearizable_at(group, started + Duration::from_secs(10), |fsm| {
            fsm.value(group)
        })
        .await
        .expect_err("a deposed leader must not serve the read");
    let elapsed = started.elapsed();
    assert!(
        matches!(error, MultiRaftError::NotLeader { .. }),
        "expected NotLeader, got {error:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "leadership loss must end the retry well before the deadline; elapsed {elapsed:?}"
    );
}
