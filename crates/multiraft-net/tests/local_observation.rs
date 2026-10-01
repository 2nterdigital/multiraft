//! Public local-only FSM consumer: no raw Raft, metrics, trigger or production probes.
use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    GroupConfig, GroupServerState, NodeOwner, RuntimeConfig, RuntimeError, RuntimeHandle,
    TryReadError,
};
use std::{io, net::TcpListener, time::Duration};
use tokio::time::Instant;

struct Bytes(Vec<u8>);
impl StateMachine for Bytes {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.0 = bytes.to_vec();
        Ok(ApplyOut {
            effects: self.0.clone(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.clone())
    }
    fn restore(&mut self, _: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = bytes.to_vec();
        Ok(())
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn start(config: ClusterConfig, voters: Vec<u64>) -> NodeOwner<Bytes> {
    NodeOwner::start(
        RuntimeConfig::new(
            config,
            vec![GroupConfig {
                group_id: 7,
                voters,
            }],
        ),
        |_| Ok(Bytes(Vec::new())),
        deadline(),
    )
    .await
    .unwrap()
}
async fn leader(handles: &[RuntimeHandle<Bytes>]) -> usize {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for (i, handle) in handles.iter().enumerate() {
                if handle
                    .read_linearizable(7, deadline(), |_| ())
                    .await
                    .is_ok()
                {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_status_and_fallible_query_preserve_bytes_error_and_weak_lifetime() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.peers = vec![(1, address)];
    let owner = start(config, vec![1]).await;
    let handle = owner.handle();
    leader(std::slice::from_ref(&handle)).await;
    let receipt = handle
        .propose(7, b"local value".to_vec(), deadline())
        .await
        .unwrap();
    assert_eq!(
        handle
            .try_read_applied(7, deadline(), |fsm| Ok::<_, io::Error>(fsm.0.clone()))
            .await
            .unwrap(),
        b"local value"
    );
    // A successful apply precedes asynchronous native metrics publication. A
    // point sample may lag; wait for that observable watermark rather than
    // inventing freshness from the write receipt or the local query value.
    let status = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = handle.local_group_status(7, deadline()).await.unwrap();
            if status
                .last_applied
                .is_some_and(|id| id.index >= receipt.index)
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!((status.group_id, status.local_node_id), (7, 1));
    assert_eq!(status.server_state, GroupServerState::Leader);
    assert!(status.running);
    assert!(status.current_term >= receipt.term);
    assert!(status.last_log_index.unwrap() >= receipt.index);
    assert!(status.last_applied.unwrap().index >= receipt.index);
    let rejected = handle
        .try_read_applied(7, deadline(), |_| {
            Err::<(), _>(io::Error::other("application query refused"))
        })
        .await;
    assert!(
        matches!(rejected, Err(TryReadError::Application(error)) if error.to_string() == "application query refused")
    );
    assert!(matches!(
        handle.local_group_status(7, Instant::now()).await,
        Err(RuntimeError::Deadline {
            outcome_unknown: false,
            ..
        })
    ));
    owner.shutdown(deadline()).await.unwrap();
    assert!(matches!(
        handle.local_group_status(7, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    assert!(matches!(
        handle
            .try_read_applied(7, deadline(), |_| Ok::<(), io::Error>(()))
            .await,
        Err(TryReadError::Runtime(RuntimeError::Closed))
    ));
    // Retained point data carries no native owner and remains only an old sample.
    assert_eq!(status.local_node_id, 1);
    TcpListener::bind(address).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn applied_query_and_status_need_no_quorum_and_cannot_replace_authority() {
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: Vec<_> = listeners
        .iter()
        .enumerate()
        .map(|(i, listener)| ((i + 1) as u64, listener.local_addr().unwrap()))
        .collect();
    drop(listeners);
    let mut configs: Vec<_> = (1..=3)
        .map(|node| {
            let mut config = ClusterConfig::for_test(node, &[1, 2, 3]);
            config.peers = peers.clone();
            config
        })
        .collect();
    let (one, two, three) = tokio::join!(
        start(configs.remove(0), vec![1, 2, 3]),
        start(configs.remove(0), vec![1, 2, 3]),
        start(configs.remove(0), vec![1, 2, 3])
    );
    let handles = [one.handle(), two.handle(), three.handle()];
    let index = leader(&handles).await;
    let mut owners = [Some(one), Some(two), Some(three)];
    let receipt = handles[index]
        .propose(7, b"confirmed before quorum loss".to_vec(), deadline())
        .await
        .unwrap();
    for (i, owner) in owners.iter_mut().enumerate() {
        if i != index {
            owner.take().unwrap().shutdown(deadline()).await.unwrap();
        }
    }
    let local = &handles[index];
    assert!(local
        .read_linearizable(7, Instant::now() + Duration::from_millis(100), |_| ())
        .await
        .is_err());
    assert_eq!(
        local
            .try_read_applied(7, deadline(), |fsm| Ok::<_, io::Error>(fsm.0.clone()))
            .await
            .unwrap(),
        b"confirmed before quorum loss"
    );
    let status = local.local_group_status(7, deadline()).await.unwrap();
    assert_eq!(status.local_node_id, (index + 1) as u64);
    assert!(status.last_applied.unwrap().index >= receipt.index);
    owners[index]
        .take()
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
}
