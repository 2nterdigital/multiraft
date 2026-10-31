//! Delayed real Raft traffic must not reach an unseeded target generation.
use super::{admitted, authority::StartupAuthority, cluster, session::Session};
use cluster::{deadline, GROUP};
use multiraft_core::typ::{Vote, VoteRequest};
use multiraft_net::GrpcRouter;
use openraft::network::RPCOption;
use openraft_multi::GroupRouter;
use std::time::Duration;

pub async fn unseeded_target_rejects_old_vote() {
    let root = cluster::scratch("unseeded-stale-vote-");
    let mut session = Session::fresh(&root).await;
    let (leader, specification, target) = session.reserve(false).await;
    let old_vote = session.nodes[leader]
        .observe_group(GROUP)
        .expect("actual old native vote observation")
        .0
        .flushed_vote;
    session
        .group(leader)
        .begin(specification.clone(), 1, &target, deadline())
        .await
        .expect("bind operation before opening new target generation");
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .expect("admit immutable new target generation"),
    );
    target
        .open_target(specification.operation, admission, deadline())
        .await
        .expect("actual currently unseeded target open");
    let configs = cluster::configurations(&root);
    let address = configs[3]
        .peers
        .iter()
        .find(|(id, _)| *id == 4)
        .expect("actual target gRPC address")
        .1;
    let router = GrpcRouter::new(vec![(4, address)], old_vote.node_id);
    let request = VoteRequest::new(
        Vote::new(old_vote.term, old_vote.node_id),
        *specification.expected_start.log_id(),
    );
    let response = router
        .vote(4, GROUP, request, RPCOption::new(Duration::from_secs(2)))
        .await;
    if let Ok(response) = &response {
        assert!(!response.vote_granted, "unseeded new generation must not grant an actual old native vote before a verified post-Begin checkpoint is installed: {response:?}");
    }
    router.close();
    router.join().await.expect("join real native RPC work");
    session.stop().await;
}
