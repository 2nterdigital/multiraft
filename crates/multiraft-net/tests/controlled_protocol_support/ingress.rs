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
    let executor = session
        .group(leader)
        .begin(specification.clone(), 1, &target, deadline())
        .await
        .expect("bind operation before opening new target generation");
    let configs = cluster::configurations(&root);
    let address = configs[3]
        .peers
        .iter()
        .find(|(id, _)| *id == 4)
        .expect("actual target gRPC address")
        .1;
    let router = GrpcRouter::new(vec![(4, address)], old_vote.node_id);
    session.nodes[3]
        .create_group(99, &[4])
        .await
        .expect("open independent sibling to prove target RPC listener remains live");
    router
        .vote(
            4,
            99,
            VoteRequest::new(
                Vote::new(old_vote.term, old_vote.node_id),
                *specification.expected_start.log_id(),
            ),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await
        .expect(
            "real normal RPC succeeds on sibling while target generation must stay quarantined",
        );
    let mut request = VoteRequest::new(
        Vote::new(old_vote.term + 1, old_vote.node_id),
        *specification.expected_start.log_id(),
    );
    // The log floor must protect startup independently of a transient leader lease.
    request.leadership_transfer = true;
    let response = router
        .vote(
            4,
            GROUP,
            request.clone(),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await;
    assert!(
        response.is_err(),
        "unseeded target must remain absent from native RPC routing: {response:?}"
    );
    let seed = session
        .group(leader)
        .prepare_seed(&executor, deadline())
        .await
        .expect("capture actual post-Begin checkpoint");
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .expect("admit immutable new target generation"),
    );
    target
        .open_target(seed, admission, deadline())
        .await
        .expect("install verified checkpoint before native route publication");
    let response = router
        .vote(4, GROUP, request, RPCOption::new(Duration::from_secs(2)))
        .await
        .expect("published target responds using its durable native state");
    assert!(
        !response.vote_granted,
        "initialized target rejects a request behind its native log floor: {response:?}"
    );
    assert!(response.last_log_id.is_some_and(|id| id.index
        > specification
            .expected_start
            .log_id()
            .expect("native starting identity")
            .index));
    router.close();
    router.join().await.expect("join real native RPC work");
    session.stop().await;
}
