//! Shared Multi-Raft networking: O(nodes) peer channels, not O(groups).
//!
//! In-process [`Router`] adapted from openraft `examples/multi-raft-kv` at tag
//! `v0.10.0-alpha.30`, implementing [`openraft_multi::GroupRouter`].
//!
//! Cross-process: [`GrpcRouter`] + tonic [`GrpcServer`] (bincode payloads).
//!
//! Public orchestration facade: [`MultiRaft`] (`use multiraft_net::MultiRaft`).

mod api;
mod conn_metrics;
mod fsm_factory;
mod group_observation;
mod grpc;
mod multiraft;
mod network;
mod node;
mod router;
mod snapshot_fetch;
mod standby_throttle;

pub use conn_metrics::ConnMetrics;
pub use fsm_factory::FsmFactoryContext;
pub use fsm_factory::StateMachineFactory;
pub use group_observation::GroupObservation;
pub use group_observation::GroupObservationReceiver;
pub use group_observation::GroupServerState;
pub use group_observation::LocalMembershipRole;
pub use group_observation::MembershipObservation;
pub use group_observation::ObservedLogId;
pub use group_observation::VoteObservation;
pub use grpc::node_rpc;
pub use grpc::GrpcRouter;
pub use grpc::GrpcServer;
pub use multiraft::wait_for_leader;
pub use multiraft::MultiRaft;
pub use multiraft::SharedFabric;
pub use multiraft_core::ObservationClosed;
pub use network::GrpcNetworkFactory;
pub use network::NetworkFactory;
pub use node::create_node;
pub use node::GroupApp;
pub use node::Node;
pub use router::NodeMessage;
pub use router::NodeRx;
pub use router::NodeTx;
pub use router::Router;
pub use router::RouterError;
pub use snapshot_fetch::pull_snapshot_chunked;
pub use snapshot_fetch::FetchedSnapshot;
pub use standby_throttle::StandbyThrottle;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Compact binary Raft RPC encoding (`bincode`).
pub fn encode<T: Serialize>(t: T) -> Vec<u8> {
    bincode::serialize(&t).expect("raft encode")
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> T {
    bincode::deserialize(bytes).expect("raft decode")
}
