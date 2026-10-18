//! Optional runtime networking, deliberately excluded from layout identity.
use crate::{ClusterLayout, NodeId, RuntimeError};
use std::net::SocketAddr;

/// Per-node dial addresses. These must be host:port strings, never URLs.
#[derive(Clone, Debug)]
pub struct SlotTransportNode {
    pub node_id: NodeId,
    pub raft: String,
    pub application_rpc: String,
}
/// A complete dial catalog plus this process's independent listener addresses.
/// Validation performs no DNS lookup; tonic resolves names when dialing. Changing these settings
/// does not change the immutable layout, manifest or native membership.
#[derive(Clone, Debug)]
pub struct SlotTransportConfig {
    pub nodes: Vec<SlotTransportNode>,
    pub raft_bind: SocketAddr,
    pub application_rpc_bind: SocketAddr,
}
impl SlotTransportConfig {
    pub fn validate(&self, layout: &ClusterLayout) -> Result<(), RuntimeError> {
        if self.nodes.len() != layout.nodes().len()
            || self.raft_bind.port() == 0
            || self.application_rpc_bind.port() == 0
        {
            return Err(RuntimeError::InvalidConfig);
        }
        for raft in [true, false] {
            let peers = self
                .nodes
                .iter()
                .map(|n| {
                    (
                        n.node_id,
                        if raft {
                            n.raft.clone()
                        } else {
                            n.application_rpc.clone()
                        },
                    )
                })
                .collect::<Vec<_>>();
            multiraft_core::validate_dial_catalog(&peers, layout.nodes().iter().map(|n| n.node_id))
                .map_err(|_| RuntimeError::InvalidConfig)?;
        }
        Ok(())
    }
    pub(crate) fn native(&self) -> multiraft_core::RaftTransportConfig {
        multiraft_core::RaftTransportConfig {
            peers: self
                .nodes
                .iter()
                .map(|n| (n.node_id, n.raft.clone()))
                .collect(),
            listen_address: self.raft_bind,
        }
    }
}
