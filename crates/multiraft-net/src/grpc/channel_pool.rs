//! Shared lazy tonic channels keyed by static peer node identity.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use multiraft_core::NodeId;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::conn_metrics::ConnMetrics;

/// Failure to obtain a tonic Channel for one configured peer.
#[derive(Debug)]
pub enum GrpcPeerChannelError {
    /// The requested peer is absent from this pool's static catalog.
    UnknownPeer {
        /// Missing peer identity.
        peer: NodeId,
    },
    /// The peer is configured but tonic could not establish its Channel.
    Connect {
        /// Configured peer identity.
        peer: NodeId,
        /// Tonic endpoint or connection failure.
        source: tonic::transport::Error,
    },
}

impl fmt::Display for GrpcPeerChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer { peer } => write!(formatter, "peer {peer} is not configured"),
            Self::Connect { peer, .. } => write!(formatter, "failed to connect to peer {peer}"),
        }
    }
}

impl Error for GrpcPeerChannelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnknownPeer { .. } => None,
            Self::Connect { source, .. } => Some(source),
        }
    }
}

/// Lazy per-peer tonic Channel cache for one static address catalog.
///
/// Separate users construct separate pools when the same node identities map
/// to different service addresses. Catalog validation remains the caller's
/// responsibility.
#[derive(Clone)]
pub struct GrpcPeerChannelPool {
    peers: Arc<HashMap<NodeId, SocketAddr>>,
    channels: Arc<Mutex<HashMap<NodeId, Channel>>>,
    metrics: ConnMetrics,
}

impl GrpcPeerChannelPool {
    /// Creates an empty Channel cache over the supplied static peer catalog.
    pub fn new(peers: Vec<(NodeId, SocketAddr)>) -> Self {
        Self {
            peers: Arc::new(peers.into_iter().collect()),
            channels: Arc::new(Mutex::new(HashMap::new())),
            metrics: ConnMetrics::new(),
        }
    }

    /// Returns the cached Channel for `peer`, connecting and caching it when absent.
    ///
    /// # Errors
    ///
    /// Returns [`GrpcPeerChannelError::UnknownPeer`] when `peer` is absent from
    /// this pool's catalog, or [`GrpcPeerChannelError::Connect`] when tonic
    /// cannot construct or establish the peer Channel.
    pub async fn channel(&self, peer: NodeId) -> Result<Channel, GrpcPeerChannelError> {
        {
            let channels = self.channels.lock().unwrap();
            if let Some(channel) = channels.get(&peer) {
                return Ok(channel.clone());
            }
        }

        let address = self
            .peers
            .get(&peer)
            .copied()
            .ok_or(GrpcPeerChannelError::UnknownPeer { peer })?;
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .map_err(|source| GrpcPeerChannelError::Connect { peer, source })?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|source| GrpcPeerChannelError::Connect { peer, source })?;

        {
            let mut channels = self.channels.lock().unwrap();
            if let Some(existing) = channels.get(&peer) {
                return Ok(existing.clone());
            }
            channels.insert(peer, channel.clone());
        }
        self.metrics.record_peer(peer);
        Ok(channel)
    }

    /// Returns how many distinct peer identities have a cached Channel.
    ///
    /// This is cached/seen cardinality, not current connectivity, liveness,
    /// membership, readiness, or quorum state.
    pub fn unique_peer_links(&self) -> usize {
        self.metrics.unique_peer_links()
    }
}
