//! Shared per-peer tonic channels with opaque connection generations.
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::conn_metrics::ConnMetrics;
use multiraft_core::NodeId;
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};

/// Failure to obtain a channel for one static peer.
#[derive(Debug)]
pub enum GrpcPeerChannelError {
    UnknownPeer {
        peer: NodeId,
    },
    Connect {
        peer: NodeId,
        source: tonic::transport::Error,
    },
    Closed {
        peer: NodeId,
    },
    GenerationExhausted {
        peer: NodeId,
    },
}
impl fmt::Display for GrpcPeerChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer { peer } => write!(f, "peer {peer} is not configured"),
            Self::Connect { peer, .. } => write!(f, "failed to connect to peer {peer}"),
            Self::Closed { peer } => write!(f, "peer {peer} channel owner is closed"),
            Self::GenerationExhausted { peer } => {
                write!(f, "peer {peer} channel generations exhausted")
            }
        }
    }
}
impl Error for GrpcPeerChannelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Generation {
    number: u64,
    channel: Channel,
}

/// One request's cached Channel generation, not each tonic-internal TCP session.
/// It is not a peer liveness observation.
/// Keep leases scoped to admitted requests; a retained lease owns its tonic channel.
#[derive(Clone, Debug)]
pub struct GrpcPeerChannelLease {
    peer: NodeId,
    generation: Arc<Generation>,
}
impl GrpcPeerChannelLease {
    pub fn peer(&self) -> NodeId {
        self.peer
    }
    /// Diagnostic sequence within this pool; not cluster identity or authority.
    pub fn generation(&self) -> u64 {
        self.generation.number
    }
    pub fn channel(&self) -> Channel {
        self.generation.channel.clone()
    }
}
struct PeerLink {
    address: SocketAddr,
    current: Mutex<Option<Arc<Generation>>>,
    connect: tokio::sync::Mutex<()>,
    next_generation: Mutex<u64>,
}

/// Generic channel owner shared across Groups or application services.
/// Different address catalogs use different pools. The pool interprets no payloads
/// and performs no RPC retries. Failed requests invalidate only their own generation.
#[derive(Clone)]
pub struct GrpcPeerChannelPool {
    peers: Arc<HashMap<NodeId, PeerLink>>,
    stopping: watch::Sender<bool>,
    metrics: ConnMetrics,
}
impl GrpcPeerChannelPool {
    pub fn new(peers: Vec<(NodeId, SocketAddr)>) -> Self {
        Self {
            peers: Arc::new(
                peers
                    .into_iter()
                    .map(|(peer, address)| {
                        (
                            peer,
                            PeerLink {
                                address,
                                current: Mutex::new(None),
                                connect: tokio::sync::Mutex::new(()),
                                next_generation: Mutex::new(1),
                            },
                        )
                    })
                    .collect(),
            ),
            stopping: watch::channel(false).0,
            metrics: ConnMetrics::new(),
        }
    }

    /// Acquire one cached generation, constructing at most one channel per peer
    /// concurrently. Cancellation releases construction admission; the next caller
    /// can connect. Closing prevents publication of a connection completing late.
    pub async fn lease(&self, peer: NodeId) -> Result<GrpcPeerChannelLease, GrpcPeerChannelError> {
        let link = self
            .peers
            .get(&peer)
            .ok_or(GrpcPeerChannelError::UnknownPeer { peer })?;
        let mut stopping = self.stopping.subscribe();
        if *stopping.borrow_and_update() {
            return Err(GrpcPeerChannelError::Closed { peer });
        }
        if let Some(generation) = link.current.lock().unwrap().clone() {
            return Ok(GrpcPeerChannelLease { peer, generation });
        }
        let connect = async {
            let _connecting = link.connect.lock().await;
            if *self.stopping.borrow() {
                return Err(GrpcPeerChannelError::Closed { peer });
            }
            if let Some(generation) = link.current.lock().unwrap().clone() {
                return Ok(GrpcPeerChannelLease { peer, generation });
            }
            let endpoint = Endpoint::from_shared(format!("http://{}", link.address))
                .map_err(|source| GrpcPeerChannelError::Connect { peer, source })?;
            let channel = endpoint
                .connect()
                .await
                .map_err(|source| GrpcPeerChannelError::Connect { peer, source })?;
            let mut current = link.current.lock().unwrap();
            if *self.stopping.borrow() {
                return Err(GrpcPeerChannelError::Closed { peer });
            }
            let mut next = link.next_generation.lock().unwrap();
            let number = *next;
            *next = next
                .checked_add(1)
                .ok_or(GrpcPeerChannelError::GenerationExhausted { peer })?;
            let generation = Arc::new(Generation { number, channel });
            *current = Some(generation.clone());
            self.metrics.record_peer(peer);
            Ok(GrpcPeerChannelLease { peer, generation })
        };
        tokio::select! {
            biased;
            _ = stopping.changed() => Err(GrpcPeerChannelError::Closed { peer }),
            result = connect => result,
        }
    }

    /// Legacy channel accessor. Request owners needing reconnect safety retain
    /// a [`GrpcPeerChannelLease`] and invalidate it only after their own failure.
    pub async fn channel(&self, peer: NodeId) -> Result<Channel, GrpcPeerChannelError> {
        self.lease(peer).await.map(|lease| lease.channel())
    }

    /// Discard a failed current generation. A stale lease or another pool's lease
    /// cannot invalidate a newer connection. This does not retry a request.
    pub fn invalidate(&self, lease: &GrpcPeerChannelLease) -> bool {
        let Some(link) = self.peers.get(&lease.peer) else {
            return false;
        };
        let mut current = link.current.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|generation| Arc::ptr_eq(generation, &lease.generation))
        {
            *current = None;
            true
        } else {
            false
        }
    }

    /// Fence new leases and cancel pending connection construction. Existing
    /// request leases remain the responsibility of their admitted request owners.
    pub fn close(&self) {
        self.stopping.send_replace(true);
        for link in self.peers.values() {
            link.current.lock().unwrap().take();
        }
    }
    pub(crate) fn stopping(&self) -> watch::Receiver<bool> {
        self.stopping.subscribe()
    }

    /// Close and drain connection constructors. This does not retain or wait for
    /// externally held channel clones; consumers must drain their requests first.
    pub async fn shutdown(&self) {
        self.close();
        for link in self.peers.values() {
            let _drained = link.connect.lock().await;
        }
    }

    /// Historical distinct peer cardinality, not current connectivity or quorum.
    pub fn unique_peer_links(&self) -> usize {
        self.metrics.unique_peer_links()
    }
}
