//! Shared per-peer tonic channels with opaque connection generations.
use crate::{CarrierCatalog, CarrierError, CarrierInstall, CarrierService};
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
    CatalogChanged {
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
            Self::CatalogChanged { peer } => {
                write!(f, "peer {peer} carrier changed during connection")
            }
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
    catalog_version: u64,
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
    /// Catalog version captured by this connection, zero for configured legacy addresses.
    pub fn catalog_version(&self) -> u64 {
        self.generation.catalog_version
    }
    pub fn channel(&self) -> Channel {
        self.generation.channel.clone()
    }
}
struct PeerLink {
    address: String,
    canonical: Option<SocketAddr>,
    current: Mutex<Option<Arc<Generation>>>,
    connect: tokio::sync::Mutex<()>,
    changed: watch::Sender<bool>,
}
struct PoolState {
    peers: HashMap<NodeId, Arc<PeerLink>>,
    retired: Vec<Arc<PeerLink>>,
    catalog: Option<CarrierCatalog>,
    pins: HashMap<NodeId, Vec<u8>>,
    next_generation: u64,
    closed: bool,
    installs_fenced: bool,
}
/// Shared current carrier catalog and generation-aware channels; it interprets no payload or authority.
#[derive(Clone)]
pub struct GrpcPeerChannelPool {
    state: Arc<Mutex<PoolState>>,
    scope: Option<[u8; 32]>,
    service: CarrierService,
    stopping: watch::Sender<bool>,
    metrics: ConnMetrics,
    #[cfg(test)]
    connect_gate: Option<Arc<carrier_tests::ConnectGate>>,
}
impl PeerLink {
    fn new(address: String, canonical: Option<SocketAddr>) -> Self {
        Self {
            address,
            canonical,
            current: Mutex::new(None),
            connect: tokio::sync::Mutex::new(()),
            changed: watch::channel(false).0,
        }
    }
    fn retire(&self) {
        self.changed.send_replace(true);
        self.current
            .lock()
            .expect("peer channel lock poisoned")
            .take();
    }
}
impl GrpcPeerChannelPool {
    pub fn new(peers: Vec<(NodeId, SocketAddr)>) -> Self {
        Self::with_carrier_config(peers, None, None, CarrierService::Raft)
    }
    /// Legacy DNS pool without canonical advertised address facts.
    pub fn with_dial_addresses(peers: Vec<(NodeId, String)>) -> Self {
        let canonical = peers
            .iter()
            .filter_map(|(id, address)| address.parse().ok().map(|address| (*id, address)))
            .collect();
        Self::with_carrier_config(canonical, Some(peers), None, CarrierService::Raft)
    }
    /// Retain configured DNS overrides only while verified advertised addresses remain unchanged.
    /// The caller validates initial configuration before publishing this pool.
    pub fn with_carrier_config(
        peers: Vec<(NodeId, SocketAddr)>,
        dial: Option<Vec<(NodeId, String)>>,
        scope: Option<[u8; 32]>,
        service: CarrierService,
    ) -> Self {
        let canonical: HashMap<_, _> = peers.iter().copied().collect();
        let dial = dial.unwrap_or_else(|| {
            peers
                .into_iter()
                .map(|(id, addr)| (id, addr.to_string()))
                .collect()
        });
        let peers = dial
            .into_iter()
            .map(|(id, address)| {
                (
                    id,
                    Arc::new(PeerLink::new(address, canonical.get(&id).copied())),
                )
            })
            .collect();
        Self {
            state: Arc::new(Mutex::new(PoolState {
                peers,
                retired: vec![],
                catalog: None,
                pins: HashMap::new(),
                next_generation: 1,
                closed: false,
                installs_fenced: false,
            })),
            scope,
            service,
            stopping: watch::channel(false).0,
            metrics: ConnMetrics::new(),
            #[cfg(test)]
            connect_gate: None,
        }
    }
    /// Configured actual transport scope, independent from membership and peer assertions.
    pub fn carrier_scope(&self) -> Option<[u8; 32]> {
        self.scope
    }
    /// Observe only this component's verified catalog; `None` means configured version zero.
    pub fn carrier_catalog(&self) -> Result<Option<CarrierCatalog>, CarrierError> {
        let state = self.state.lock().expect("carrier state lock poisoned");
        if state.closed || state.installs_fenced {
            return Err(CarrierError::Closed);
        }
        Ok(state.catalog.clone())
    }
    /// Atomically install authenticated carrier facts, without creating Groups or changing membership.
    pub fn install_carrier_catalog(
        &self,
        input: &CarrierCatalog,
    ) -> Result<CarrierInstall, CarrierError> {
        let (catalog, receipt) = input.canonical()?;
        let mut state = self.state.lock().expect("carrier state lock poisoned");
        if state.closed || state.installs_fenced {
            return Err(CarrierError::Closed);
        }
        if catalog.scope != self.scope {
            return Err(CarrierError::ScopeMismatch);
        }
        if let Some(current) = &state.catalog {
            if catalog.version < current.version {
                return Err(CarrierError::Stale {
                    current: current.version,
                });
            }
            if catalog.version == current.version {
                return if current == &catalog {
                    Ok(receipt)
                } else {
                    Err(CarrierError::Conflict {
                        current: current.version,
                    })
                };
            }
        }
        for peer in &catalog.peers {
            if state
                .pins
                .get(&peer.node_id)
                .is_some_and(|identity| identity != &peer.identity)
            {
                return Err(CarrierError::IdentityMismatch { node: peer.node_id });
            }
        }
        let peers = catalog
            .peers
            .iter()
            .map(|peer| {
                let address = self.service.address(peer);
                let dial = state
                    .peers
                    .get(&peer.node_id)
                    .filter(|old| old.canonical == Some(address))
                    .map_or_else(|| address.to_string(), |old| old.address.clone());
                (peer.node_id, Arc::new(PeerLink::new(dial, Some(address))))
            })
            .collect();
        let old = std::mem::replace(&mut state.peers, peers);
        for (_, link) in old {
            link.retire();
            state.retired.push(link);
        }
        // Completed old constructors need no retained drain witness. A held mutex
        // is retained until shutdown observes its actual release.
        state
            .retired
            .retain(|link| link.connect.try_lock().is_err());
        for peer in &catalog.peers {
            state.pins.insert(peer.node_id, peer.identity.clone());
        }
        state.catalog = Some(catalog);
        Ok(receipt)
    }
    pub(crate) fn peer_address(&self, peer: NodeId) -> Option<SocketAddr> {
        self.state
            .lock()
            .expect("carrier state lock poisoned")
            .peers
            .get(&peer)
            .and_then(|link| link.canonical)
    }
    pub(crate) fn fence_carrier_installs(&self) {
        self.state
            .lock()
            .expect("carrier state lock poisoned")
            .installs_fenced = true;
    }
    /// Acquire against the current catalog. Late old connects cannot publish after replacement.
    pub async fn lease(&self, peer: NodeId) -> Result<GrpcPeerChannelLease, GrpcPeerChannelError> {
        let (link, version) = {
            let state = self.state.lock().expect("carrier state lock poisoned");
            if state.closed {
                return Err(GrpcPeerChannelError::Closed { peer });
            }
            let link = state
                .peers
                .get(&peer)
                .ok_or(GrpcPeerChannelError::UnknownPeer { peer })?
                .clone();
            if let Some(generation) = link
                .current
                .lock()
                .expect("peer channel lock poisoned")
                .clone()
            {
                return Ok(GrpcPeerChannelLease { peer, generation });
            }
            (
                link,
                state.catalog.as_ref().map_or(0, |catalog| catalog.version),
            )
        };
        let mut stopping = self.stopping.subscribe();
        let mut changed = link.changed.subscribe();
        let connect = async {
            let _connecting = link.connect.lock().await;
            if *link.changed.borrow() {
                return Err(GrpcPeerChannelError::CatalogChanged { peer });
            }
            if let Some(generation) = link
                .current
                .lock()
                .expect("peer channel lock poisoned")
                .clone()
            {
                return Ok(GrpcPeerChannelLease { peer, generation });
            }
            let endpoint = Endpoint::from_shared(format!("http://{}", link.address))
                .map_err(|source| GrpcPeerChannelError::Connect { peer, source })?;
            let channel = endpoint
                .connect()
                .await
                .map_err(|source| GrpcPeerChannelError::Connect { peer, source })?;
            #[cfg(test)]
            if let Some(gate) = self
                .connect_gate
                .as_ref()
                .filter(|gate| gate.version == version)
            {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            let mut state = self.state.lock().expect("carrier state lock poisoned");
            if state.closed {
                return Err(GrpcPeerChannelError::Closed { peer });
            }
            if state
                .peers
                .get(&peer)
                .is_none_or(|current| !Arc::ptr_eq(current, &link))
            {
                return Err(GrpcPeerChannelError::CatalogChanged { peer });
            }
            let number = state.next_generation;
            state.next_generation = number
                .checked_add(1)
                .ok_or(GrpcPeerChannelError::GenerationExhausted { peer })?;
            let generation = Arc::new(Generation {
                number,
                channel,
                catalog_version: version,
            });
            *link.current.lock().expect("peer channel lock poisoned") = Some(generation.clone());
            self.metrics.record_peer(peer);
            Ok(GrpcPeerChannelLease { peer, generation })
        };
        if *stopping.borrow() {
            return Err(GrpcPeerChannelError::Closed { peer });
        }
        if *changed.borrow() {
            return Err(GrpcPeerChannelError::CatalogChanged { peer });
        }
        tokio::select! {biased;_=stopping.changed()=>Err(GrpcPeerChannelError::Closed {peer}),_=changed.changed()=>Err(GrpcPeerChannelError::CatalogChanged {peer}),result=connect=>result}
    }
    pub async fn channel(&self, peer: NodeId) -> Result<Channel, GrpcPeerChannelError> {
        self.lease(peer).await.map(|lease| lease.channel())
    }
    /// A stale lease, including a previous catalog's lease, cannot evict the current channel.
    pub fn invalidate(&self, lease: &GrpcPeerChannelLease) -> bool {
        let state = self.state.lock().expect("carrier state lock poisoned");
        let Some(link) = state.peers.get(&lease.peer) else {
            return false;
        };
        let mut current = link.current.lock().expect("peer channel lock poisoned");
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
    pub fn close(&self) {
        let mut state = self.state.lock().expect("carrier state lock poisoned");
        state.closed = true;
        state.installs_fenced = true;
        self.stopping.send_replace(true);
        for link in state.peers.values().chain(state.retired.iter()) {
            link.retire();
        }
    }
    pub(crate) fn stopping(&self) -> watch::Receiver<bool> {
        self.stopping.subscribe()
    }
    /// Close and actually drain all current or retired connection constructors.
    pub async fn shutdown(&self) {
        self.close();
        let links = {
            let state = self.state.lock().expect("carrier state lock poisoned");
            state
                .peers
                .values()
                .chain(state.retired.iter())
                .cloned()
                .collect::<Vec<_>>()
        };
        for link in links {
            let _drained = link.connect.lock().await;
        }
    }
    pub fn unique_peer_links(&self) -> usize {
        self.metrics.unique_peer_links()
    }
}

#[cfg(test)]
mod carrier_tests;
