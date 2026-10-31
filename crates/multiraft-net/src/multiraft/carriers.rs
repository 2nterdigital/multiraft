//! Native transport catalog access; frozen Genesis membership metadata remains in ClusterConfig.
use super::*;
use crate::{CarrierCatalog, CarrierError, CarrierInstall};
use std::net::SocketAddr;
impl<S: StateMachine> MultiRaft<S> {
    pub(crate) fn install_carrier_catalog(
        &self,
        catalog: &CarrierCatalog,
    ) -> Result<CarrierInstall, CarrierError> {
        if !catalog
            .peers
            .iter()
            .any(|peer| peer.node_id == self.node_id)
        {
            return Err(CarrierError::InvalidCatalog("local node is missing"));
        }
        match &self.net {
            NetBackend::Grpc { router } => router.install_carrier_catalog(catalog),
            _ => Err(CarrierError::UnsupportedTransport),
        }
    }
    pub(crate) fn carrier_catalog(&self) -> Result<Option<CarrierCatalog>, CarrierError> {
        match &self.net {
            NetBackend::Grpc { router } => router.carrier_catalog(),
            _ => Err(CarrierError::UnsupportedTransport),
        }
    }
    pub(crate) fn carrier_scope(&self) -> Option<[u8; 32]> {
        self.config.raft_transport_fingerprint
    }
    pub(crate) fn fence_carrier_installs(&self) {
        if let NetBackend::Grpc { router } = &self.net {
            router.fence_carrier_installs();
        }
    }
    pub(super) fn peer_carrier_address(&self, peer: NodeId) -> Option<SocketAddr> {
        match &self.net {
            NetBackend::Grpc { router } => router.peer_carrier_address(peer),
            _ => self
                .config
                .peers
                .iter()
                .find(|(id, _)| *id == peer)
                .map(|(_, address)| *address),
        }
    }
}
