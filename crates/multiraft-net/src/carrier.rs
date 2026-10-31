//! Versioned transport addresses are configuration facts, never replica or consensus authority.
use multiraft_core::NodeId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, net::SocketAddr};

/// Stable authenticated passport binding and transport addresses for one node.
/// `identity` must bind persistent node identity, not an ephemeral boot or lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CarrierPeer {
    pub node_id: NodeId,
    pub identity: Vec<u8>,
    pub raft: SocketAddr,
    pub application_rpc: SocketAddr,
}
/// Full trusted address catalog. Hosts authenticate passports, domain and profile before installation.
/// Installing this DTO grants no Group admission, membership, election or business authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CarrierCatalog {
    pub version: u64,
    pub scope: Option<[u8; 32]>,
    pub peers: Vec<CarrierPeer>,
}
/// Acknowledgement for exactly one installed transport component, not a multi-component transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarrierInstall {
    pub version: u64,
    pub digest: [u8; 32],
}
/// Transport plane selected by a shared channel pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarrierService {
    Raft,
    ApplicationRpc,
}
impl CarrierService {
    pub(crate) fn address(self, peer: &CarrierPeer) -> SocketAddr {
        match self {
            Self::Raft => peer.raft,
            Self::ApplicationRpc => peer.application_rpc,
        }
    }
}
/// Rejected catalog changes preserve the last installed version and all identity pins.
#[derive(Debug, thiserror::Error)]
pub enum CarrierError {
    #[error("carrier owner is closed")]
    Closed,
    #[error("carrier scope differs from the configured transport domain")]
    ScopeMismatch,
    #[error("invalid carrier catalog: {0}")]
    InvalidCatalog(&'static str),
    #[error("stale carrier version; current version is {current}")]
    Stale { current: u64 },
    #[error("conflicting carrier content at version {current}")]
    Conflict { current: u64 },
    #[error("carrier identity changed for node {node}")]
    IdentityMismatch { node: NodeId },
    #[error("carrier catalogs require the grpc transport")]
    UnsupportedTransport,
    #[error(transparent)]
    Runtime(#[from] crate::RuntimeError),
    #[error(transparent)]
    Rpc(#[from] crate::application_rpc::RpcError),
}
impl CarrierCatalog {
    /// Validate and hash canonical full content without installing or granting any authority.
    pub fn validated_install(&self) -> Result<CarrierInstall, CarrierError> {
        self.canonical().map(|(_, receipt)| receipt)
    }

    pub(crate) fn canonical(&self) -> Result<(Self, CarrierInstall), CarrierError> {
        if self.version == 0 || self.peers.is_empty() || self.peers.len() > 4096 {
            return Err(CarrierError::InvalidCatalog(
                "version and bounded peer set must be nonempty",
            ));
        }
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for peer in &self.peers {
            if peer.node_id == 0
                || peer.identity.is_empty()
                || peer.identity.len() > 4096
                || !ids.insert(peer.node_id)
            {
                return Err(CarrierError::InvalidCatalog(
                    "invalid or duplicate node identity",
                ));
            }
            for address in [peer.raft, peer.application_rpc] {
                if address.ip().is_unspecified()
                    || address.port() == 0
                    || !addresses.insert(address)
                {
                    return Err(CarrierError::InvalidCatalog(
                        "invalid or duplicate transport address",
                    ));
                }
            }
        }
        let mut catalog = self.clone();
        catalog.peers.sort_by_key(|peer| peer.node_id);
        let bytes = serde_json::to_vec(&catalog)
            .map_err(|_| CarrierError::InvalidCatalog("catalog encoding failed"))?;
        let digest = Sha256::digest(bytes).into();
        Ok((
            catalog,
            CarrierInstall {
                version: self.version,
                digest,
            },
        ))
    }
}
