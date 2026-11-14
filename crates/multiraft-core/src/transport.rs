//! Dial catalogs are transport settings, never native membership identity.
use crate::NodeId;
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
};

/// Optional complete Raft dial catalog and independent local listener address.
#[derive(Clone, Debug)]
pub struct RaftTransportConfig {
    pub peers: Vec<(NodeId, String)>,
    pub listen_address: SocketAddr,
}
impl RaftTransportConfig {
    pub fn validate(&self, peers: &[(NodeId, SocketAddr)]) -> Result<(), &'static str> {
        if self.listen_address.port() == 0 {
            return Err("transport bind port must be nonzero");
        }
        validate_dial_catalog(&self.peers, peers.iter().map(|(id, _)| *id))
    }
}

/// Validate a complete catalog without DNS resolution or partial fallback.
pub fn validate_dial_catalog(
    peers: &[(NodeId, String)],
    expected: impl IntoIterator<Item = NodeId>,
) -> Result<(), &'static str> {
    let expected: BTreeSet<_> = expected.into_iter().collect();
    if peers.len() != expected.len() {
        return Err("transport peer coverage mismatch");
    }
    let mut actual = BTreeSet::new();
    for (id, address) in peers {
        if !actual.insert(*id) || !valid_dial_address(address) {
            return Err("invalid or duplicate transport peer");
        }
    }
    if actual != expected {
        return Err("transport peer coverage mismatch");
    }
    Ok(())
}

/// Host:port only: DNS names, IPv4, or bracketed IPv6; no URI components.
/// Keep the hostname intact so tonic resolves it on connection and reconnect.
pub fn valid_dial_address(address: &str) -> bool {
    if address.is_empty()
        || address.len() > 260
        || !address.is_ascii()
        || address.bytes().any(|c| c.is_ascii_whitespace())
    {
        return false;
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        return false;
    };
    if port.is_empty()
        || !port.bytes().all(|b| b.is_ascii_digit())
        || !port.parse::<u16>().is_ok_and(|p| p != 0)
    {
        return false;
    }
    if host.starts_with('[') && host.ends_with(']') {
        return host[1..host.len() - 1]
            .parse::<std::net::Ipv6Addr>()
            .is_ok_and(|ip| !ip.is_unspecified());
    }
    if let Ok(ip) = host.parse::<Ipv4Addr>() {
        return !ip.is_unspecified();
    }
    let host = host.strip_suffix('.').unwrap_or(host);
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}
