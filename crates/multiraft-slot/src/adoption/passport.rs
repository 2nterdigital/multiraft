//! Checked OLD local passport facts, avoiding application-side duplicate legacy decoding.
use super::{invalid, ClusterLayout};
use crate::{ManifestDigest, RouteDigest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io;

/// Original format-5 local passport values checked against the actual Slot manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyPassportIdentity {
    format_version: u32,
    data_space_id: u64,
    cluster_id: String,
    old_layout_digest: ManifestDigest,
    route_version: u32,
    bucket_count: u32,
    route_digest: RouteDigest,
    slot_count: u32,
    group_id_base: u64,
    max_skew_ms: u64,
    created_at_ms: u64,
    raw_sha256: [u8; 32],
}
impl LegacyPassportIdentity {
    pub fn format_version(&self) -> u32 {
        self.format_version
    }
    pub fn data_space_id(&self) -> u64 {
        self.data_space_id
    }
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    pub fn old_layout_digest(&self) -> ManifestDigest {
        self.old_layout_digest
    }
    pub fn route_version(&self) -> u32 {
        self.route_version
    }
    pub fn bucket_count(&self) -> u32 {
        self.bucket_count
    }
    pub fn route_digest(&self) -> RouteDigest {
        self.route_digest
    }
    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }
    pub fn group_id_base(&self) -> u64 {
        self.group_id_base
    }
    pub fn max_skew_ms(&self) -> u64 {
        self.max_skew_ms
    }
    pub fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }
    pub fn raw_sha256(&self) -> [u8; 32] {
        self.raw_sha256
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Passport {
    format_version: u32,
    space_id: u64,
    cluster_id: String,
    layout_digest: String,
    route_version: u32,
    bucket_count: u32,
    route_digest: String,
    slot_count: u32,
    group_id_base: u64,
    max_skew_ms: u64,
    created_at_ms: u64,
}
pub(super) fn inspect(bytes: &[u8], layout: &ClusterLayout) -> io::Result<LegacyPassportIdentity> {
    let passport: Passport = serde_json::from_slice(bytes).map_err(io::Error::other)?;
    let config = layout.config();
    if passport.format_version != 5
        || passport.space_id == 0
        || passport.cluster_id != config.cluster_id
        || passport.layout_digest != layout.digest().to_hex()
        || passport.route_digest != layout.route_digest().to_hex()
        || passport.route_version != config.route_version.wire_id()
        || passport.bucket_count != config.bucket_count
        || passport.slot_count != config.slot_count
        || passport.group_id_base != config.group_id_base
        || passport.max_skew_ms == 0
        || passport.created_at_ms == 0
    {
        return Err(invalid(
            "legacy passport does not match actual Slot manifest",
        ));
    }
    Ok(LegacyPassportIdentity {
        format_version: passport.format_version,
        data_space_id: passport.space_id,
        cluster_id: passport.cluster_id,
        old_layout_digest: layout.digest(),
        route_version: passport.route_version,
        bucket_count: passport.bucket_count,
        route_digest: layout.route_digest(),
        slot_count: passport.slot_count,
        group_id_base: passport.group_id_base,
        max_skew_ms: passport.max_skew_ms,
        created_at_ms: passport.created_at_ms,
        raw_sha256: Sha256::digest(bytes).into(),
    })
}
