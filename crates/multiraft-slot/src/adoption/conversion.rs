//! One maintenance authorization owns every crash-resumable conversion write.
use super::*;
use crate::{
    SlotLocalIdentity, SlotStartupDisposition, StrictSlotStartup, CONTROLLED_SLOT_PROTOCOL_VERSION,
};
use multiraft_core::TypeConfig;
use multiraft_store::{
    adoption::{adoption_directory, AdoptionAnchor, AdoptionComplete},
    RocksLogDb,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io::Write};

/// Trusted maintenance coordinator authorization, bound to exact Native-produced evidence.
/// Native validates these bindings; authentication belongs to the maintenance coordinator.
/// Physical domain and the new UUID are authorized inputs, absent from OLD Native metadata.
#[derive(Debug, Clone)]
pub struct UpgradeAuthorization {
    pub session_id: [u8; 16],
    pub authorization_id: [u8; 16],
    pub cold_manifest_sha256: [u8; 32],
    pub physical_domain: [u8; 32],
    pub root: PathBuf,
    /// Local node id must come from the OLD Slot manifest, never the leader id.
    pub node_id: u64,
    pub data_space_id: u64,
    pub groups: BTreeSet<u64>,
    pub old_manifest_digest: [u8; 32],
    pub inventory_digest: [u8; 32],
    pub new_identity_digest: [u8; 32],
    pub startup: StrictSlotStartup,
}
/// Durable Native conversion result; this grants no shared Store or ingress permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionReceipt {
    pub authorization_digest: [u8; 32],
    pub node_id: u64,
    pub generation: u64,
    pub identities: BTreeMap<u64, Vec<u8>>,
    pub fingerprint: [u8; 32],
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u32,
    authorization: serde_json::Value,
    receipt: AdoptionReceipt,
    root: PathBuf,
    layout: LayoutConfig,
    node: u64,
    space: u64,
    files: BTreeMap<PathBuf, [u8; 32]>,
    replicas: Vec<LegacyReplica>,
    manifest: Vec<u8>,
    passport: Vec<u8>,
    new_manifest: Vec<u8>,
}
impl AdoptionEvidence {
    /// The first controlled replica generation is independent of old FSM namespace generations.
    pub fn controlled_generation(&self) -> u64 {
        1
    }
    /// The target identity digest binds UUID, protocol, logical space and compatibility tuple.
    pub fn new_identity_digest(&self, identity: &SlotLocalIdentity) -> io::Result<[u8; 32]> {
        Ok(Sha256::digest(serde_json::to_vec(identity).map_err(io::Error::other)?).into())
    }
    /// Derive the exact per-group identity from original evidence and the authorized target identity.
    pub fn replica_identity(
        &self,
        group: u64,
        identity: &SlotLocalIdentity,
    ) -> io::Result<Vec<u8>> {
        if !self.replicas.iter().any(|r| r.group == group) {
            return Err(invalid("group absent from original evidence"));
        }
        let mut hash = Sha256::new();
        hash.update(b"multiraft-legacy-adoption-generation-v1\0");
        hash.update(self.inventory);
        hash.update(self.node.to_be_bytes());
        hash.update(group.to_be_bytes());
        hash.update(self.new_identity_digest(identity)?);
        Ok(hash.finalize().to_vec())
    }
    /// The shared controlled transport fingerprint excludes this node's local UUID.
    pub fn transport_fingerprint(&self, startup: &StrictSlotStartup) -> io::Result<[u8; 32]> {
        startup
            .validate_profile(&self.layout, self.node, CONTROLLED_SLOT_PROTOCOL_VERSION)
            .map_err(io::Error::other)?;
        Ok(startup.transport_digest(self.layout.digest()).0)
    }
}
fn authorization_value(auth: &UpgradeAuthorization) -> serde_json::Value {
    serde_json::json!({"session":auth.session_id,"authorization":auth.authorization_id,
        "cold_manifest_sha256":auth.cold_manifest_sha256,"physical_domain":auth.physical_domain,
        "root":auth.root,"node":auth.node_id,"space":auth.data_space_id,"groups":auth.groups,
        "old_manifest_digest":auth.old_manifest_digest,"inventory_digest":auth.inventory_digest,
        "new_identity_digest":auth.new_identity_digest,"identity":auth.startup.identity,
        "directory_version":auth.startup.directory.version,"directory_nodes":auth.startup.directory.nodes})
}
fn prepare(evidence: &AdoptionEvidence, auth: &UpgradeAuthorization) -> io::Result<Intent> {
    if auth.session_id == [0; 16]
        || auth.authorization_id == [0; 16]
        || auth.cold_manifest_sha256 == [0; 32]
        || auth.physical_domain == [0; 32]
        || auth.root != evidence.root
        || auth.node_id != evidence.node
        || auth.data_space_id != evidence.space
        || auth.inventory_digest != evidence.inventory
        || auth.old_manifest_digest != evidence.layout.digest().0
        || auth.groups != evidence.replicas.iter().map(|r| r.group).collect()
        || auth.startup.disposition != SlotStartupDisposition::RecoverExisting
        || auth.startup.identity.data_space_id != evidence.space
        || auth.new_identity_digest != evidence.new_identity_digest(&auth.startup.identity)?
    {
        return Err(invalid(
            "maintenance authorization does not bind exact original evidence",
        ));
    }
    let old: super::Manifest =
        serde_json::from_slice(&evidence.manifest).map_err(io::Error::other)?;
    if old.layout != *evidence.layout.config()
        || old.local_node_id != evidence.node
        || evidence.files.get(Path::new(crate::MANIFEST_FILE_NAME))
            != Some(&Sha256::digest(&evidence.manifest).into())
        || evidence.replicas.len() != auth.groups.len()
    {
        return Err(invalid(
            "stored original manifest or group inventory mismatch",
        ));
    }
    for original in &evidence.replicas {
        if original.live != multiraft_store::DiskFsmDb::group_path(&evidence.root, original.group)
            || original.workspace != multiraft_store::DiskFsmDb::workspace_path(&original.live)?
        {
            return Err(invalid(
                "adoption evidence paths escape the exact authorized group scope",
            ));
        }
        multiraft_store::adoption::verify_adoption_fsm(original)?;
    }
    let fingerprint = evidence.transport_fingerprint(&auth.startup)?;
    let authorization = authorization_value(auth);
    let authorization_digest =
        Sha256::digest(serde_json::to_vec(&authorization).map_err(io::Error::other)?).into();
    let identities = evidence
        .replicas
        .iter()
        .map(|r| {
            Ok((
                r.group,
                evidence.replica_identity(r.group, &auth.startup.identity)?,
            ))
        })
        .collect::<io::Result<_>>()?;
    let new_manifest =
        crate::strict_manifest::adoption_manifest(&evidence.layout, evidence.node, &auth.startup)?;
    Ok(Intent {
        version: 1,
        authorization,
        receipt: AdoptionReceipt {
            authorization_digest,
            node_id: evidence.node,
            generation: 1,
            identities,
            fingerprint,
        },
        root: evidence.root.clone(),
        layout: evidence.layout.config().clone(),
        node: evidence.node,
        space: evidence.space,
        files: evidence.files.clone(),
        replicas: evidence.replicas.clone(),
        manifest: evidence.manifest.clone(),
        passport: evidence.passport.clone(),
        new_manifest,
    })
}
fn publish(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if path.exists() {
        if fs::read(path)? != bytes {
            return Err(invalid("immutable adoption record conflict"));
        }
        fs::File::open(path)?.sync_all()?;
        fs::File::open(
            path.parent()
                .ok_or_else(|| invalid("record parent missing"))?,
        )?
        .sync_all()?;
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("record parent missing"))?;
    fs::create_dir_all(parent)?;
    let pending = parent.join(format!(".{:x}.pending", Sha256::digest(bytes)));
    match fs::remove_file(&pending) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::hard_link(&pending, path)?;
    fs::remove_file(pending)?;
    fs::File::open(parent)?.sync_all()?;
    if let Some(parent) = parent.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
fn verify_payload(intent: &Intent) -> io::Result<()> {
    let actual = inventory(&intent.root)?;
    let protected = |path: &Path| {
        !path.starts_with("raft-rocksdb")
            && !path.starts_with("controlled-adoption")
            && path != Path::new(crate::MANIFEST_FILE_NAME)
    };
    let expected: BTreeMap<_, _> = intent.files.iter().filter(|(p, _)| protected(p)).collect();
    let current: BTreeMap<_, _> = actual.iter().filter(|(p, _)| protected(p)).collect();
    if current != expected
        || fs::read(
            intent
                .root
                .parent()
                .ok_or_else(|| invalid("parent missing"))?
                .join("data-space.json"),
        )? != intent.passport
    {
        return Err(invalid(
            "actual legacy payload or passport changed during conversion",
        ));
    }
    let manifest = fs::read(intent.root.join(crate::MANIFEST_FILE_NAME))?;
    if manifest != intent.manifest && manifest != intent.new_manifest {
        return Err(invalid("conversion manifest conflict"));
    }
    Ok(())
}
pub(super) fn inspect_resume(root: &Path) -> io::Result<AdoptionEvidence> {
    let intent: Intent =
        serde_json::from_slice(&fs::read(adoption_directory(root).join("intent.json"))?)
            .map_err(io::Error::other)?;
    if intent.version != 1
        || intent.root != root
        || intent.node != intent.receipt.node_id
        || intent.receipt.generation != 1
    {
        return Err(invalid("conversion intent identity mismatch"));
    }
    verify_payload(&intent)?;
    Ok(AdoptionEvidence {
        root: intent.root,
        layout: ClusterLayout::new(intent.layout).map_err(io::Error::other)?,
        node: intent.node,
        space: intent.space,
        inventory: digest(&intent.files),
        files: intent.files,
        replicas: intent.replicas,
        manifest: intent.manifest,
        passport: intent.passport,
    })
}

/// Convert exactly the inspected cold original groups under one trusted authorization.
/// Intent precedes enrollment; completion follows every durable adoption anchor.
/// Same-authorization crash resume is allowed; conflicting authorization never repairs data.
///
/// # Errors
/// Rejects changed inventory, missing/conflicting evidence and every authorization mismatch.
pub fn adopt_existing(
    evidence: &AdoptionEvidence,
    auth: &UpgradeAuthorization,
) -> io::Result<AdoptionReceipt> {
    // A kernel-owned directory lock serializes maintenance writers without adding
    // a lock file to the original inventory; process death releases the lock.
    let owner = fs::File::open(&evidence.root)?;
    owner.lock()?;
    let expected = prepare(evidence, auth)?;
    let directory = adoption_directory(&evidence.root);
    let path = directory.join("intent.json");
    let bytes = serde_json::to_vec(&expected).map_err(io::Error::other)?;
    if path.exists() {
        if fs::read(&path)? != bytes {
            return Err(invalid(
                "adoption already belongs to another authorization or evidence",
            ));
        }
        verify_payload(&expected)?;
    } else {
        let fresh = inspect_existing(&evidence.root)?;
        if fresh.inventory != evidence.inventory
            || fresh.manifest != evidence.manifest
            || fresh.passport != evidence.passport
            || fresh.replicas != evidence.replicas
        {
            return Err(invalid("legacy evidence changed after inspection"));
        }
        publish(&path, &bytes)?;
    }
    let log_path = evidence.root.join("raft-rocksdb");
    if !log_path.join("CURRENT").is_file() {
        return Err(invalid("legacy log disappeared before conversion"));
    }
    let db = RocksLogDb::<TypeConfig>::open(&log_path)?;
    // Verify all namespaces before converting the first one. The writable engine
    // holds RocksDB exclusivity against every other native startup.
    for original in &evidence.replicas {
        db.verify_adoption_log(original, &expected.receipt.identities[&original.group])?;
    }
    let mut anchors = BTreeMap::new();
    for original in &evidence.replicas {
        let identity = &expected.receipt.identities[&original.group];
        // Existing generation receipts compare exact authorization-derived physical identity.
        db.enroll_controlled(original.group, 1, identity)?;
        let anchor = AdoptionAnchor {
            version: 1,
            authorization_digest: expected.receipt.authorization_digest,
            root: evidence.root.clone(),
            node: evidence.node,
            generation: 1,
            identity: identity.clone(),
            fingerprint: expected.receipt.fingerprint,
            original: original.clone(),
        };
        let bytes = serde_json::to_vec(&anchor).map_err(io::Error::other)?;
        publish(
            &directory.join(format!("group-{}.anchor.json", original.group)),
            &bytes,
        )?;
        anchors.insert(original.group, Sha256::digest(&bytes).into());
    }
    let manifest_path = evidence.root.join(crate::MANIFEST_FILE_NAME);
    if fs::read(&manifest_path)? != expected.new_manifest {
        let pending = directory.join("manifest.pending");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)?;
        file.write_all(&expected.new_manifest)?;
        file.sync_all()?;
        fs::rename(pending, manifest_path)?;
        fs::File::open(&evidence.root)?.sync_all()?;
    }
    let complete = AdoptionComplete {
        version: 1,
        authorization_digest: expected.receipt.authorization_digest,
        node: evidence.node,
        root: evidence.root.clone(),
        anchors,
    };
    publish(
        &directory.join("complete.json"),
        &serde_json::to_vec(&complete).map_err(io::Error::other)?,
    )?;
    Ok(expected.receipt)
}
