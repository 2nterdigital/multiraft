//! Read actual cold legacy stores without creating namespaces or rewriting payloads.
mod anchor;
use crate::{disk_snapshot::DiskManifest, DiskFsmDb, DISK_FSM_META_CF};
pub use anchor::{adoption_directory, load_adoption_anchor, AdoptionAnchor, AdoptionComplete};
use multiraft_core::{
    typ::{LogId, StoredMembership, Vote},
    TypeConfig,
};
use openraft::alias::DefaultEntryOf;
use openraft::{entry::RaftEntry, EntryPayload};
use rocksdb::{IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};

/// Actual persistent facts from one cold original replica.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyReplica {
    pub group: u64,
    pub applied: LogId,
    pub membership: StoredMembership,
    pub committed: Option<LogId>,
    pub tail: Option<LogId>,
    pub purged: Option<LogId>,
    pub vote: Option<Vote>,
    pub log_digest: [u8; 32],
    pub live: PathBuf,
    pub workspace: PathBuf,
    pub checkpoints: Vec<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Applied {
    version: u32,
    last_applied: LogId,
    membership: StoredMembership,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    ever_written: bool,
    vote: Option<Vote>,
    committed: Option<LogId>,
    purged: Option<LogId>,
    tail: Option<LogId>,
    bytes: u64,
}
pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(io::Error::other)
}
fn existing(path: &Path, cf: bool) -> io::Result<DB> {
    if !path.join("CURRENT").is_file() || std::fs::symlink_metadata(path)?.file_type().is_symlink()
    {
        return Err(invalid("legacy database is missing or linked"));
    }
    let options = Options::default();
    if cf {
        let names = DB::list_cf(&options, path).map_err(io::Error::other)?;
        DB::open_cf_for_read_only(&options, path, names, false).map_err(io::Error::other)
    } else {
        DB::open_for_read_only(&options, path, false).map_err(io::Error::other)
    }
}
fn applied(path: &Path, group: u64) -> io::Result<Applied> {
    let db = existing(path, true)?;
    let cf = db
        .cf_handle(DISK_FSM_META_CF)
        .ok_or_else(|| invalid("missing native FSM metadata"))?;
    if db
        .get_cf(cf, b"schema")
        .map_err(io::Error::other)?
        .as_deref()
        != Some(b"multiraft-disk-fsm-v1")
        || db
            .get_cf(cf, b"group")
            .map_err(io::Error::other)?
            .as_deref()
            != Some(group.to_be_bytes().as_slice())
        || db
            .get_cf(cf, b"app_format")
            .map_err(io::Error::other)?
            .is_none()
    {
        return Err(invalid("legacy FSM markers do not match"));
    }
    let raw = db
        .get_cf(cf, b"applied")
        .map_err(io::Error::other)?
        .ok_or_else(|| invalid("missing actual applied record"))?;
    let record: Applied = decode(&raw)?;
    if record.version != 1
        || record.membership.log_id().is_none()
        || record
            .membership
            .log_id()
            .is_some_and(|id| id.index > record.last_applied.index || id > record.last_applied)
        || record.membership.membership().get_joint_config().len() != 1
        || record
            .membership
            .membership()
            .learner_ids()
            .next()
            .is_some()
        || record.membership.membership().voter_ids().count() != 3
    {
        return Err(invalid(
            "legacy applied membership is not an original uniform RF3 configuration",
        ));
    }
    Ok(record)
}
fn covers(later: LogId, earlier: LogId) -> bool {
    later.index >= earlier.index
        && later >= earlier
        && (later.index != earlier.index || later == earlier)
}

/// Inspect existing logs, live applied state and each published checkpoint.
/// Paths are supplied from observed storage ownership, independently of generation numbers.
/// This opens RocksDB read-only and never follows a descriptor's absolute source path.
///
/// # Errors
/// Missing state, inconsistent full log identities, malformed memberships and snapshot proofs fail closed.
pub fn inspect_legacy_replica(
    log_path: &Path,
    live: &Path,
    workspace: &Path,
    group: u64,
) -> io::Result<LegacyReplica> {
    if DiskFsmDb::workspace_path(live)? != workspace {
        return Err(invalid(
            "legacy workspace does not belong to the live store",
        ));
    }
    let actual = applied(live, group)?;
    let log = existing(log_path, false)?;
    if log.get(b"schema").map_err(io::Error::other)?.as_deref() != Some(b"multiraft-rocks-log-v1") {
        return Err(invalid("not an unconverted legacy log engine"));
    }
    let mut prefix = vec![b'g'];
    prefix.extend_from_slice(&group.to_be_bytes());
    let mut metadata = None;
    let mut entries = BTreeMap::new();
    let mut digest = Sha256::new();
    let mut schema = false;
    let mut bytes = 0u64;
    for row in log.iterator(IteratorMode::Start) {
        let (key, value) = row.map_err(io::Error::other)?;
        if !key.starts_with(&prefix) {
            continue;
        }
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(&key);
        let canonical = canonical_log_value(key.get(9).copied(), &value)?;
        digest.update((canonical.len() as u64).to_be_bytes());
        digest.update(&canonical);
        match key.get(9) {
            Some(b'm') if key.len() == 10 => {
                let object: serde_json::Value = decode(&value)?;
                for field in [
                    "ever_written",
                    "vote",
                    "committed",
                    "purged",
                    "tail",
                    "bytes",
                ] {
                    if object.get(field).is_none() {
                        return Err(invalid(
                            "legacy metadata is missing a required recovery field",
                        ));
                    }
                }
                metadata = Some(decode::<Metadata>(&value)?);
            }
            Some(b'r') if key.len() == 10 && value.as_ref() == b"multiraft-rocks-log-v1" => {
                schema = true
            }
            Some(b'e') if key.len() == 18 => {
                let entry: DefaultEntryOf<TypeConfig> = decode(&value)?;
                let index = u64::from_be_bytes(key[10..].try_into().map_err(io::Error::other)?);
                if entry.log_id().index != index {
                    return Err(invalid("legacy entry identity/key mismatch"));
                }
                bytes = bytes
                    .checked_add(value.len() as u64)
                    .ok_or_else(|| invalid("log bytes overflow"))?;
                entries.insert(index, entry);
            }
            _ => return Err(invalid("unknown legacy log namespace key")),
        }
    }
    let meta = metadata.ok_or_else(|| invalid("missing legacy log metadata"))?;
    if !schema
        || !meta.ever_written
        || bytes != meta.bytes
        || entries.values().next_back().map(|e| e.log_id) != meta.tail
    {
        return Err(invalid("legacy log metadata differs from real entries"));
    }
    let mut next = meta.purged.map_or(Some(0), |id| id.index.checked_add(1));
    let mut previous = None;
    for entry in entries.values() {
        if previous.is_some_and(|id| entry.log_id <= id)
            || matches!(&entry.payload,EntryPayload::Normal(request) if request.control.is_some())
        {
            return Err(invalid(
                "legacy log identities are nonmonotonic or already contain controlled entries",
            ));
        }
        previous = Some(entry.log_id);
        if next != Some(entry.log_id.index) {
            return Err(invalid("legacy log gap"));
        }
        next = entry.log_id.index.checked_add(1);
    }
    let retained_or_purged = |id: LogId| {
        entries
            .get(&id.index)
            .is_some_and(|entry| entry.log_id == id)
            || meta.purged.is_some_and(|purged| covers(purged, id))
    };
    if !retained_or_purged(actual.last_applied)
        || meta.committed.is_some_and(|id| !retained_or_purged(id))
    {
        return Err(invalid(
            "legacy applied/committed identity has no retained or purged proof",
        ));
    }
    let mut membership_proven = false;
    if let Some(id) = actual.membership.log_id() {
        if let Some(entry) = entries.get(&id.index) {
            if entry.log_id != *id
                || !matches!(&entry.payload, EntryPayload::Membership(m) if m == actual.membership.membership())
            {
                return Err(invalid(
                    "legacy membership differs from its actual native log entry",
                ));
            }
            membership_proven = true;
        }
    }
    let mut checkpoints = Vec::new();
    for row in std::fs::read_dir(workspace)? {
        let row = row?;
        if row.path().extension().and_then(|s| s.to_str()) != Some("manifest") {
            continue;
        }
        let manifest = DiskManifest::decode(&std::fs::read(row.path())?, group)?;
        let checkpoint = row.path().with_extension("");
        if manifest.source.file_name() != checkpoint.file_name() {
            return Err(invalid("legacy checkpoint source identity mismatch"));
        }
        let cut = applied(&checkpoint, group)?;
        if manifest.meta.last_log_id != Some(cut.last_applied)
            || manifest.meta.last_membership != cut.membership
            || !covers(actual.last_applied, cut.last_applied)
        {
            return Err(invalid(
                "legacy checkpoint actual state differs from its descriptor",
            ));
        }
        for file in manifest.files {
            let metadata = std::fs::symlink_metadata(checkpoint.join(file.name))?;
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() != file.size
            {
                return Err(invalid("legacy checkpoint file inventory mismatch"));
            }
        }
        membership_proven |= cut.membership == actual.membership;
        checkpoints.push(checkpoint);
    }
    if !membership_proven {
        return Err(invalid(
            "legacy membership has no actual retained log or checkpoint proof",
        ));
    }
    if checkpoints.is_empty() {
        return Err(invalid("legacy replica has no published checkpoint proof"));
    }
    checkpoints.sort();
    Ok(LegacyReplica {
        group,
        applied: actual.last_applied,
        membership: actual.membership,
        committed: meta.committed,
        tail: meta.tail,
        purged: meta.purged,
        vote: meta.vote,
        log_digest: digest.finalize().into(),
        live: live.to_owned(),
        workspace: workspace.to_owned(),
        checkpoints,
    })
}

/// Stable digest encoding ignores only the explicit controlled-generation metadata addition.
pub(crate) fn canonical_log_value(tag: Option<u8>, value: &[u8]) -> io::Result<Vec<u8>> {
    if tag == Some(b'r') {
        return Ok(value.to_vec());
    }
    let mut value: serde_json::Value = serde_json::from_slice(value).map_err(io::Error::other)?;
    if tag == Some(b'm') {
        value
            .as_object_mut()
            .ok_or_else(|| invalid("metadata is not object"))?
            .remove("controlled_generation");
    }
    serde_json::to_vec(&value).map_err(io::Error::other)
}

/// Independently recheck original native FSM facts before a conversion resumes.
/// Critical facts recovered from a durable conversion record are never trusted alone.
///
/// # Errors
/// Actual live/checkpoint records must retain the exact original full identities.
pub fn verify_adoption_fsm(original: &LegacyReplica) -> io::Result<()> {
    let live = applied(&original.live, original.group)?;
    if live.last_applied != original.applied
        || live.membership != original.membership
        || DiskFsmDb::workspace_path(&original.live)? != original.workspace
    {
        return Err(invalid(
            "actual original FSM state differs from adoption evidence",
        ));
    }
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&original.workspace)? {
        let entry = entry?;
        if entry.path().extension().and_then(|s| s.to_str()) != Some("manifest") {
            continue;
        }
        let checkpoint = entry.path().with_extension("");
        let manifest = DiskManifest::decode(&std::fs::read(entry.path())?, original.group)?;
        let cut = applied(&checkpoint, original.group)?;
        if manifest.meta.last_log_id != Some(cut.last_applied)
            || manifest.meta.last_membership != cut.membership
            || !covers(original.applied, cut.last_applied)
            || manifest.source.file_name() != checkpoint.file_name()
        {
            return Err(invalid(
                "actual original checkpoint proof differs from adoption evidence",
            ));
        }
        found.push(checkpoint);
    }
    found.sort();
    if found != original.checkpoints {
        return Err(invalid("original checkpoint inventory changed"));
    }
    Ok(())
}
