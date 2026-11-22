//! Mixed running with multiraft nodes on OpenRaft 0.10.0-alpha.30.
//!
//! `fixtures/alpha30/` holds bytes written by the alpha.30 build of the
//! serde golden-sample harness (multiraft's codecs: legacy bincode and JSON).
//! Layouts without a fixture (disk manifest, native metadata, seed header) are
//! restated below as `Alpha30*` structs with alpha.30's field order and
//! `deny_unknown_fields`. The manifest check restates every rule of alpha.30's
//! `DiskManifest::decode`; the native metadata and seed header checks restate
//! only the rules that read `snapshot_id` or compare the meta.
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use multiraft_core::typ::{AppendEntriesResponse, LogId, RaftError, StoredMembership, Vote};
use multiraft_core::{OperationId, OperationRecord, ReplacementSpec, SnapshotMetaWithId};
use multiraft_net::portable::NativeSeedHeader;
use multiraft_net::{decode, encode};
use multiraft_store::disk_snapshot::{
    disk_snapshot_meta, wire_snapshot_meta, DiskManifest, ManifestFile,
};
use multiraft_store::{is_disk_snapshot, SnapshotCatalog};
use openraft::raft::AppendError;
use openraft::vote::RaftLeaderIdExt;
use openraft::{BasicNode, Membership};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const SNAPSHOT_ID: &str = "disk-v1-7-2-102-abcdef";
/// Alpha.30's disk manifest magic.
const MAGIC: &[u8] = b"multiraft-disk-snapshot-v1\n";

fn lid(term: u64, node: u64, index: u64) -> LogId {
    LogId::new(
        openraft::alias::LeaderIdOf::<multiraft_core::TypeConfig>::new_committed(term, node),
        index,
    )
}

/// The fixtures' membership: joint `{1,2,3}` / `{2,3,4}`, five nodes.
fn membership() -> StoredMembership {
    let configs = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])];
    let nodes: BTreeMap<u64, BasicNode> = (1..=5)
        .map(|i| (i, BasicNode::new(format!("10.0.0.{i}:7000"))))
        .collect();
    StoredMembership::new(
        Some(lid(3, 1, 40)),
        Membership::new(configs, nodes).unwrap(),
    )
}

fn golden_meta() -> SnapshotMetaWithId {
    SnapshotMetaWithId {
        last_log_id: Some(lid(7, 2, 102)),
        last_membership: membership(),
        snapshot_id: SNAPSHOT_ID.into(),
    }
}

/// Alpha.30 `openraft::SnapshotMeta`.
#[derive(Debug, PartialEq, Deserialize)]
struct Alpha30SnapshotMeta {
    last_log_id: Option<LogId>,
    last_membership: StoredMembership,
    snapshot_id: String,
}
impl PartialEq<SnapshotMetaWithId> for Alpha30SnapshotMeta {
    fn eq(&self, other: &SnapshotMetaWithId) -> bool {
        self.last_log_id == other.last_log_id
            && self.last_membership == other.last_membership
            && self.snapshot_id == other.snapshot_id
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Alpha30ManifestFile {
    name: String,
    size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Alpha30DiskManifest {
    group: u64,
    meta: Alpha30SnapshotMeta,
    files: Vec<Alpha30ManifestFile>,
    source: PathBuf,
}

/// Alpha.30 `DiskManifest::decode` (`multiraft-store/src/disk_snapshot.rs` at
/// dev `035956c`), every acceptance rule restated; it panics where alpha.30
/// returns an error.
fn alpha30_decode_manifest(bytes: &[u8], group: u64) -> Alpha30DiskManifest {
    const MAX_FILES: usize = 100_000;
    const MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
    assert!(bytes.len() <= MAX_MANIFEST_BYTES);
    let manifest: Alpha30DiskManifest =
        serde_json::from_slice(bytes.strip_prefix(MAGIC).unwrap()).unwrap();
    assert_eq!(manifest.group, group);
    assert!(manifest.meta.snapshot_id.starts_with("disk-v1-"));
    assert!(manifest.meta.snapshot_id.len() <= 4096);
    assert!(manifest.meta.last_log_id.is_some());
    assert!(manifest.meta.last_membership.log_id().as_ref() <= manifest.meta.last_log_id.as_ref());
    assert!(!manifest.files.is_empty() && manifest.files.len() <= MAX_FILES);
    let mut names = BTreeSet::new();
    for file in &manifest.files {
        let name = file.name.as_str();
        assert!(
            !name.is_empty()
                && name.len() <= 255
                && name != "."
                && name != ".."
                && !name.contains(['/', '\\', '\0'])
                && names.insert(name)
        );
    }
    manifest
        .files
        .iter()
        .try_fold(0_u64, |total, file| total.checked_add(file.size))
        .unwrap();
    manifest
}

fn manifest() -> DiskManifest {
    DiskManifest {
        group: 7,
        meta: golden_meta(),
        files: vec![ManifestFile {
            name: "CURRENT".into(),
            size: 16,
        }],
        source: "/x".into(),
    }
}

#[test]
fn snapshot_meta_bytes_equal_alpha30() {
    let bin = include_bytes!("fixtures/alpha30/snapshot_meta.bin");
    let json = include_bytes!("fixtures/alpha30/snapshot_meta.json");
    assert_eq!(encode(golden_meta()), bin);
    assert_eq!(serde_json::to_vec(&golden_meta()).unwrap(), json);
    assert_eq!(decode::<SnapshotMetaWithId>(bin).unwrap(), golden_meta());
    assert_eq!(
        serde_json::from_slice::<SnapshotMetaWithId>(json).unwrap(),
        golden_meta()
    );
}

#[test]
fn snapshot_rpc_tuple_equals_alpha30_and_keeps_the_manifest_id() {
    let data = b"\x00snapshot-bytes\xff".to_vec();
    let tuple = (Vote::new_committed(7, 2), golden_meta(), data.clone());
    assert_eq!(
        encode(&tuple),
        include_bytes!("fixtures/alpha30/snapshot_rpc.bin")
    );
    let (vote, meta, payload): (Vote, Alpha30SnapshotMeta, Vec<u8>) =
        decode(&encode(&tuple)).unwrap();
    assert_eq!((vote, &payload), (tuple.0, &data));
    assert_eq!(meta, golden_meta());

    // A disk sender puts the manifest's own metadata on the wire: alpha.30
    // classifies it as a disk snapshot and `verify_disk_snapshot` requires
    // `manifest.meta == meta`, id included.
    let bytes = manifest().encode().unwrap();
    let wire = wire_snapshot_meta(7, &golden_meta().meta(), &bytes).unwrap();
    assert_eq!(wire, golden_meta());
    assert!(is_disk_snapshot(&wire, &bytes));
    let (_, old_meta, old_bytes): (Vote, Alpha30SnapshotMeta, Vec<u8>) =
        decode(&encode((Vote::new_committed(7, 2), &wire, &bytes))).unwrap();
    assert!(old_bytes.starts_with(MAGIC) && old_meta.snapshot_id.starts_with("disk-v1-"));
    assert_eq!(alpha30_decode_manifest(&old_bytes, 7).meta, wire);
    assert_eq!(old_meta, wire);

    // A whole image gets a non-empty id that alpha.30 never takes for a disk one.
    let image = wire_snapshot_meta(7, &golden_meta().meta(), &data).unwrap();
    let (_, old_image, _): (Vote, Alpha30SnapshotMeta, Vec<u8>) =
        decode(&encode((Vote::new_committed(7, 2), &image, &data))).unwrap();
    assert!(!old_image.snapshot_id.is_empty());
    assert!(!old_image.snapshot_id.starts_with("disk-v1-"));
    assert_eq!(old_image.last_log_id, golden_meta().last_log_id);
}

#[test]
fn disk_manifests_cross_both_ways() {
    // New -> alpha.30.
    let decoded = alpha30_decode_manifest(&manifest().encode().unwrap(), 7);
    assert_eq!(decoded.meta, golden_meta());
    assert_eq!(decoded.group, 7);

    // Alpha.30 manifest on disk -> new, around the alpha.30 metadata bytes.
    let mut old = MAGIC.to_vec();
    old.extend_from_slice(br#"{"group":7,"meta":"#);
    old.extend_from_slice(include_bytes!("fixtures/alpha30/snapshot_meta.json"));
    old.extend_from_slice(br#","files":[{"name":"CURRENT","size":16}],"source":"/x"}"#);
    assert_eq!(DiskManifest::decode(&old, 7).unwrap(), manifest());
    assert_eq!(
        disk_snapshot_meta(7, &golden_meta().meta(), &old),
        Some(golden_meta())
    );
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Alpha30NativeMeta {
    version: u32,
    group: u64,
    meta: Alpha30SnapshotMeta,
    size: usize,
    data_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Alpha30Active {
    version: u32,
    group: u64,
    generation: String,
    metadata_sha256: String,
}

fn hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn native_catalog_metadata_crosses_both_ways() {
    // New -> alpha.30: the published generation's metadata keeps a non-empty id.
    let root = tempfile::tempdir().unwrap();
    let catalog = SnapshotCatalog::new(root.path(), 1);
    let mut meta = golden_meta();
    meta.snapshot_id = "native-7-7-2-102-1".into();
    catalog.publish_native(7, &meta, b"image", 1024).unwrap();
    let native = root.path().join("7/native-v1");
    let active: Alpha30Active =
        serde_json::from_slice(&std::fs::read(native.join("active.json")).unwrap()).unwrap();
    let old: Alpha30NativeMeta = serde_json::from_slice(
        &std::fs::read(native.join(&active.generation).join("native-meta.json")).unwrap(),
    )
    .unwrap();
    assert!(!old.meta.snapshot_id.is_empty() && old.meta.snapshot_id.len() <= 4096);
    assert_eq!(old.meta, meta);

    // Alpha.30 generation on disk -> new, around the alpha.30 metadata bytes.
    let root = tempfile::tempdir().unwrap();
    let data = b"old image";
    let mut metadata = br#"{"version":1,"group":7,"meta":"#.to_vec();
    metadata.extend_from_slice(include_bytes!("fixtures/alpha30/snapshot_meta.json"));
    metadata.extend_from_slice(
        format!(r#","size":{},"data_sha256":"{}"}}"#, data.len(), hex(data)).as_bytes(),
    );
    let generation = hex(&metadata);
    let dir = root.path().join("7/native-v1").join(&generation);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("data.bin"), data).unwrap();
    std::fs::write(dir.join("native-meta.json"), &metadata).unwrap();
    std::fs::write(
        root.path().join("7/native-v1/active.json"),
        format!(
            r#"{{"version":1,"group":7,"generation":"{generation}","metadata_sha256":"{}"}}"#,
            hex(&metadata)
        ),
    )
    .unwrap();
    let loaded = SnapshotCatalog::new(root.path(), 1)
        .load_native(7, 1024)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.meta, golden_meta());
    assert_eq!(loaded.data, data);
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Alpha30SeedHeader {
    #[allow(dead_code)]
    record: OperationRecord,
    #[allow(dead_code)]
    vote: Vote,
    meta: Alpha30SnapshotMeta,
    manifest: Vec<u8>,
}

#[test]
fn controlled_seed_header_is_accepted_by_alpha30() {
    let header = NativeSeedHeader {
        record: OperationRecord {
            specification: ReplacementSpec {
                operation: OperationId {
                    sequence: 1,
                    nonce: [7; 16],
                },
                expected_start: membership(),
                target_voters: BTreeSet::from([2, 3, 4]),
                target_generation: 1,
            },
            executor_epoch: 1,
            terminal: None,
        },
        vote: Vote::new_committed(7, 2),
        meta: golden_meta(),
        manifest: manifest().encode().unwrap(),
    };
    let old: Alpha30SeedHeader =
        serde_json::from_slice(&serde_json::to_vec(&header).unwrap()).unwrap();
    // Alpha.30 `NativeTargetEndpoint::start` refuses unless the manifest's
    // metadata equals the header's.
    assert_eq!(
        alpha30_decode_manifest(&old.manifest, 7).meta,
        golden_meta()
    );
    assert_eq!(old.meta, golden_meta());
}

type WireAppend = Result<AppendEntriesResponse, RaftError>;

#[test]
fn append_responses_keep_the_alpha30_bytes() {
    let success_bin: &[u8] = include_bytes!("fixtures/alpha30/append_resp_success.bin");
    let success_json: &[u8] = include_bytes!("fixtures/alpha30/append_resp_success.json");
    let prev = Some(lid(7, 2, 99));
    let last = Some(lid(7, 2, 102));
    type Case = (
        &'static [u8],
        &'static [u8],
        Option<LogId>,
        Option<LogId>,
        openraft::raft::AppendResult<multiraft_core::TypeConfig>,
    );
    let cases: [Case; 5] = [
        (success_bin, success_json, prev, last, Ok(last)),
        // Heartbeat: no entries, so the acknowledged position is `prev`.
        (success_bin, success_json, last, last, Ok(last)),
        // First append to an empty log: no `prev`.
        (success_bin, success_json, None, prev, Ok(prev)),
        (
            include_bytes!("fixtures/alpha30/append_resp_conflict.bin"),
            include_bytes!("fixtures/alpha30/append_resp_conflict.json"),
            prev,
            last,
            Err(AppendError::Conflict {
                log_id: lid(7, 2, 99),
                hint: None,
            }),
        ),
        (
            include_bytes!("fixtures/alpha30/append_resp_higher_vote.bin"),
            include_bytes!("fixtures/alpha30/append_resp_higher_vote.json"),
            prev,
            last,
            Err(AppendError::HigherVote(Vote::new(8, 3))),
        ),
    ];
    for (bin, json, prev, last, result) in cases {
        // Follower side, `/raft/append` (bincode) and `/raft/append-json/v1`.
        let wire: WireAppend = Ok(AppendEntriesResponse::from(result.clone()));
        assert_eq!(encode(&wire), bin);
        assert_eq!(serde_json::to_vec(&wire).unwrap(), json);
        // Leader side, from an alpha.30 follower.
        for old in [
            decode::<WireAppend>(bin).unwrap(),
            serde_json::from_slice::<WireAppend>(json).unwrap(),
        ] {
            assert_eq!(old.unwrap().into_append_result(prev, last), result);
        }
    }
}
