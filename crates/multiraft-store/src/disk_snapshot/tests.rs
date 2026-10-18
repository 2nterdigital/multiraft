use super::*;
use multiraft_core::typ::LogId;
use openraft::alias::LeaderIdOf;
use openraft::vote::RaftLeaderIdExt;
use std::fs;
use std::path::Path;

fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue-190/unit");
    fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

fn meta(index: u64) -> SnapshotMetaOf<TypeConfig> {
    SnapshotMetaOf::<TypeConfig> {
        last_log_id: Some(LogId::new(
            LeaderIdOf::<TypeConfig>::new_committed(2, 1),
            index,
        )),
        last_membership: Default::default(),
        snapshot_id: format!("{DISK_SNAPSHOT_ID_PREFIX}7-{index}-x"),
    }
}

fn manifest(files: &[(&str, u64)]) -> DiskManifest {
    DiskManifest {
        group: 7,
        meta: meta(5),
        files: files
            .iter()
            .map(|(name, size)| ManifestFile {
                name: (*name).into(),
                size: *size,
            })
            .collect(),
        source: "/nowhere".into(),
    }
}

#[test]
fn manifest_roundtrip_and_structural_refusals() {
    let good = manifest(&[("CURRENT", 16), ("000001.sst", 1 << 40)]);
    let bytes = good.encode().unwrap();
    assert!(is_disk_snapshot(&good.meta, &bytes));
    assert_eq!(DiskManifest::decode(&bytes, 7).unwrap(), good);
    assert!(DiskManifest::decode(&bytes, 8).is_err(), "other group");
    assert!(!is_disk_snapshot(&meta(5), b"whole image"));
    let mut whole = meta(5);
    whole.snapshot_id = "native-7-1-5-1".into();
    assert!(!is_disk_snapshot(&whole, &bytes), "id prefix is required");

    for bad in [
        manifest(&[]),
        manifest(&[("../escape", 1)]),
        manifest(&[("a/b", 1)]),
        manifest(&[("dup", 1), ("dup", 2)]),
        manifest(&[("a", u64::MAX), ("b", 1)]),
    ] {
        assert!(DiskManifest::decode(&bad.encode().unwrap(), 7).is_err());
    }
    let mut no_cut = manifest(&[("a", 1)]);
    no_cut.meta.last_log_id = None;
    assert!(DiskManifest::decode(&no_cut.encode().unwrap(), 7).is_err());
    assert!(DiskManifest::decode(b"multiraft-disk-snapshot-v1\n{", 7).is_err());
}

fn write_tree(dir: &Path, marker: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(dir.join("CURRENT"), marker).unwrap();
}

#[test]
fn interrupted_swap_restores_the_old_store_and_finished_swap_keeps_the_new() {
    let root = scratch();
    let live = root.path().join("group-7");
    let workspace = root.path().join("group-7.snapshots");
    write_tree(&live, "old");
    fs::create_dir_all(&workspace).unwrap();

    // Crash after the first rename: live is missing, retired holds the old store.
    fs::rename(&live, workspace.join("retired-1")).unwrap();
    recover_swap(&live, &workspace).unwrap();
    assert_eq!(fs::read_to_string(live.join("CURRENT")).unwrap(), "old");
    assert!(!workspace.join("retired-1").exists());

    // Crash after the second rename: the new store is live, retired is garbage.
    let staged = workspace.join("staging-1");
    write_tree(&staged, "new");
    fs::rename(&live, workspace.join("retired-2")).unwrap();
    fs::rename(&staged, &live).unwrap();
    recover_swap(&live, &workspace).unwrap();
    assert_eq!(fs::read_to_string(live.join("CURRENT")).unwrap(), "new");
    assert!(!workspace.join("retired-2").exists());
}

#[test]
fn swap_rollback_and_commit() {
    let root = scratch();
    let live = root.path().join("group-7");
    let workspace = root.path().join("group-7.snapshots");
    write_tree(&live, "old");
    let staged = workspace.join("staging-1");
    write_tree(&staged, "new");
    let swapped = install_dir(&live, &workspace, &staged).unwrap();
    assert_eq!(fs::read_to_string(live.join("CURRENT")).unwrap(), "new");
    swapped.rollback().unwrap();
    assert_eq!(fs::read_to_string(live.join("CURRENT")).unwrap(), "old");
    assert_eq!(
        fs::read_dir(&workspace).unwrap().count(),
        0,
        "nothing left behind"
    );

    write_tree(&staged, "new");
    install_dir(&live, &workspace, &staged)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(fs::read_to_string(live.join("CURRENT")).unwrap(), "new");
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
}

#[test]
fn workspace_keeps_only_the_newest_complete_checkpoint() {
    let root = scratch();
    let workspace = Workspace::open(root.path().join("ws")).unwrap();
    for index in [3, 9] {
        let capture = workspace.temp_checkpoint();
        write_tree(capture.path(), "x");
        let published = workspace.publish(7, capture, meta(index)).unwrap();
        assert_eq!(published.manifest.meta.last_log_id, meta(index).last_log_id);
    }
    // An older capture loses to the published newer snapshot.
    let stale = workspace.temp_checkpoint();
    write_tree(stale.path(), "y");
    let stale_path = stale.path().to_owned();
    let kept = workspace.publish(7, stale, meta(4)).unwrap();
    assert_eq!(kept.manifest.meta.last_log_id, meta(9).last_log_id);
    assert!(!stale_path.exists(), "losing capture removed");
    assert_eq!(fs::read_dir(root.path().join("ws")).unwrap().count(), 2);

    // Leftovers of interrupted work vanish on open; the published one stays.
    fs::create_dir(root.path().join("ws/staging-1")).unwrap();
    fs::create_dir(root.path().join("ws/tmp-1")).unwrap();
    fs::create_dir(root.path().join("ws/checkpoint-00000000000000000099-z")).unwrap();
    let reopened = Workspace::open(root.path().join("ws")).unwrap();
    let latest = reopened.latest(7).unwrap().unwrap();
    assert_eq!(latest.manifest.meta.last_log_id, meta(9).last_log_id);
    assert_eq!(fs::read_dir(root.path().join("ws")).unwrap().count(), 2);
}

#[test]
fn chunk_stream_roundtrip_survives_republish_and_rejects_corruption() {
    let root = scratch();
    let workspace = Workspace::open(root.path().join("ws")).unwrap();
    let capture = workspace.temp_checkpoint();
    fs::create_dir_all(capture.path()).unwrap();
    let big: Vec<u8> = (0..(MAX_CHUNK_BYTES * 2 + 17))
        .map(|i| (i * 31 % 251) as u8)
        .collect();
    fs::write(capture.path().join("big.sst"), &big).unwrap();
    fs::write(capture.path().join("empty"), b"").unwrap();
    let published = workspace.publish(7, capture, meta(5)).unwrap();

    let mut source = ChunkSource::open(published.bytes.clone(), 7).unwrap();
    // A newer compaction removes the published directory; the lease keeps the files.
    let newer = workspace.temp_checkpoint();
    write_tree(newer.path(), "n");
    workspace.publish(7, newer, meta(6)).unwrap();
    assert!(!published.manifest.source.exists());

    let receive = |corrupt: bool, source: &mut ChunkSource| -> io::Result<Incoming> {
        let mut incoming: Option<Incoming> = None;
        let mut seen = 0;
        while let Some(chunk) = source.next_chunk()? {
            match chunk {
                DiskChunk::Begin { transfer, manifest } => {
                    incoming = Some(Incoming::begin(&workspace, 7, transfer, manifest)?);
                }
                DiskChunk::Data {
                    file,
                    offset,
                    mut bytes,
                    sha256,
                    ..
                } => {
                    assert!(bytes.len() <= MAX_CHUNK_BYTES);
                    seen += 1;
                    if corrupt && seen == 1 {
                        bytes[0] ^= 1;
                    }
                    incoming
                        .as_mut()
                        .unwrap()
                        .write(file, offset, &bytes, sha256)?;
                }
            }
        }
        Ok(incoming.unwrap())
    };
    let incoming = receive(false, &mut source).unwrap();
    assert!(incoming.complete());
    assert_eq!(fs::read(incoming.dir().join("big.sst")).unwrap(), big);
    let staged = incoming.dir().to_owned();
    drop(incoming);
    assert!(!staged.exists(), "unconsumed staging is removed");

    let lease_parent = root.path().join("ws");
    drop(source);
    let leases = fs::read_dir(&lease_parent)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".send-")
        })
        .count();
    assert_eq!(leases, 0, "lease removed when the sender finishes");

    let current = workspace.latest(7).unwrap().unwrap();
    let mut source = ChunkSource::open(current.bytes, 7).unwrap();
    let error = receive(true, &mut source).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
}

#[test]
fn an_exact_retry_of_the_last_chunk_is_accepted_and_other_disorder_is_not() {
    let root = scratch();
    let workspace = Workspace::open(root.path().join("ws")).unwrap();
    let capture = workspace.temp_checkpoint();
    fs::create_dir_all(capture.path()).unwrap();
    let data: Vec<u8> = (0..(MAX_CHUNK_BYTES + 5)).map(|i| (i % 7) as u8).collect();
    fs::write(capture.path().join("a.sst"), &data).unwrap();
    let published = workspace.publish(7, capture, meta(5)).unwrap();
    let mut source = ChunkSource::open(published.bytes, 7).unwrap();
    let DiskChunk::Begin { transfer, manifest } = source.next_chunk().unwrap().unwrap() else {
        panic!("begin first");
    };
    let mut incoming = Incoming::begin(&workspace, 7, transfer, manifest).unwrap();
    let mut chunks = Vec::new();
    while let Some(DiskChunk::Data {
        file,
        offset,
        bytes,
        sha256,
        ..
    }) = source.next_chunk().unwrap()
    {
        chunks.push((file, offset, bytes, sha256));
    }
    let (file, offset, bytes, sha) = &chunks[0];
    incoming.write(*file, *offset, bytes, *sha).unwrap();
    incoming.write(*file, *offset, bytes, *sha).unwrap();
    let (file2, offset2, bytes2, sha2) = &chunks[1];
    let mut incoming = {
        let mut fresh =
            Incoming::begin(&workspace, 7, transfer, incoming.manifest_bytes().to_vec()).unwrap();
        fresh.write(*file, *offset, bytes, *sha).unwrap();
        assert!(
            fresh.write(*file2, offset2 + 1, bytes2, *sha2).is_err(),
            "gap refused"
        );
        Incoming::begin(&workspace, 7, transfer, fresh.manifest_bytes().to_vec()).unwrap()
    };
    for (file, offset, bytes, sha) in &chunks {
        incoming.write(*file, *offset, bytes, *sha).unwrap();
    }
    // Retrying the final (digest-carrying) chunk after the file completed is a no-op.
    let (file, offset, bytes, sha) = chunks.last().unwrap();
    incoming.write(*file, *offset, bytes, *sha).unwrap();
    assert!(incoming.complete());
    assert_eq!(fs::read(incoming.dir().join("a.sst")).unwrap(), data);
}
