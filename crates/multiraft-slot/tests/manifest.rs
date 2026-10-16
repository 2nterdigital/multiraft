use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use multiraft_slot::{
    ClusterLayout, HashVersion, LayoutConfig, NodeAddress, SlotAssignment, SlotError, SlotId,
    SlotManifest, FORMAT_VERSION, MANIFEST_FILE_NAME, MAX_KEY_LEN, MAX_NODES, MAX_SLOTS,
    MAX_VOTERS,
};

fn config(count: u32) -> LayoutConfig {
    LayoutConfig {
        cluster_id: "test-cluster".into(),
        format_version: FORMAT_VERSION,
        hash_version: HashVersion::Sha256V1,
        slot_count: count,
        group_id_base: 100,
        nodes: vec![
            NodeAddress {
                node_id: 2,
                raft: "127.0.0.1:2002".parse().unwrap(),
                application_rpc: "127.0.0.1:3002".parse().unwrap(),
            },
            NodeAddress {
                node_id: 1,
                raft: "127.0.0.1:2001".parse().unwrap(),
                application_rpc: "127.0.0.1:3001".parse().unwrap(),
            },
        ],
        slots: (0..count)
            .rev()
            .map(|index| SlotAssignment {
                slot: SlotId(index),
                voters: vec![2, 1],
            })
            .collect(),
    }
}

struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Honor the caller's project-local TMPDIR; never fall back to system temp.
        let root = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "slot-manifest-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn normalization_produces_identical_layout_digest_and_routes() {
    let original = config(17);
    let layout = ClusterLayout::new(original.clone()).unwrap();
    let mut reordered = original;
    reordered.nodes.reverse();
    reordered.slots.reverse();
    for slot in &mut reordered.slots {
        slot.voters.reverse();
    }
    let other = ClusterLayout::new(reordered).unwrap();
    assert_eq!(layout, other);
    assert_eq!(
        layout.digest().0,
        [
            36, 168, 99, 39, 42, 54, 34, 224, 87, 112, 164, 205, 171, 176, 181, 248, 56, 18, 255,
            201, 81, 25, 245, 91, 130, 73, 51, 111, 168, 1, 143, 104
        ]
    );
    assert_eq!(layout.nodes()[0].node_id, 1);
    for (key, expected) in [
        (b"".as_slice(), 11),
        (b"channel-a".as_slice(), 16),
        (b"channel-b".as_slice(), 9),
        (b"\x00\xff\x80".as_slice(), 13),
    ] {
        assert_eq!(layout.slot_for_key(key).unwrap(), SlotId(expected));
        assert_eq!(
            layout.slot_for_key(key).unwrap(),
            other.slot_for_key(key).unwrap()
        );
    }
    assert!(layout.slot_for_key(&vec![0; MAX_KEY_LEN]).is_ok());
    assert!(matches!(
        layout.slot_for_key(&vec![0; MAX_KEY_LEN + 1]),
        Err(SlotError::KeyTooLong { .. })
    ));
    assert_eq!(layout.group_for_slot(SlotId(16)).unwrap(), 116);
    assert_eq!(layout.slot_for_group(116).unwrap(), SlotId(16));
    assert!(layout.group_for_slot(SlotId(17)).is_err());
    assert!(layout.slot_for_group(99).is_err());
    assert!(layout.slot_for_group(117).is_err());
}

#[test]
fn malformed_layouts_are_rejected_before_routing() {
    let rejects = |mutate: fn(&mut LayoutConfig)| {
        let mut value = config(2);
        mutate(&mut value);
        assert!(ClusterLayout::new(value).is_err());
    };
    rejects(|c| c.cluster_id.clear());
    rejects(|c| c.format_version = 2);
    rejects(|c| c.slot_count = 0);
    rejects(|c| c.slot_count = MAX_SLOTS + 1);
    rejects(|c| c.group_id_base = u64::MAX);
    rejects(|c| c.nodes.clear());
    rejects(|c| c.nodes.push(c.nodes[0].clone()));
    rejects(|c| c.nodes = vec![c.nodes[0].clone(); MAX_NODES + 1]);
    rejects(|c| c.slots.pop().map(|_| ()).unwrap());
    rejects(|c| c.slots[0].slot = SlotId(0));
    rejects(|c| c.slots[0].slot = SlotId(2));
    rejects(|c| c.slots[0].voters.clear());
    rejects(|c| c.slots[0].voters = vec![1, 1]);
    rejects(|c| c.slots[0].voters = vec![99]);
    rejects(|c| c.slots[0].voters = vec![1; MAX_VOTERS + 1]);
    let mut edge = config(1);
    edge.group_id_base = u64::MAX;
    assert_eq!(
        ClusterLayout::new(edge)
            .unwrap()
            .group_for_slot(SlotId(0))
            .unwrap(),
        u64::MAX
    );
}

#[test]
fn restart_matches_identity_and_changed_layout_or_local_node_fails() {
    let dir = TestDirectory::new();
    let layout = ClusterLayout::new(config(2)).unwrap();
    let first = SlotManifest::open_or_create(&dir.0, &layout, 1).unwrap();
    let bytes = fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap();
    assert_eq!(
        first,
        SlotManifest::open_or_create(&dir.0, &layout, 1).unwrap()
    );
    assert_eq!(first.local_node_id(), 1);
    assert_eq!(first.layout_digest(), layout.digest());
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &layout, 2),
        Err(SlotError::ManifestMismatch(_))
    ));
    for mutation in [0, 1, 2, 3] {
        let mut changed = config(2);
        match mutation {
            0 => changed.cluster_id.push('x'),
            1 => changed.group_id_base += 1,
            2 => changed.nodes[0].application_rpc.set_port(9000),
            _ => changed.slots[0].voters = vec![1],
        }
        let changed = ClusterLayout::new(changed).unwrap();
        assert_ne!(changed.digest(), layout.digest());
        assert!(layout.verify_digest(changed.digest()).is_err());
        assert!(matches!(
            SlotManifest::open_or_create(&dir.0, &changed, 1),
            Err(SlotError::ManifestMismatch(_))
        ));
    }
    assert!(
        SlotManifest::open_or_create(&dir.0, &ClusterLayout::new(config(3)).unwrap(), 1).is_err()
    );
    assert_eq!(bytes, fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap());
}

#[test]
fn missing_or_corrupt_manifest_never_adopts_existing_data() {
    let layout = ClusterLayout::new(config(2)).unwrap();
    let dir = TestDirectory::new();
    fs::create_dir(dir.0.join("raft-log")).unwrap();
    fs::write(dir.0.join("raft-log/evidence"), b"old log").unwrap();
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &layout, 1),
        Err(SlotError::DataDirectoryNotEmpty)
    ));
    assert_eq!(
        fs::read(dir.0.join("raft-log/evidence")).unwrap(),
        b"old log"
    );
    assert!(!dir.0.join(MANIFEST_FILE_NAME).exists());
    let dir = TestDirectory::new();
    for bad in [b"{".as_slice(), b"null".as_slice(), b"".as_slice()] {
        fs::write(dir.0.join(MANIFEST_FILE_NAME), bad).unwrap();
        assert!(matches!(
            SlotManifest::open_or_create(&dir.0, &layout, 1),
            Err(SlotError::InvalidManifest(_))
        ));
        assert_eq!(fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap(), bad);
    }
}

#[test]
fn interrupted_creation_preserves_evidence_and_refuses_restart() {
    let dir = TestDirectory::new();
    let layout = ClusterLayout::new(config(2)).unwrap();
    fs::write(dir.0.join(".slot-manifest.creating"), b"evidence").unwrap();
    fs::write(dir.0.join(MANIFEST_FILE_NAME), b"partial").unwrap();
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &layout, 1),
        Err(SlotError::CreationInProgress)
    ));
    assert_eq!(
        fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap(),
        b"partial"
    );
}

#[test]
fn racing_initializers_cannot_overwrite_a_different_local_identity() {
    let dir = TestDirectory::new();
    let layout = ClusterLayout::new(config(2)).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = [1, 2]
            .into_iter()
            .map(|node| {
                let barrier = Arc::clone(&barrier);
                let path = &dir.0;
                let layout = &layout;
                scope.spawn(move || {
                    barrier.wait();
                    SlotManifest::open_or_create(path, layout, node)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        SlotManifest::open_or_create(&dir.0, &layout, winner.local_node_id()).unwrap(),
        winner
    );
    let bytes = fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap();
    assert!(SlotManifest::open_or_create(&dir.0, &layout, 3 - winner.local_node_id()).is_err());
    assert_eq!(fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap(), bytes);
}

#[test]
fn unknown_persisted_fields_and_digest_tampering_fail_closed() {
    let dir = TestDirectory::new();
    let layout = ClusterLayout::new(config(2)).unwrap();
    SlotManifest::open_or_create(&dir.0, &layout, 1).unwrap();
    let path = dir.0.join(MANIFEST_FILE_NAME);
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    value["layout_digest"][0] = serde_json::json!(255);
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &layout, 1),
        Err(SlotError::InvalidManifest(_))
    ));
    value["unexpected"] = serde_json::json!(true);
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &layout, 1),
        Err(SlotError::InvalidManifest(_))
    ));
}

#[test]
fn ipv6_flowinfo_is_rejected_before_lossy_json_or_manifest_creation() {
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    for application_address in [false, true] {
        let mut input = config(2);
        let address = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 2001, 7, 0));
        if application_address {
            input.nodes[0].application_rpc = address;
        } else {
            input.nodes[0].raft = address;
        }
        // SocketAddr serde uses text and therefore discards flowinfo. Reject the
        // typed input before that lossy representation can become an identity.
        let json = serde_json::to_vec(&input).unwrap();
        let decoded: LayoutConfig = serde_json::from_slice(&json).unwrap();
        assert_ne!(input, decoded);
        assert!(matches!(
            ClusterLayout::new(input),
            Err(SlotError::Config(_))
        ));
    }
}

#[test]
fn ipv6_scope_ids_roundtrip_json_and_persisted_identity() {
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    let dir = TestDirectory::new();
    let mut input = config(2);
    input.nodes[0].raft = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 2002, 0, 17));
    input.nodes[0].application_rpc =
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 3002, 0, 23));
    let json = serde_json::to_vec(&input).unwrap();
    let decoded: LayoutConfig = serde_json::from_slice(&json).unwrap();
    assert_eq!(input, decoded);
    let layout = ClusterLayout::new(input).unwrap();
    let decoded_layout = ClusterLayout::new(decoded).unwrap();
    assert_eq!(layout.digest(), decoded_layout.digest());
    let created = SlotManifest::open_or_create(&dir.0, &layout, 2).unwrap();
    let restarted = SlotManifest::open_or_create(&dir.0, &layout, 2).unwrap();
    assert_eq!(created, restarted);
    assert_eq!(restarted.layout(), &decoded_layout);
}
