//! Two-level routing, layout v2 validation, digests, placement, and manifest v2.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use multiraft_slot::{
    contiguous_bucket_table, plan_layout, ClusterLayout, HashBucketId, KeyRouter, LayoutConfig,
    PlacementInput, PlacementNode, SlotError, SlotId, SlotManifest, BUCKET_COUNT,
    MANIFEST_FILE_NAME, MAX_KEY_LEN,
};

fn node(id: u64) -> PlacementNode {
    PlacementNode {
        node_id: id,
        raft: format!("127.0.0.1:{}", 7100 + id).parse().unwrap(),
        application_rpc: format!("127.0.0.1:{}", 7200 + id).parse().unwrap(),
        weight: 1,
    }
}

fn input(ids: &[u64]) -> PlacementInput {
    PlacementInput {
        cluster_id: "layout-test".into(),
        slot_count: 12,
        group_id_base: 100,
        nodes: ids.iter().copied().map(node).collect(),
    }
}

fn twelve() -> ClusterLayout {
    plan_layout(&input(&[1, 2, 3])).unwrap()
}

fn rejected(mutate: impl FnOnce(&mut LayoutConfig)) -> String {
    let mut config = twelve().config().clone();
    mutate(&mut config);
    match ClusterLayout::new(config) {
        Err(SlotError::Config(message)) => message,
        other => panic!("expected config rejection, got {other:?}"),
    }
}

struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "slot-layout-{}-{}",
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
fn twelve_slot_table_matches_contract_ranges_and_covers_each_bucket_once() {
    let table = contiguous_bucket_table(12).unwrap();
    assert_eq!(table.len(), 256);
    let expected: [(u32, std::ops::RangeInclusive<usize>); 12] = [
        (0, 0..=21),
        (1, 22..=43),
        (2, 44..=65),
        (3, 66..=87),
        (4, 88..=108),
        (5, 109..=129),
        (6, 130..=150),
        (7, 151..=171),
        (8, 172..=192),
        (9, 193..=213),
        (10, 214..=234),
        (11, 235..=255),
    ];
    let mut covered = 0;
    for (slot, range) in expected {
        let width = range.end() - range.start() + 1;
        assert_eq!(width, if slot < 4 { 22 } else { 21 });
        for bucket in range {
            assert_eq!(table[bucket], slot, "bucket {bucket}");
            covered += 1;
        }
    }
    assert_eq!(covered, 256);
    assert_eq!(twelve().config().buckets, table);
    assert!(contiguous_bucket_table(0).is_err());
    assert!(contiguous_bucket_table(257).is_err());
    assert_eq!(
        contiguous_bucket_table(256).unwrap(),
        (0..256).collect::<Vec<_>>()
    );
    assert_eq!(contiguous_bucket_table(1).unwrap(), vec![0; 256]);
}

/// Buckets were computed independently with Python `hashlib`; slots follow
/// the contract's 12-slot ranges.
#[test]
fn frozen_key_vectors_route_to_bucket_and_slot() {
    let layout = twelve();
    let router = layout.router();
    let vectors: [(Vec<u8>, u16, u32); 8] = [
        (b"".to_vec(), 164, 7),
        (b"channel-a".to_vec(), 79, 3),
        (b"channel-b".to_vec(), 182, 8),
        (b"\x00\xff\x80".to_vec(), 248, 11),
        (b"key-0".to_vec(), 250, 11),
        (b"\x03user-1".to_vec(), 112, 5),
        (vec![0; MAX_KEY_LEN], 167, 7),
        (vec![0xff; MAX_KEY_LEN], 189, 8),
    ];
    for (key, bucket, slot) in vectors {
        let found = router.bucket_for_key(&key).unwrap();
        assert_eq!(found.get(), bucket);
        assert_eq!(router.slot_for_bucket(found), SlotId(slot));
        assert_eq!(router.slot_for_key(&key).unwrap(), SlotId(slot));
        assert_eq!(layout.slot_for_key(&key).unwrap(), SlotId(slot));
        assert_eq!(router.group_for_key(&key).unwrap(), 100 + u64::from(slot));
    }
    for long in [MAX_KEY_LEN + 1, MAX_KEY_LEN + 100] {
        let key = vec![0; long];
        assert!(matches!(
            router.bucket_for_key(&key),
            Err(SlotError::KeyTooLong { actual, max }) if actual == long && max == MAX_KEY_LEN
        ));
        assert!(router.slot_for_key(&key).is_err());
        assert!(router.group_for_key(&key).is_err());
        assert!(layout.slot_for_key(&key).is_err());
    }
}

#[test]
fn bucket_ids_and_router_construction_are_checked() {
    assert_eq!(HashBucketId::new(255).unwrap().get(), 255);
    assert!(matches!(
        HashBucketId::new(256),
        Err(SlotError::UnknownBucket(256))
    ));
    let table = contiguous_bucket_table(12).unwrap();
    let router = KeyRouter::new(&table, 12, 100).unwrap();
    let copy = router;
    assert_eq!(copy, router);
    assert_eq!(router.buckets().collect::<Vec<_>>(), table);
    assert_eq!(router.slot_count(), 12);
    assert_eq!(router.group_id_base(), 100);
    assert!(KeyRouter::new(&table[..255], 12, 100).is_err());
    let mut long = table.clone();
    long.push(0);
    assert!(KeyRouter::new(&long, 12, 100).is_err());
    let mut out_of_range = table.clone();
    out_of_range[0] = 12;
    assert!(KeyRouter::new(&out_of_range, 12, 100).is_err());
    let mut empty_slot = table.clone();
    empty_slot[235..].fill(10);
    assert!(KeyRouter::new(&empty_slot, 12, 100).is_err());
    assert!(KeyRouter::new(&table, 0, 100).is_err());
    assert!(KeyRouter::new(&vec![0; 256], 257, 100).is_err());
    assert!(KeyRouter::new(&table, 12, u64::MAX - 10).is_err());
    assert!(KeyRouter::new(&table, 12, u64::MAX - 11).is_ok());
}

#[test]
fn invalid_v2_layouts_are_rejected() {
    assert!(rejected(|c| c.format_version = 1).contains("format_version 1"));
    assert!(rejected(|c| c.bucket_count = 255).contains("bucket_count"));
    assert!(rejected(|c| c.bucket_count = 512).contains("bucket_count"));
    assert!(rejected(|c| {
        c.buckets.pop();
    })
    .contains("256 entries"));
    assert!(rejected(|c| c.buckets.push(0)).contains("256 entries"));
    assert!(rejected(|c| c.buckets[17] = 12).contains("slot >= slot_count"));
    assert!(rejected(|c| c.buckets[235..].fill(0)).contains("at least one bucket"));
    assert!(rejected(|c| c.slot_count = 257).contains("1..=256"));
    assert!(rejected(|c| c.slots[3].preferred_initializer = 9).contains("preferred_initializer"));
    assert!(rejected(|c| c.slots[3].soft_preferred_leader = 9).contains("soft_preferred_leader"));
    assert!(rejected(|c| {
        c.slots[3].voters = vec![1, 2];
        c.slots[3].preferred_initializer = 1;
        c.slots[3].soft_preferred_leader = 3;
    })
    .contains("soft_preferred_leader"));
    assert!(rejected(|c| c.nodes[1].node_id = 1).contains("duplicate node id"));
    assert!(rejected(|c| c.nodes[1].raft = c.nodes[0].raft).contains("duplicate node address"));
    assert!(rejected(|c| c.nodes[2].application_rpc = c.nodes[0].raft)
        .contains("duplicate node address"));
}

#[test]
fn v1_and_unknown_field_json_is_rejected_with_clear_errors() {
    let v1 = br#"{"cluster_id":"old","format_version":1,"hash_version":"Sha256V1","slot_count":1,
        "group_id_base":100,"nodes":[{"node_id":1,"raft":"127.0.0.1:1","application_rpc":"127.0.0.1:2"}],
        "slots":[{"slot":0,"voters":[1]}]}"#;
    match LayoutConfig::from_json(v1) {
        Err(SlotError::Config(message)) => {
            assert!(message.contains("format_version 1"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    let layout = twelve();
    let json = layout.to_canonical_json();
    let decoded = LayoutConfig::from_json(&json).unwrap();
    assert_eq!(ClusterLayout::new(decoded).unwrap(), layout);
    let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    value["hash_version"] = serde_json::json!("Sha256V1");
    assert!(LayoutConfig::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    value["slots"][0]["extra"] = serde_json::json!(1);
    assert!(LayoutConfig::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(LayoutConfig::from_json(b"{}").is_err());
}

#[test]
fn route_digest_ignores_addresses_and_preferences_layout_digest_does_not() {
    let layout = twelve();
    let route = layout.route_digest();
    let digest = layout.layout_digest();
    assert_eq!(layout.digest(), digest);
    // Frozen; independently reproduced with Python hashlib over the artifact table.
    assert_eq!(
        route.to_hex(),
        "ffe93697dd89b42e91e6e7a1ba0e3f0abb273d09ad9c9ae68d5459c3eca80236"
    );
    // Route digest is the same for every 12-slot/base-100 layout.
    let mut other = input(&[4, 5, 6, 7]);
    other.cluster_id = "other".into();
    let other = plan_layout(&other).unwrap();
    assert_eq!(other.route_digest(), route);
    assert_ne!(other.layout_digest(), digest);

    let mut preference = layout.config().clone();
    preference.slots[0].soft_preferred_leader = preference.slots[0].voters[0];
    preference.slots[0].preferred_initializer = preference.slots[0].voters[0];
    assert_ne!(
        preference.slots[0].soft_preferred_leader,
        layout.config().slots[0].soft_preferred_leader
    );
    let preference = ClusterLayout::new(preference).unwrap();
    assert_eq!(preference.route_digest(), route);
    assert_ne!(preference.layout_digest(), digest);
    assert!(layout.verify_digest(preference.layout_digest()).is_err());
    assert!(layout
        .verify_route_digest(preference.route_digest())
        .is_ok());

    // The two preferences are hashed independently.
    let mut only_initializer = layout.config().clone();
    only_initializer.slots[0].preferred_initializer = only_initializer.slots[0].voters[0];
    let mut only_leader = layout.config().clone();
    only_leader.slots[0].soft_preferred_leader = only_leader.slots[0].voters[0];
    let a = ClusterLayout::new(only_initializer)
        .unwrap()
        .layout_digest();
    let b = ClusterLayout::new(only_leader).unwrap().layout_digest();
    assert_ne!(a, b);
    assert_ne!(a, digest);

    let mut table = layout.config().clone();
    table.buckets.swap(21, 22);
    let table = ClusterLayout::new(table).unwrap();
    assert_ne!(table.route_digest(), route);
    assert_ne!(table.layout_digest(), digest);
    assert!(layout.verify_route_digest(table.route_digest()).is_err());

    let mut base = input(&[1, 2, 3]);
    base.group_id_base = 101;
    assert_ne!(plan_layout(&base).unwrap().route_digest(), route);
}

#[test]
fn placement_is_balanced_and_independent_of_node_order() {
    let layout = twelve();
    let config = layout.config();
    assert_eq!(config.slot_count, 12);
    assert_eq!(config.bucket_count, BUCKET_COUNT);
    let mut replicas = [0; 4];
    let mut preferred = [0; 4];
    for slot in &config.slots {
        assert_eq!(slot.voters, vec![1, 2, 3]);
        assert_eq!(slot.preferred_initializer, slot.soft_preferred_leader);
        for voter in &slot.voters {
            replicas[*voter as usize] += 1;
        }
        preferred[slot.soft_preferred_leader as usize] += 1;
    }
    assert_eq!(&replicas[1..], &[12, 12, 12]);
    assert_eq!(&preferred[1..], &[4, 4, 4]);
    let leaders: Vec<u64> = config
        .slots
        .iter()
        .map(|s| s.soft_preferred_leader)
        .collect();
    assert_eq!(leaders, [2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1, 3]);
    let canonical = layout.to_canonical_json();
    for order in [[1, 3, 2], [2, 1, 3], [2, 3, 1], [3, 1, 2], [3, 2, 1]] {
        let permuted = plan_layout(&input(&order)).unwrap();
        assert_eq!(permuted, layout);
        assert_eq!(permuted.route_digest(), layout.route_digest());
        assert_eq!(permuted.layout_digest(), layout.layout_digest());
        assert_eq!(permuted.to_canonical_json(), canonical);
    }
    // Ids permuted relative to addresses produce a different (but still
    // order-independent) layout.
    let mut swapped = input(&[1, 2, 3]);
    swapped.nodes[0].node_id = 3;
    swapped.nodes[2].node_id = 1;
    let swapped_layout = plan_layout(&swapped).unwrap();
    assert_ne!(swapped_layout.layout_digest(), layout.layout_digest());
    swapped.nodes.reverse();
    assert_eq!(plan_layout(&swapped).unwrap(), swapped_layout);
}

#[test]
fn placement_spreads_replicas_over_more_nodes_and_honors_weights() {
    let six = plan_layout(&input(&[1, 2, 3, 4, 5, 6])).unwrap();
    let mut replicas = [0; 7];
    let mut preferred = [0; 7];
    for slot in &six.config().slots {
        assert_eq!(slot.voters.len(), 3);
        assert!(slot.voters.contains(&slot.soft_preferred_leader));
        for voter in &slot.voters {
            replicas[*voter as usize] += 1;
        }
        preferred[slot.soft_preferred_leader as usize] += 1;
    }
    assert_eq!(&replicas[1..], &[6; 6]);
    assert_eq!(&preferred[1..], &[2; 6]);

    let mut weighted = input(&[1, 2, 3, 4]);
    weighted.nodes[3].weight = 3;
    let weighted = plan_layout(&weighted).unwrap();
    let heavy = weighted
        .config()
        .slots
        .iter()
        .filter(|slot| slot.voters.contains(&4))
        .count();
    assert_eq!(heavy, 12);
}

#[test]
fn placement_rejects_invalid_node_sets() {
    let reject = |input: PlacementInput, needle: &str| match plan_layout(&input) {
        Err(SlotError::Config(message)) => assert!(message.contains(needle), "{message}"),
        other => panic!("{other:?}"),
    };
    reject(input(&[1, 2]), "at least 3");
    reject(input(&[]), "at least 3");
    reject(input(&[1, 2, 2]), "duplicate node id");
    let mut address = input(&[1, 2, 3]);
    address.nodes[2].raft = address.nodes[0].raft;
    reject(address, "duplicate node address");
    let mut cross = input(&[1, 2, 3]);
    cross.nodes[2].application_rpc = cross.nodes[1].raft;
    reject(cross, "duplicate node address");
    let mut zero = input(&[1, 2, 3]);
    zero.nodes[1].weight = 0;
    reject(zero, "weight");
    let mut slots = input(&[1, 2, 3]);
    slots.slot_count = 0;
    reject(slots.clone(), "slot_count");
    slots.slot_count = 257;
    reject(slots, "slot_count");
    let parsed: PlacementInput = serde_json::from_str(
        r#"{"cluster_id":"c","slot_count":12,"group_id_base":100,"nodes":[
            {"node_id":1,"raft":"127.0.0.1:1","application_rpc":"127.0.0.1:2"}]}"#,
    )
    .unwrap();
    assert_eq!(parsed.nodes[0].weight, 1);
    assert!(serde_json::from_str::<PlacementInput>(
        r#"{"cluster_id":"c","slot_count":12,"group_id_base":100,"nodes":[],"rf":3}"#
    )
    .is_err());
}

#[test]
fn manifest_v2_roundtrip_and_changes_are_rejected() {
    let dir = TestDirectory::new();
    let layout = twelve();
    let created = SlotManifest::open_or_create(&dir.0, &layout, 2).unwrap();
    assert_eq!(created.layout_digest(), layout.layout_digest());
    assert_eq!(created.route_digest(), layout.route_digest());
    let bytes = fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap();
    let stored: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(stored["layout"]["format_version"], 2);
    assert!(stored["route_digest"].is_array());
    assert_eq!(
        SlotManifest::open_or_create(&dir.0, &layout, 2).unwrap(),
        created
    );
    let mut preference = layout.config().clone();
    preference.slots[5].soft_preferred_leader = preference.slots[5].voters[0];
    let preference = ClusterLayout::new(preference).unwrap();
    assert_eq!(preference.route_digest(), layout.route_digest());
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &preference, 2),
        Err(SlotError::ManifestMismatch(_))
    ));
    assert_eq!(fs::read(dir.0.join(MANIFEST_FILE_NAME)).unwrap(), bytes);

    // Tampering with the stored route digest fails closed.
    let mut value = stored.clone();
    value["route_digest"][0] =
        serde_json::json!((stored["route_digest"][0].as_u64().unwrap() + 1) % 256);
    fs::write(
        dir.0.join(MANIFEST_FILE_NAME),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        SlotManifest::open_or_create(&dir.0, &layout, 2),
        Err(SlotError::InvalidManifest(message)) if message.contains("route digest")
    ));
}

#[test]
fn v1_manifest_is_rejected_and_left_byte_for_byte_unchanged() {
    let dir = TestDirectory::new();
    let v1 = br#"{"layout":{"cluster_id":"layout-test","format_version":1,"hash_version":"Sha256V1","slot_count":1,"group_id_base":100,"nodes":[{"node_id":1,"raft":"127.0.0.1:7101","application_rpc":"127.0.0.1:7201"}],"slots":[{"slot":0,"voters":[1]}]},"layout_digest":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"local_node_id":1}"#;
    let path = dir.0.join(MANIFEST_FILE_NAME);
    fs::write(&path, v1).unwrap();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    match SlotManifest::open_or_create(&dir.0, &twelve(), 1) {
        Err(SlotError::InvalidManifest(message)) => {
            assert!(message.contains("format_version 1"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(fs::read(&path).unwrap(), v1);
    assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
    assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 1);
}
