//! The committed canonical 3-node / 12-slot demo artifact is exactly what
//! placement generates, and its digests are frozen.
use multiraft_slot::{plan_layout, ClusterLayout, LayoutConfig, PlacementInput};

const INPUT: &[u8] = include_bytes!("../layouts/rf3-12slot.input.json");
const ARTIFACT: &[u8] = include_bytes!("../layouts/rf3-12slot.json");
const ROUTE_DIGEST: &str = "ffe93697dd89b42e91e6e7a1ba0e3f0abb273d09ad9c9ae68d5459c3eca80236";
const LAYOUT_DIGEST: &str = "ae26714e93f0bc09438390ff7de7f881787b15598bf65899c227819bd80c7990";

#[test]
fn committed_artifact_matches_regenerated_placement_and_frozen_digests() {
    let input: PlacementInput = serde_json::from_slice(INPUT).unwrap();
    let generated = plan_layout(&input).unwrap();
    assert_eq!(
        String::from_utf8(generated.to_canonical_json()).unwrap(),
        String::from_utf8(ARTIFACT.to_vec()).unwrap()
    );
    let loaded = ClusterLayout::new(LayoutConfig::from_json(ARTIFACT).unwrap()).unwrap();
    assert_eq!(loaded, generated);
    assert_eq!(loaded.route_digest().to_hex(), ROUTE_DIGEST);
    assert_eq!(loaded.layout_digest().to_hex(), LAYOUT_DIGEST);
}
