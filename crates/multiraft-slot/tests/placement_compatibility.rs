//! Frozen canonical byte hashes captured from initial allocator commit 48cf464.
use multiraft_slot::{plan_layout, PlacementInput, PlacementNode};
use sha2::{Digest, Sha256};

#[test]
fn initial_weighted_and_equal_layouts_preserve_canonical_bytes() {
    let mut input = PlacementInput {
        cluster_id: "compatibility".into(),
        slot_count: 12,
        group_id_base: 100,
        nodes: (1..=4)
            .map(|node_id| PlacementNode {
                node_id,
                raft: format!("127.0.0.1:{}", node_id * 2).parse().unwrap(),
                application_rpc: format!("127.0.0.1:{}", node_id * 2 + 1).parse().unwrap(),
                weight: 1,
            })
            .collect(),
    };
    for weighted in [false, true] {
        if weighted {
            for node in &mut input.nodes {
                node.weight = node.node_id as u32;
            }
        }
        let layout = plan_layout(&input).unwrap();
        let digest = Sha256::digest(layout.to_canonical_json());
        let hash: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        let expected = if weighted {
            "8dcc1aaa9cc376070575450277f026d24d7ab28b97a2a462d1b1a0fc0a2cc387"
        } else {
            "ac72931ede2bbe0d75043513a30c773ca4bbb151e246348de0e8c45c411a606f"
        };
        assert_eq!(hash, expected, "weighted={weighted}");
    }
}
