//! Pure incremental placement contract, independent of native runtime state.
use multiraft_slot::placement::{
    plan_incremental, CurrentPlacement, PlacementInput, PlacementNode,
};
use multiraft_slot::{NodeId, SlotId};

fn input(nodes: u64, slots: u32) -> PlacementInput {
    PlacementInput {
        cluster_id: "incremental".into(),
        slot_count: slots,
        group_id_base: 100,
        nodes: (1..=nodes)
            .map(|node_id| PlacementNode {
                node_id,
                raft: format!("127.0.0.1:{}", node_id * 2).parse().unwrap(),
                application_rpc: format!("127.0.0.1:{}", node_id * 2 + 1).parse().unwrap(),
                weight: 1,
            })
            .collect(),
    }
}

fn current(slots: u32, voters: &[NodeId]) -> Vec<CurrentPlacement> {
    (0..slots)
        .map(|slot| CurrentPlacement {
            slot: SlotId(slot),
            voters: voters.to_vec(),
        })
        .collect()
}

#[test]
fn three_to_four_keeps_all_but_nine_replicas() {
    let result = plan_incremental(&input(4, 12), &current(12, &[1, 2, 3])).unwrap();
    assert_eq!(result.replacements.len(), 9);
    let mut counts = [0; 4];
    let mut outgoing = [0; 3];
    for target in result.targets {
        assert_eq!(target.voters.len(), 3);
        assert!(target.voters.contains(&target.preferred_leader));
        for voter in target.voters {
            counts[voter as usize - 1] += 1;
        }
    }
    for replacement in result.replacements {
        outgoing[replacement.source as usize - 1] += 1;
        assert_eq!(replacement.target, 4);
    }
    assert_eq!(counts, [9; 4]);
    assert_eq!(outgoing, [3; 3]);
}

fn check_plan(input: &PlacementInput, before: &[CurrentPlacement]) -> usize {
    let result = plan_incremental(input, before).unwrap();
    let mut counts = vec![0; input.nodes.len()];
    let mut actual = before.to_vec();
    actual.sort_by_key(|slot| slot.slot);
    for slot in &mut actual {
        slot.voters.sort_unstable();
    }
    let mut last = None;
    for step in &result.replacements {
        let key = (step.slot, step.source, step.target);
        assert!(last.is_none_or(|previous| previous < key));
        last = Some(key);
        let slot = &mut actual[step.slot.0 as usize];
        assert_eq!(step.expected_voters, slot.voters);
        assert!(slot.voters.contains(&step.source));
        assert!(!slot.voters.contains(&step.target));
        slot.voters.retain(|&node| node != step.source);
        slot.voters.push(step.target);
        slot.voters.sort_unstable();
        assert_eq!(step.result_voters, slot.voters);
    }
    assert_eq!(result.targets.len(), input.slot_count as usize);
    for (ordinal, target) in result.targets.iter().enumerate() {
        assert_eq!(target.slot, SlotId(ordinal as u32));
        assert_eq!(actual[ordinal].voters, target.voters);
        assert_eq!(target.voters.len(), 3);
        assert!(target.voters.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(target.voters.contains(&target.preferred_leader));
        for node in &target.voters {
            let index = input
                .nodes
                .iter()
                .position(|candidate| candidate.node_id == *node)
                .unwrap();
            counts[index] += 1;
        }
    }
    let total = input.slot_count as usize * 3;
    let floor = total / input.nodes.len();
    let ceil = total.div_ceil(input.nodes.len());
    assert!(
        counts.iter().all(|&count| count == floor || count == ceil),
        "counts: {counts:?}"
    );
    result.replacements.len()
}

#[test]
fn balanced_four_to_six_needs_twelve_replacements() {
    let initial = multiraft_slot::plan_layout(&input(4, 12)).unwrap();
    let before: Vec<_> = initial
        .config()
        .slots
        .iter()
        .map(|slot| CurrentPlacement {
            slot: slot.slot,
            voters: slot.voters.clone(),
        })
        .collect();
    assert_eq!(check_plan(&input(6, 12), &before), 12);
}

#[test]
fn three_to_nine_chains_multiple_replacements_in_a_slot() {
    let before = current(12, &[1, 2, 3]);
    let result = plan_incremental(&input(9, 12), &before).unwrap();
    assert_eq!(check_plan(&input(9, 12), &before), 24);
    assert!(result
        .replacements
        .windows(2)
        .any(|pair| pair[0].slot == pair[1].slot));
}

#[test]
fn balanced_membership_is_retained_even_when_genesis_would_choose_differently() {
    let before = vec![
        CurrentPlacement {
            slot: SlotId(0),
            voters: vec![2, 3, 4],
        },
        CurrentPlacement {
            slot: SlotId(1),
            voters: vec![1, 3, 4],
        },
        CurrentPlacement {
            slot: SlotId(2),
            voters: vec![1, 2, 4],
        },
        CurrentPlacement {
            slot: SlotId(3),
            voters: vec![1, 2, 3],
        },
    ];
    assert_eq!(check_plan(&input(4, 4), &before), 0);
}

#[test]
fn remainder_nodes_are_chosen_for_retention_instead_of_fixed_low_ids() {
    let before = current(2, &[3, 4, 5]);
    // Six replicas over five nodes means only one node gets two. Giving that
    // extra to node 1 forces three moves; retaining it on 3/4/5 needs only two.
    assert_eq!(check_plan(&input(5, 2), &before), 2);
    assert_eq!(check_plan(&input(4, 1), &current(1, &[2, 3, 4])), 0);
}

#[test]
fn uneven_current_distribution_keeps_every_replica_allowed_by_balance() {
    assert_eq!(check_plan(&input(4, 5), &current(5, &[1, 2, 3])), 3);
}

#[test]
fn node_slot_and_voter_permutations_produce_identical_bytes() {
    let mut input = input(9, 12);
    // Noncontiguous identities and the largest identity exercise stable ties.
    for (index, node) in input.nodes.iter_mut().enumerate() {
        node.node_id = if index == 8 {
            u64::MAX
        } else {
            index as u64 * 11
        };
    }
    let mut before = current(12, &[0, 11, 22]);
    let expected = serde_json::to_vec(&plan_incremental(&input, &before).unwrap()).unwrap();
    for _ in 0..input.nodes.len() {
        input.nodes.rotate_left(1);
        before.rotate_left(1);
        for slot in &mut before {
            slot.voters.rotate_left(1);
        }
        assert_eq!(
            serde_json::to_vec(&plan_incremental(&input, &before).unwrap()).unwrap(),
            expected
        );
    }
}

fn reject(input: &PlacementInput, before: &[CurrentPlacement], needle: &str) {
    match plan_incremental(input, before) {
        Err(multiraft_slot::SlotError::Config(message)) => {
            assert!(message.contains(needle), "{message}")
        }
        other => panic!("expected {needle}: {other:?}"),
    }
}

#[test]
fn malformed_memberships_and_slots_are_rejected() {
    let input = input(4, 2);
    reject(&input, &[], "exactly one");
    let mut before = current(2, &[1, 2, 3]);
    before[1].slot = SlotId(0);
    reject(&input, &before, "slot");
    before[1].slot = SlotId(2);
    reject(&input, &before, "slot");
    before[1].slot = SlotId(u32::MAX);
    reject(&input, &before, "slot");
    for voters in [
        vec![],
        vec![1, 2],
        vec![1, 2, 3, 4],
        vec![1, 1, 2],
        vec![1, 2, 9],
    ] {
        let mut before = current(2, &[1, 2, 3]);
        before[0].voters = voters;
        reject(
            &input,
            &before,
            if before[0].voters.len() == 3 {
                if before[0].voters[1] == 1 {
                    "duplicate"
                } else {
                    "unknown"
                }
            } else {
                "3 voters"
            },
        );
    }
}

#[test]
fn malformed_candidates_weights_and_metadata_are_rejected() {
    let before = current(2, &[1, 2, 3]);
    reject(&input(2, 2), &before, "at least 3");
    reject(&input(1025, 2), &before, "1024");
    for slot_count in [0, 257, u32::MAX] {
        reject(&input(4, slot_count), &before, "slot_count");
    }
    let mut invalid = input(4, 2);
    invalid.nodes[3].node_id = 3;
    reject(&invalid, &before, "duplicate node id");
    invalid = input(4, 2);
    invalid.nodes[3].raft = invalid.nodes[0].application_rpc;
    reject(&invalid, &before, "duplicate node address");
    invalid = input(4, 2);
    invalid.nodes[3].weight = 0;
    reject(&invalid, &before, "weight");
    invalid.nodes[3].weight = 2;
    reject(&invalid, &before, "equal node weights");
    invalid = input(4, 2);
    invalid.cluster_id.clear();
    reject(&invalid, &before, "cluster_id");
    invalid.cluster_id = "a".repeat(257);
    reject(&invalid, &before, "cluster_id");
    invalid = input(4, 2);
    invalid.group_id_base = u64::MAX;
    reject(&invalid, &before, "overflows");
    invalid = input(4, 2);
    invalid.nodes[3].raft = std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
        std::net::Ipv6Addr::LOCALHOST,
        100,
        1,
        0,
    ));
    reject(&invalid, &before, "flowinfo");
}

#[test]
fn equal_nonunit_weights_and_input_defense_limits_are_supported() {
    let mut equal = input(4, 2);
    for node in &mut equal.nodes {
        node.weight = u32::MAX;
    }
    assert_eq!(check_plan(&equal, &current(2, &[1, 2, 3])), 1);
    assert_eq!(check_plan(&input(1024, 1), &current(1, &[1, 2, 3])), 0);
    assert_eq!(check_plan(&input(3, 256), &current(256, &[1, 2, 3])), 0);
}

fn triples(nodes: u64) -> Vec<Vec<NodeId>> {
    let mut result = Vec::new();
    for a in 1..=nodes {
        for b in a + 1..=nodes {
            for c in b + 1..=nodes {
                result.push(vec![a, b, c]);
            }
        }
    }
    result
}

// Exhaustively list target voter sets and directly count edits. This oracle has
// no flow graph, capacities, residual edges, shortest paths or remainder policy.
fn oracle(before: &[CurrentPlacement], options: &[Vec<NodeId>], nodes: usize) -> usize {
    fn visit(
        before: &[CurrentPlacement],
        options: &[Vec<NodeId>],
        counts: &mut [usize],
        slot: usize,
        cost: usize,
        best: &mut usize,
    ) {
        if slot == before.len() {
            let total = before.len() * 3;
            if counts.iter().all(|&count| {
                count == total / counts.len() || count == total.div_ceil(counts.len())
            }) {
                *best = (*best).min(cost);
            }
            return;
        }
        for voters in options {
            for &node in voters {
                counts[node as usize - 1] += 1;
            }
            let edits = voters
                .iter()
                .filter(|node| !before[slot].voters.contains(node))
                .count();
            visit(before, options, counts, slot + 1, cost + edits, best);
            for &node in voters {
                counts[node as usize - 1] -= 1;
            }
        }
    }
    let mut best = usize::MAX;
    visit(before, options, &mut vec![0; nodes], 0, 0, &mut best);
    best
}

#[test]
fn small_exhaustive_oracle_proves_minimum_cost_including_remainder_choices() {
    for (nodes, slots) in [(3, 2), (4, 2), (5, 2), (6, 2), (4, 3), (5, 3)] {
        let options = triples(nodes);
        let combinations = options.len().pow(slots);
        for mut code in 0..combinations {
            let before: Vec<_> = (0..slots)
                .map(|slot| {
                    let voters = options[code % options.len()].clone();
                    code /= options.len();
                    CurrentPlacement {
                        slot: SlotId(slot),
                        voters,
                    }
                })
                .collect();
            let expected = oracle(&before, &options, nodes as usize);
            assert_eq!(
                check_plan(&input(nodes, slots), &before),
                expected,
                "nodes {nodes}, before {before:?}"
            );
        }
    }
}
