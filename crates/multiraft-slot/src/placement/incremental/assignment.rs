//! Capacitated min-cost flow; replica retention precedes every tie decision.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::super::{PlacementNode, REPLICATION_FACTOR};
use crate::{SlotAssignment, SlotError};

#[derive(Debug)]
struct Edge {
    to: usize,
    reverse: usize,
    capacity: usize,
    cost: i64,
}

fn add_edge(graph: &mut [Vec<Edge>], from: usize, to: usize, capacity: usize, cost: i64) {
    let reverse = graph[to].len();
    let forward = graph[from].len();
    graph[from].push(Edge {
        to,
        reverse,
        capacity,
        cost,
    });
    graph[to].push(Edge {
        to: from,
        reverse: forward,
        capacity: 0,
        cost: -cost,
    });
}

pub(super) fn minimum_replacements(
    nodes: &[PlacementNode],
    slots: &[SlotAssignment],
) -> Result<Vec<Vec<usize>>, SlotError> {
    let source = 0;
    let first_slot = 1;
    let first_node = first_slot + slots.len();
    let sink = first_node + nodes.len();
    let mut graph: Vec<Vec<Edge>> = (0..=sink).map(|_| Vec::new()).collect();
    let total = slots.len() * REPLICATION_FACTOR;
    let floor = total / nodes.len();
    let remainder = total % nodes.len();
    for (slot_index, slot) in slots.iter().enumerate() {
        let vertex = first_slot + slot_index;
        // Insert Slot->node edges before the reverse source edge, allowing
        // extraction by canonical node index after the final residual flow.
        for (node_index, node) in nodes.iter().enumerate() {
            let cost = i64::from(!slot.voters.contains(&node.node_id));
            add_edge(&mut graph, vertex, first_node + node_index, 1, cost);
        }
        add_edge(&mut graph, source, vertex, REPLICATION_FACTOR, 0);
    }
    for node_index in 0..nodes.len() {
        add_edge(&mut graph, first_node + node_index, sink, floor, 0);
        if remainder != 0 {
            // This penalty exceeds the entire possible replacement cost. Hence
            // all floor capacities fill before selecting which nodes get ceil.
            // Exactly `remainder` extra edges then have constant total penalty;
            // the remaining objective is precisely the number of new replicas.
            add_edge(
                &mut graph,
                first_node + node_index,
                sink,
                1,
                total as i64 + 1,
            );
        }
    }
    send_flow(&mut graph, source, sink, total)?;
    Ok((0..slots.len())
        .map(|slot| {
            graph[first_slot + slot][..nodes.len()]
                .iter()
                .enumerate()
                .filter_map(|(node, edge)| (edge.capacity == 0).then_some(node))
                .collect()
        })
        .collect())
}

fn send_flow(
    graph: &mut [Vec<Edge>],
    source: usize,
    sink: usize,
    total: usize,
) -> Result<(), SlotError> {
    let mut potential = vec![0_i64; graph.len()];
    for _ in 0..total {
        let mut distance = vec![i64::MAX; graph.len()];
        let mut previous = vec![None; graph.len()];
        let mut queue = BinaryHeap::new();
        distance[source] = 0;
        queue.push(Reverse((0, source)));
        while let Some(Reverse((cost, vertex))) = queue.pop() {
            if cost != distance[vertex] {
                continue;
            }
            for (index, edge) in graph[vertex].iter().enumerate() {
                if edge.capacity == 0 {
                    continue;
                }
                let next = cost + edge.cost + potential[vertex] - potential[edge.to];
                if next < distance[edge.to] {
                    distance[edge.to] = next;
                    previous[edge.to] = Some((vertex, index));
                    queue.push(Reverse((next, edge.to)));
                }
            }
        }
        if distance[sink] == i64::MAX {
            return Err(SlotError::Config(
                "no balanced RF3 assignment exists".into(),
            ));
        }
        for (value, cost) in potential.iter_mut().zip(distance) {
            if cost != i64::MAX {
                *value += cost;
            }
        }
        let mut vertex = sink;
        while vertex != source {
            let (from, index) = previous[vertex].ok_or_else(|| {
                SlotError::Config("incomplete incremental assignment path".into())
            })?;
            let reverse = graph[from][index].reverse;
            graph[from][index].capacity -= 1;
            graph[vertex][reverse].capacity += 1;
            vertex = from;
        }
    }
    Ok(())
}
