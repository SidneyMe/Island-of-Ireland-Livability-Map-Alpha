//! k-nearest distinct labels by multi-source Dijkstra (daily access v4).
//!
//! Mirrors `precompute/daily_access_reach.py`: seeds start at their object
//! connector / approach offset, the cutoff applies to the full distance, edge
//! lengths are the graph's f32 values summed in f64, and the heap pops in
//! `(distance, node, label)` order. Only origin nodes keep distances; other
//! nodes only remember which labels they already accepted.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::reachability::GraphCsr;

pub const DISTANCE_MISSING: u16 = u16::MAX;
pub const LABEL_MISSING: u32 = u32::MAX;
const NO_ROW: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Seed {
    pub node: u32,
    pub label: u32,
    pub offset_m: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct HeapItem {
    distance: f64,
    node: u32,
    label: u32,
}

impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap: reverse for the smallest (distance, node, label).
        other
            .distance
            .total_cmp(&self.distance)
            .then_with(|| other.node.cmp(&self.node))
            .then_with(|| other.label.cmp(&self.label))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Distances in metres to the 0.1 m uint16 code (half up); too far = missing.
pub fn quantize_distance(distance_m: f64) -> u16 {
    let code = (distance_m * 10.0 + 0.5).floor();
    if code >= f64::from(DISTANCE_MISSING) {
        DISTANCE_MISSING
    } else {
        code as u16
    }
}

pub struct KindResult {
    pub k: usize,
    /// `origins.len() * k`, row-major, sorted per row by (code, label).
    pub distance_codes: Vec<u16>,
    pub labels: Vec<u32>,
    pub raw_distances_m: Vec<f64>,
}

pub fn k_nearest_labels(
    csr: &GraphCsr,
    seeds: &[Seed],
    k: usize,
    cutoff_m: f64,
    origins: &[u32],
) -> KindResult {
    assert!(k > 0 && k <= u8::MAX as usize, "k must be in 1..=255");
    let node_count = csr.node_count;
    let mut row_of_node = vec![NO_ROW; node_count];
    for (row, &node) in origins.iter().enumerate() {
        row_of_node[node as usize] = row as u32;
    }
    let mut accepted_count = vec![0_u8; node_count];
    let mut accepted_labels = vec![LABEL_MISSING; node_count * k];
    let mut raw = vec![f64::NAN; origins.len() * k];
    let mut raw_labels = vec![LABEL_MISSING; origins.len() * k];

    let mut heap: BinaryHeap<HeapItem> = seeds
        .iter()
        .filter(|seed| seed.offset_m <= cutoff_m)
        .map(|seed| HeapItem {
            distance: seed.offset_m,
            node: seed.node,
            label: seed.label,
        })
        .collect();

    let has_label = |labels: &[u32], count: u8, node: usize, label: u32| -> bool {
        labels[node * k..node * k + count as usize].contains(&label)
    };

    while let Some(item) = heap.pop() {
        let node = item.node as usize;
        let count = accepted_count[node];
        if count as usize >= k || has_label(&accepted_labels, count, node, item.label) {
            continue;
        }
        accepted_labels[node * k + count as usize] = item.label;
        let row = row_of_node[node];
        if row != NO_ROW {
            let slot = row as usize * k + count as usize;
            raw[slot] = item.distance;
            raw_labels[slot] = item.label;
        }
        accepted_count[node] = count + 1;

        let start = csr.offsets[node] as usize;
        let end = csr.offsets[node + 1] as usize;
        for slot in start..end {
            let neighbour = csr.targets[slot] as usize;
            let next = item.distance + f64::from(csr.lengths[slot]);
            if next > cutoff_m {
                continue;
            }
            let neighbour_count = accepted_count[neighbour];
            if neighbour_count as usize >= k
                || has_label(&accepted_labels, neighbour_count, neighbour, item.label)
            {
                continue;
            }
            heap.push(HeapItem {
                distance: next,
                node: neighbour as u32,
                label: item.label,
            });
        }
    }

    let mut distance_codes = vec![DISTANCE_MISSING; origins.len() * k];
    let mut labels = vec![LABEL_MISSING; origins.len() * k];
    let mut raw_distances_m = vec![f64::NAN; origins.len() * k];
    for row in 0..origins.len() {
        let mut entries: Vec<(u16, u32, f64)> = (0..k)
            .filter(|column| raw_labels[row * k + column] != LABEL_MISSING)
            .map(|column| {
                let distance = raw[row * k + column];
                (quantize_distance(distance), raw_labels[row * k + column], distance)
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        for (column, (code, label, distance)) in entries.into_iter().enumerate() {
            distance_codes[row * k + column] = code;
            labels[row * k + column] = label;
            raw_distances_m[row * k + column] = distance;
        }
    }
    KindResult {
        k,
        distance_codes,
        labels,
        raw_distances_m,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line_csr(lengths: &[f32]) -> GraphCsr {
        // Path 0 - 1 - 2 - ... with symmetric edges.
        let node_count = lengths.len() + 1;
        let mut adjacency: Vec<Vec<(u32, f32)>> = vec![Vec::new(); node_count];
        for (index, &length) in lengths.iter().enumerate() {
            adjacency[index].push((index as u32 + 1, length));
            adjacency[index + 1].push((index as u32, length));
        }
        let mut offsets = vec![0_u64];
        let mut targets = Vec::new();
        let mut edge_lengths = Vec::new();
        for list in adjacency {
            for (target, length) in list {
                targets.push(target);
                edge_lengths.push(length);
            }
            offsets.push(targets.len() as u64);
        }
        GraphCsr {
            node_count,
            edge_count: targets.len(),
            offsets,
            targets,
            lengths: edge_lengths,
        }
    }

    #[test]
    fn keeps_distinct_labels_and_cutoff() {
        let csr = line_csr(&[100.0, 100.0, 100.0]);
        let seeds = [
            Seed { node: 0, label: 7, offset_m: 0.0 },
            Seed { node: 0, label: 7, offset_m: 5.0 },
            Seed { node: 3, label: 9, offset_m: 20.0 },
        ];
        let result = k_nearest_labels(&csr, &seeds, 3, 250.0, &[1, 2]);
        // node 1: label 7 at 100 m, label 9 at 220 m.
        assert_eq!(&result.distance_codes[0..3], &[1000, 2200, DISTANCE_MISSING]);
        assert_eq!(&result.labels[0..3], &[7, 9, LABEL_MISSING]);
        // node 2: label 9 at 120 m, label 7 at 200 m.
        assert_eq!(&result.distance_codes[3..6], &[1200, 2000, DISTANCE_MISSING]);
    }

    #[test]
    fn seed_offset_counts_toward_cutoff() {
        let csr = line_csr(&[700.0]);
        let result = k_nearest_labels(&csr, &[Seed { node: 1, label: 0, offset_m: 120.0 }], 1, 800.0, &[0, 1]);
        assert_eq!(result.distance_codes, vec![DISTANCE_MISSING, 1200]);
    }

    #[test]
    fn quantization_is_half_up() {
        assert_eq!(quantize_distance(123.45), 1235);
        assert_eq!(quantize_distance(0.04), 0);
        assert_eq!(quantize_distance(10_000.0), DISTANCE_MISSING);
    }
}
