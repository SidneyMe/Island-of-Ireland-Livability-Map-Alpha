//! Edge entries of points and footprints (daily access v4).
//!
//! Mirrors `precompute/daily_access_snap.py`: every walk edge within the
//! radius whose straight connector stays on land is an entry; there is no
//! nearest-node-per-component rule. Python decides whether a point lies
//! deeper than the land band (then the boundary test is skipped) and passes
//! it as a flag; band points are tested against the boundary segments here.

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::Path;

use geo::MultiPolygon;

use super::geometry::{endpoint_costs, footprint_pieces, point_pieces, EdgeFrame, Piece};
use crate::reachability::load_graph_csr;
use crate::surface::itm::wgs84_to_itm;

pub const REASON_ATTACHED: u8 = 0;
pub const REASON_OUTSIDE_LAND_MASK: u8 = 1;
pub const REASON_NO_EDGE_WITHIN_RADIUS: u8 = 2;
pub const REASON_NO_LAND_CONNECTOR: u8 = 3;

/// Node coordinates (ITM) and the undirected edges (`a < b` CSR slots, in slot order).
pub struct Network {
    pub x: Vec<f64>,
    pub y: Vec<f64>,
    pub edge_a: Vec<u32>,
    pub edge_b: Vec<u32>,
    pub edge_length: Vec<f64>,
}

impl Network {
    pub fn from_parts(x: Vec<f64>, y: Vec<f64>, offsets: &[u64], targets: &[u32], lengths: &[f32]) -> Self {
        let mut edge_a = Vec::new();
        let mut edge_b = Vec::new();
        let mut edge_length = Vec::new();
        for node in 0..offsets.len().saturating_sub(1) {
            for slot in offsets[node] as usize..offsets[node + 1] as usize {
                let target = targets[slot];
                if (node as u32) < target {
                    edge_a.push(node as u32);
                    edge_b.push(target);
                    edge_length.push(f64::from(lengths[slot]));
                }
            }
        }
        Self { x, y, edge_a, edge_b, edge_length }
    }

    pub fn load(graph_dir: &Path) -> Result<Self, Box<dyn Error>> {
        let node_bytes = fs::read(graph_dir.join("walk_graph.nodes.bin"))?;
        if node_bytes.len() % 8 != 0 {
            return Err("walk_graph.nodes.bin has an invalid length".into());
        }
        let mut x = Vec::with_capacity(node_bytes.len() / 8);
        let mut y = Vec::with_capacity(node_bytes.len() / 8);
        for chunk in node_bytes.chunks_exact(8) {
            let lat = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as f64;
            let lon = f32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]) as f64;
            let (easting, northing) = wgs84_to_itm(lat, lon);
            x.push(easting);
            y.push(northing);
        }
        let csr = load_graph_csr(graph_dir)?;
        if csr.node_count != x.len() {
            return Err("walk_graph.nodes.bin does not match the CSR node count".into());
        }
        Ok(Self::from_parts(x, y, &csr.offsets, &csr.targets, &csr.lengths))
    }

    pub fn edge_count(&self) -> usize {
        self.edge_a.len()
    }

    pub fn frame(&self, edge: u32) -> EdgeFrame {
        let (a, b) = (self.edge_a[edge as usize], self.edge_b[edge as usize]);
        EdgeFrame::new(
            edge,
            a,
            b,
            self.x[a as usize],
            self.y[a as usize],
            self.x[b as usize],
            self.y[b as usize],
            self.edge_length[edge as usize],
        )
    }

    pub fn edge_bbox(&self, edge: u32) -> [f64; 4] {
        let (a, b) = (self.edge_a[edge as usize] as usize, self.edge_b[edge as usize] as usize);
        [
            self.x[a].min(self.x[b]),
            self.y[a].min(self.y[b]),
            self.x[a].max(self.x[b]),
            self.y[a].max(self.y[b]),
        ]
    }
}

/// Uniform-grid CSR index of edge bboxes, restricted to edges meeting `extent`.
pub struct EdgeGrid {
    cell_m: f64,
    min_x: f64,
    min_y: f64,
    cols: i64,
    rows: i64,
    offsets: Vec<u64>,
    edges: Vec<u32>,
}

impl EdgeGrid {
    pub fn new(network: &Network, extent: [f64; 4], cell_m: f64) -> Self {
        let cols = (((extent[2] - extent[0]) / cell_m).floor() as i64 + 1).max(1);
        let rows = (((extent[3] - extent[1]) / cell_m).floor() as i64 + 1).max(1);
        let mut grid = Self { cell_m, min_x: extent[0], min_y: extent[1], cols, rows, offsets: Vec::new(), edges: Vec::new() };
        let mut pairs: Vec<(u64, u32)> = Vec::new();
        for edge in 0..network.edge_count() as u32 {
            let bbox = network.edge_bbox(edge);
            if bbox[2] < extent[0] || bbox[0] > extent[2] || bbox[3] < extent[1] || bbox[1] > extent[3] {
                continue;
            }
            let (c0, r0, c1, r1) = grid.cell_range(bbox);
            for row in r0..=r1 {
                for col in c0..=c1 {
                    pairs.push(((row * cols + col) as u64, edge));
                }
            }
        }
        pairs.sort_unstable();
        let cell_count = (cols * rows) as usize;
        let mut offsets = vec![0_u64; cell_count + 1];
        for &(cell, _) in &pairs {
            offsets[cell as usize + 1] += 1;
        }
        for index in 0..cell_count {
            offsets[index + 1] += offsets[index];
        }
        grid.offsets = offsets;
        grid.edges = pairs.into_iter().map(|(_, edge)| edge).collect();
        grid
    }

    fn cell_range(&self, bbox: [f64; 4]) -> (i64, i64, i64, i64) {
        let clamp_col = |value: f64| (((value - self.min_x) / self.cell_m).floor() as i64).clamp(0, self.cols - 1);
        let clamp_row = |value: f64| (((value - self.min_y) / self.cell_m).floor() as i64).clamp(0, self.rows - 1);
        (clamp_col(bbox[0]), clamp_row(bbox[1]), clamp_col(bbox[2]), clamp_row(bbox[3]))
    }

    /// Edges whose bbox may meet the box (superset; sorted, unique).
    pub fn query(&self, network: &Network, bbox: [f64; 4]) -> Vec<u32> {
        let (c0, r0, c1, r1) = self.cell_range(bbox);
        let mut found = Vec::new();
        for row in r0..=r1 {
            for col in c0..=c1 {
                let cell = (row * self.cols + col) as usize;
                for &edge in &self.edges[self.offsets[cell] as usize..self.offsets[cell + 1] as usize] {
                    let e = network.edge_bbox(edge);
                    if e[2] >= bbox[0] && e[0] <= bbox[2] && e[3] >= bbox[1] && e[1] <= bbox[3] {
                        found.push(edge);
                    }
                }
            }
        }
        found.sort_unstable();
        found.dedup();
        found
    }
}

/// Uniform-grid index of land-mask boundary segments.
pub struct BoundaryIndex {
    cell_m: f64,
    segments: Vec<[f64; 4]>,
    buckets: HashMap<(i64, i64), Vec<u32>>,
}

impl BoundaryIndex {
    pub fn from_multipolygon(mask: &MultiPolygon<f64>, cell_m: f64) -> Self {
        let mut index = Self { cell_m, segments: Vec::new(), buckets: HashMap::new() };
        for polygon in mask.0.iter() {
            for ring in std::iter::once(polygon.exterior()).chain(polygon.interiors().iter()) {
                for line in ring.lines() {
                    index.insert([line.start.x, line.start.y, line.end.x, line.end.y]);
                }
            }
        }
        index
    }

    fn bucket(&self, value: f64) -> i64 {
        (value / self.cell_m).floor() as i64
    }

    fn insert(&mut self, segment: [f64; 4]) {
        let id = self.segments.len() as u32;
        self.segments.push(segment);
        let (x0, x1) = (segment[0].min(segment[2]), segment[0].max(segment[2]));
        let (y0, y1) = (segment[1].min(segment[3]), segment[1].max(segment[3]));
        for bx in self.bucket(x0)..=self.bucket(x1) {
            for by in self.bucket(y0)..=self.bucket(y1) {
                self.buckets.entry((bx, by)).or_default().push(id);
            }
        }
    }

    /// Segments whose bbox may meet the box, in index order.
    pub fn near(&self, bbox: [f64; 4]) -> Vec<[f64; 4]> {
        let mut ids: Vec<u32> = Vec::new();
        for bx in self.bucket(bbox[0])..=self.bucket(bbox[2]) {
            for by in self.bucket(bbox[1])..=self.bucket(bbox[3]) {
                if let Some(found) = self.buckets.get(&(bx, by)) {
                    ids.extend_from_slice(found);
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter().map(|id| self.segments[id as usize]).collect()
    }
}

/// Admissible pieces per edge and the derived endpoint costs.
#[derive(Default, Debug, Clone)]
pub struct Entries {
    pub pieces: Vec<(u32, Piece)>,
    /// `(node, cost)`, one per node (minimum), sorted by node.
    pub node_costs: Vec<(u32, f64)>,
}

impl Entries {
    pub fn has_network(&self) -> bool {
        !self.pieces.is_empty()
    }

    fn add(&mut self, frame: &EdgeFrame, pieces: &[Piece]) {
        if pieces.is_empty() {
            return;
        }
        self.pieces.extend(pieces.iter().map(|piece| (frame.edge, *piece)));
        let (to_a, to_b) = endpoint_costs(frame, pieces, 0.0);
        self.node_costs.push((frame.node_a, to_a));
        self.node_costs.push((frame.node_b, to_b));
    }

    fn finish(&mut self) {
        self.node_costs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        self.node_costs.dedup_by_key(|item| item.0);
    }
}

pub struct EntryContext<'a> {
    pub network: &'a Network,
    pub grid: &'a EdgeGrid,
    pub boundary: &'a BoundaryIndex,
    pub radius_m: f64,
    pub merge_m: f64,
}

impl EntryContext<'_> {
    /// Every admissible entry of a point on land; `interior` skips the boundary test.
    pub fn point(&self, x: f64, y: f64, interior: bool) -> (Entries, u8) {
        let radius = self.radius_m;
        let bbox = [x - radius, y - radius, x + radius, y + radius];
        let boundary = if interior { None } else { Some(self.boundary.near(bbox)) };
        let candidates = self.grid.query(self.network, bbox);
        let mut entries = Entries::default();
        let mut scratch = Vec::new();
        for &edge in &candidates {
            let frame = self.network.frame(edge);
            scratch.clear();
            point_pieces(&frame, x, y, radius, boundary.as_deref(), None, self.merge_m, &mut scratch);
            entries.add(&frame, &scratch);
        }
        entries.finish();
        if entries.has_network() {
            return (entries, REASON_ATTACHED);
        }
        let in_radius = candidates.iter().any(|&edge| {
            scratch.clear();
            point_pieces(&self.network.frame(edge), x, y, radius, None, None, self.merge_m, &mut scratch);
            !scratch.is_empty()
        });
        (entries, if in_radius { REASON_NO_LAND_CONNECTOR } else { REASON_NO_EDGE_WITHIN_RADIUS })
    }

    /// Entries of a polygonal object: connectors from its outline, 0 inside.
    pub fn footprint(&self, rings: &[&[[f64; 2]]], interior: bool) -> (Entries, u8) {
        let radius = self.radius_m;
        let mut bounds = [f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY];
        for ring in rings {
            for vertex in ring.iter() {
                bounds[0] = bounds[0].min(vertex[0]);
                bounds[1] = bounds[1].min(vertex[1]);
                bounds[2] = bounds[2].max(vertex[0]);
                bounds[3] = bounds[3].max(vertex[1]);
            }
        }
        let bbox = [bounds[0] - radius, bounds[1] - radius, bounds[2] + radius, bounds[3] + radius];
        let boundary = if interior { None } else { Some(self.boundary.near(bbox)) };
        let candidates = self.grid.query(self.network, bbox);
        let mut entries = Entries::default();
        let mut scratch = Vec::new();
        for &edge in &candidates {
            let frame = self.network.frame(edge);
            scratch.clear();
            footprint_pieces(&frame, rings, radius, boundary.as_deref(), self.merge_m, &mut scratch);
            entries.add(&frame, &scratch);
        }
        entries.finish();
        if entries.has_network() {
            return (entries, REASON_ATTACHED);
        }
        let in_radius = candidates.iter().any(|&edge| {
            scratch.clear();
            footprint_pieces(&self.network.frame(edge), rings, radius, None, self.merge_m, &mut scratch);
            !scratch.is_empty()
        });
        (entries, if in_radius { REASON_NO_LAND_CONNECTOR } else { REASON_NO_EDGE_WITHIN_RADIUS })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::polygon;

    fn network(points: &[(f64, f64)], edges: &[(u32, u32)]) -> Network {
        let mut adjacency: Vec<Vec<(u32, f32)>> = vec![Vec::new(); points.len()];
        for &(a, b) in edges {
            let length = (points[a as usize].0 - points[b as usize].0).hypot(points[a as usize].1 - points[b as usize].1) as f32;
            adjacency[a as usize].push((b, length));
            adjacency[b as usize].push((a, length));
        }
        let mut offsets = vec![0_u64];
        let mut targets = Vec::new();
        let mut lengths = Vec::new();
        for list in adjacency {
            for (target, length) in list {
                targets.push(target);
                lengths.push(length);
            }
            offsets.push(targets.len() as u64);
        }
        Network::from_parts(points.iter().map(|p| p.0).collect(), points.iter().map(|p| p.1).collect(), &offsets, &targets, &lengths)
    }

    #[test]
    fn middle_of_a_long_edge_is_an_entry() {
        let net = network(&[(0.0, 0.0), (2000.0, 0.0)], &[(0, 1)]);
        let grid = EdgeGrid::new(&net, [-500.0, -500.0, 2500.0, 500.0], 250.0);
        let mask = MultiPolygon(vec![polygon![(x: -5000.0, y: -5000.0), (x: 5000.0, y: -5000.0), (x: 5000.0, y: 5000.0), (x: -5000.0, y: 5000.0)]]);
        let boundary = BoundaryIndex::from_multipolygon(&mask, 250.0);
        let context = EntryContext { network: &net, grid: &grid, boundary: &boundary, radius_m: 250.0, merge_m: 1e-7 };
        let (entries, reason) = context.point(1000.0, 40.0, false);
        assert_eq!(reason, REASON_ATTACHED);
        assert_eq!(entries.node_costs.len(), 2);
        // c = 1: the cost to a falls towards a, so the radius limit is the minimum.
        let expected = 250.0 + 1000.0 - (250.0_f64 * 250.0 - 40.0 * 40.0).sqrt();
        assert!((entries.node_costs[0].1 - expected).abs() < 1e-6, "{:?}", entries.node_costs);
        let (none, reason) = context.point(1000.0, 400.0, false);
        assert!(!none.has_network());
        assert_eq!(reason, REASON_NO_EDGE_WITHIN_RADIUS);
    }
}
