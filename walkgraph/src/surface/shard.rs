//! Per-shard processing: compute land-coverage ratios via point sampling
//! and snap each land cell to the nearest walk graph node.
//!
//! Deliberately avoids `geo::BooleanOps` — the sweep-line implementation in
//! geo 0.28 panics on complex coastal MultiPolygon geometries (Ireland has
//! 669 polygon components after simplification).
//!
//! Instead, we use a per-shard polygon subset (only polygons whose AABB
//! overlaps the shard) + point-in-polygon sampling for land coverage ratios.
//! This avoids the "large mainland AABB matches every cell" problem.

use std::error::Error;
use std::path::Path;

use geo::{BoundingRect, Contains, MultiPolygon, Point, Polygon};

use super::kdtree::NodeKdTree;
use super::npz::{npy_bool, npy_f32, npy_i32, write_npz};

// ── Global polygon bounding-box index ───────────────────────────────────────

/// Stores per-polygon bounding boxes alongside the polygon geometry.
/// Used to build per-shard subsets cheaply.
pub struct PolyIndex {
    pub entries: Vec<(f64, f64, f64, f64, Polygon<f64>)>, // (min_x, min_y, max_x, max_y, poly)
}

impl PolyIndex {
    pub fn build(mp: &MultiPolygon<f64>) -> Self {
        let entries =
            mp.0.iter()
                .filter_map(|poly| {
                    let bb = poly.bounding_rect()?;
                    Some((bb.min().x, bb.min().y, bb.max().x, bb.max().y, poly.clone()))
                })
                .collect();
        Self { entries }
    }

    /// True if the entire shard rectangle is covered by inset-point containment.
    /// Checks four inset corners + centre — used for the "fully inland" fast path.
    pub fn contains_rect_approx(&self, rx0: f64, ry0: f64, rx1: f64, ry1: f64) -> bool {
        const INSET: f64 = 50.0;
        let cx = (rx0 + rx1) * 0.5;
        let cy = (ry0 + ry1) * 0.5;
        let test_pts = [
            (rx0 + INSET, ry0 + INSET),
            (rx1 - INSET, ry0 + INSET),
            (rx0 + INSET, ry1 - INSET),
            (rx1 - INSET, ry1 - INSET),
            (cx, cy),
        ];
        test_pts
            .iter()
            .all(|&(x, y)| self.contains_point_global(x, y))
    }

    /// Point-in-polygon against ALL polygon entries (used for the 5-point inland check).
    fn contains_point_global(&self, x: f64, y: f64) -> bool {
        let pt = Point::new(x, y);
        for (min_x, min_y, max_x, max_y, poly) in &self.entries {
            if x >= *min_x && x <= *max_x && y >= *min_y && y <= *max_y {
                if poly.contains(&pt) {
                    return true;
                }
            }
        }
        false
    }

    /// Build a per-shard subset: only polygons whose AABB overlaps (sx0,sy0,sx1,sy1).
    /// This is the key optimisation — most shards only overlap 1-5 polygons.
    pub fn shard_subset(&self, sx0: f64, sy0: f64, sx1: f64, sy1: f64) -> ShardPolyIndex<'_> {
        let refs: Vec<&(f64, f64, f64, f64, Polygon<f64>)> = self
            .entries
            .iter()
            .filter(|(min_x, min_y, max_x, max_y, _)| {
                sx1 >= *min_x && sx0 <= *max_x && sy1 >= *min_y && sy0 <= *max_y
            })
            .collect();
        ShardPolyIndex { refs }
    }
}

// ── Per-shard polygon view ────────────────────────────────────────────────────

/// Subset of `PolyIndex` entries relevant to one shard.
/// Borrows from the global index — no polygon cloning.
pub struct ShardPolyIndex<'a> {
    refs: Vec<&'a (f64, f64, f64, f64, Polygon<f64>)>,
}

pub struct CellPolyIndex<'a> {
    refs: Vec<&'a (f64, f64, f64, f64, Polygon<f64>)>,
}

impl<'a> ShardPolyIndex<'a> {
    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    pub fn cell_candidates(&self, cx0: f64, cy0: f64, cx1: f64, cy1: f64) -> CellPolyIndex<'a> {
        let refs = self
            .refs
            .iter()
            .copied()
            .filter(|(min_x, min_y, max_x, max_y, _)| {
                cx1 >= *min_x && cx0 <= *max_x && cy1 >= *min_y && cy0 <= *max_y
            })
            .collect();
        CellPolyIndex { refs }
    }

    /// True if the point (x, y) is contained in any relevant polygon.
    #[inline]
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        let pt = Point::new(x, y);
        for &&(min_x, min_y, max_x, max_y, ref poly) in &self.refs {
            if x >= min_x && x <= max_x && y >= min_y && y <= max_y {
                if poly.contains(&pt) {
                    return true;
                }
            }
        }
        false
    }
}

impl<'a> CellPolyIndex<'a> {
    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    #[inline]
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        let pt = Point::new(x, y);
        for &&(min_x, min_y, max_x, max_y, ref poly) in &self.refs {
            if x >= min_x && x <= max_x && y >= min_y && y <= max_y {
                if poly.contains(&pt) {
                    return true;
                }
            }
        }
        false
    }
}

// ── Shard entry ──────────────────────────────────────────────────────────────

pub struct ShardEntry {
    pub shard_id: String,
    pub x_min_m: i64,
    pub y_min_m: i64,
    pub shard_size_m: i64,
    pub cells_per_side: usize,
    pub resolution_m: i64,
}

impl ShardEntry {
    pub fn x_max_m(&self) -> i64 {
        self.x_min_m + self.shard_size_m
    }
    pub fn y_max_m(&self) -> i64 {
        self.y_min_m + self.shard_size_m
    }
}

// ── Per-cell land coverage ratio ─────────────────────────────────────────────

const SAMPLE_GRID: usize = 4; // 4x4 = 16 sample points per cell
const FULL_LAND_PROBE_OFFSETS: &[(f64, f64)] = &[
    (0.25, 0.25),
    (0.75, 0.25),
    (0.50, 0.50),
    (0.25, 0.75),
    (0.75, 0.75),
];

/// Estimate what fraction of a cell's area overlaps the study area.
/// Samples a 4x4 grid of interior points. Returns a value in [0.0, 1.0].
#[inline]
fn sample_cell_ratio(cell_idx: &CellPolyIndex<'_>, cx_min: f64, cy_min: f64, res: f64) -> f32 {
    let step = res / (SAMPLE_GRID as f64 + 1.0);
    let mut inside = 0u32;
    for row in 1..=(SAMPLE_GRID as u32) {
        for col in 1..=(SAMPLE_GRID as u32) {
            let x = cx_min + col as f64 * step;
            let y = cy_min + row as f64 * step;
            if cell_idx.contains_point(x, y) {
                inside += 1;
            }
        }
    }
    inside as f32 / (SAMPLE_GRID * SAMPLE_GRID) as f32
}

#[inline]
fn cell_area_ratio(shard_idx: &ShardPolyIndex<'_>, cx_min: f64, cy_min: f64, res: f64) -> f32 {
    let cell_idx = shard_idx.cell_candidates(cx_min, cy_min, cx_min + res, cy_min + res);
    if cell_idx.is_empty() {
        return 0.0;
    }

    let fully_land = FULL_LAND_PROBE_OFFSETS.iter().all(|(x_factor, y_factor)| {
        cell_idx.contains_point(cx_min + (res * x_factor), cy_min + (res * y_factor))
    });
    if fully_land {
        return 1.0;
    }

    sample_cell_ratio(&cell_idx, cx_min, cy_min, res)
}

// ── Main shard processor ─────────────────────────────────────────────────────

pub fn process_shard(
    entry: &ShardEntry,
    poly_index: &PolyIndex,
    kd: &NodeKdTree,
    out_path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let n = entry.cells_per_side;
    let total_cells = n * n;
    let res = entry.resolution_m as f64;

    let shard_x0 = entry.x_min_m as f64;
    let shard_y0 = entry.y_min_m as f64;
    let shard_x1 = entry.x_max_m() as f64;
    let shard_y1 = entry.y_max_m() as f64;

    let mut ratios = vec![0.0f32; total_cells];
    let mut node_idx = vec![-1i32; total_cells];

    // ── Fast path: shard is entirely inland ──────────────────────────────────
    let fully_inland = poly_index.contains_rect_approx(shard_x0, shard_y0, shard_x1, shard_y1);

    if fully_inland {
        ratios.fill(1.0f32);
    } else {
        // ── Build per-shard polygon subset ───────────────────────────────────
        // Only polygons whose AABB overlaps this shard are relevant. For a
        // typical inland shard this is 1-2 polygons; for a coastal shard ~5-20.
        let shard_idx = poly_index.shard_subset(shard_x0, shard_y0, shard_x1, shard_y1);

        if !shard_idx.is_empty() {
            // ── Per-cell sampling ─────────────────────────────────────────────
            // Filter candidate polygons per cell before sampling so coastal
            // cells only inspect the small set of overlapping geometries.
            for row in 0..n {
                let cy_min = shard_y0 + (row as f64) * res;
                for col in 0..n {
                    let cx_min = shard_x0 + (col as f64) * res;
                    ratios[row * n + col] = cell_area_ratio(&shard_idx, cx_min, cy_min, res);
                }
            }
        }
    }

    // ── Snap each land cell to the nearest walk graph node ───────────────────
    let half = res * 0.5;
    for row in 0..n {
        let cy_centre = shard_y0 + (row as f64) * res + half;
        for col in 0..n {
            let flat = row * n + col;
            if ratios[flat] <= 0.0 {
                continue;
            }
            let cx_centre = shard_x0 + (col as f64) * res + half;
            node_idx[flat] = kd.nearest(cx_centre, cy_centre) as i32;
        }
    }

    // ── Build valid_land_mask ─────────────────────────────────────────────────
    let valid_land: Vec<bool> = ratios.iter().map(|&r| r > 0.0).collect();

    // ── Write .npz ───────────────────────────────────────────────────────────
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let x_min_arr = [entry.x_min_m as i32];
    let y_min_arr = [entry.y_min_m as i32];

    let npz_entries: &[(&str, Vec<u8>)] = &[
        ("origin_node_idx", npy_i32(&node_idx, &[n, n])),
        ("effective_area_ratio", npy_f32(&ratios, &[n, n])),
        ("valid_land_mask", npy_bool(&valid_land, &[n, n])),
        ("x_min_m", npy_i32(&x_min_arr, &[1])),
        ("y_min_m", npy_i32(&y_min_arr, &[1])),
    ];

    write_npz(out_path, npz_entries)?;
    Ok(())
}
