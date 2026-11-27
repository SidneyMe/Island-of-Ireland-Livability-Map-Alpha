//! Daily access score v4: native entries, reach and cell scoring.
//!
//! The Python reference (`precompute/daily_access_*.py`) defines the
//! behaviour; `precompute/daily_access_native.py` drives these entry points
//! through little-endian binary files and checks parity.
//!
//! * `daily-access-attach`: cells (`x f64, y f64, flags u8`; bit 0 = origin on
//!   land, bit 1 = deeper than the land band) + optional `u32` group per cell
//!   -> per-cell entry flag and the sorted endpoint nodes every group needs.
//!   Entries themselves are never stored: the score step recomputes them.
//! * `daily-access-objects`: object records -> per-kind node seeds (minimum
//!   offset per `(node, label)`) and a per-object loss reason.
//! * `daily-access-reach`: per service kind, seeds (`node u32, label u32,
//!   offset f64`) -> uint16 0.1 m codes + uint32 labels for the origins. Kinds
//!   run one after another so per-node state never exists for two kinds.
//! * `daily-access-score`: one block of cells + reach rows of the block's
//!   nodes + objects near the block (same-edge pieces) + per-cell penalties ->
//!   per cell status and quantised total/components/penalties/access.

pub mod geometry;
pub mod reach;
pub mod score;
pub mod snap;

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use rayon::prelude::*;
use serde::Deserialize;

use crate::reachability::load_graph_csr;
use crate::surface::load_study_area;

use self::geometry::{same_edge_distance, Piece, PIECE_LINE, PIECE_POINT, PIECE_ZERO};
use self::reach::{k_nearest_labels, Seed};
use self::score::{
    quantize_access, quantize_score, KindDistances, ScoreParams, Scorer, NEEDS, NETWORK_NO_WALKING_NETWORK,
    NETWORK_OK, NETWORK_OUTSIDE_MASK, UINT16_MISSING,
};
use self::snap::{BoundaryIndex, EdgeGrid, EntryContext, Network, REASON_ATTACHED, REASON_OUTSIDE_LAND_MASK};

type AnyError = Box<dyn Error>;

pub const CELL_FLAG_ON_LAND: u8 = 1;
pub const CELL_FLAG_INTERIOR: u8 = 2;
pub const OBJECT_FLAG_ON_LAND: u8 = 1;
pub const OBJECT_FLAG_INTERIOR: u8 = 2;
pub const OBJECT_FLAG_FOOTPRINT: u8 = 4;
const GRID_CELL_M: f64 = 250.0;

fn read_values<T, const N: usize>(path: &Path, decode: fn([u8; N]) -> T) -> Result<Vec<T>, AnyError> {
    let bytes = fs::read(path).map_err(|err| format!("{}: {err}", path.display()))?;
    if bytes.len() % N != 0 {
        return Err(format!("{} has an invalid length", path.display()).into());
    }
    Ok(bytes
        .chunks_exact(N)
        .map(|chunk| decode(chunk.try_into().expect("chunk size")))
        .collect())
}

fn write_values<T: Copy, const N: usize>(path: &Path, values: &[T], encode: fn(T) -> [u8; N]) -> Result<(), AnyError> {
    let mut bytes = Vec::with_capacity(values.len() * N);
    for value in values {
        bytes.extend_from_slice(&encode(*value));
    }
    fs::write(path, bytes).map_err(|err| format!("{}: {err}", path.display()).into())
}

fn read_cells(path: &Path) -> Result<Vec<(f64, f64, u8)>, AnyError> {
    let bytes = fs::read(path)?;
    if bytes.len() % 17 != 0 {
        return Err("cells.bin must hold (f64 x, f64 y, u8 flags) records".into());
    }
    Ok(bytes
        .chunks_exact(17)
        .map(|chunk| {
            (
                f64::from_le_bytes(chunk[0..8].try_into().unwrap()),
                f64::from_le_bytes(chunk[8..16].try_into().unwrap()),
                chunk[16],
            )
        })
        .collect())
}

fn cells_extent(cells: &[(f64, f64, u8)], pad: f64) -> [f64; 4] {
    let mut extent = [f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY];
    for &(x, y, flags) in cells {
        if flags & CELL_FLAG_ON_LAND == 0 {
            continue;
        }
        extent = [extent[0].min(x), extent[1].min(y), extent[2].max(x), extent[3].max(y)];
    }
    if !extent[0].is_finite() {
        return [0.0, 0.0, 0.0, 0.0];
    }
    [extent[0] - pad, extent[1] - pad, extent[2] + pad, extent[3] + pad]
}

// --- objects ------------------------------------------------------------------

/// One geometric source: `kind u16, flags u8, pad u8, label u32, x f64, y f64,
/// first_ring u32, ring_count u32` (32 bytes). Rings live in
/// `ring_offsets.bin` (u64, vertex offsets) + `ring_vertices.bin` (f64 x, y),
/// each ring closed (first vertex repeated).
#[derive(Clone, Copy, Debug)]
pub struct ObjectRecord {
    pub kind: u16,
    pub flags: u8,
    pub label: u32,
    pub x: f64,
    pub y: f64,
    pub first_ring: u32,
    pub ring_count: u32,
}

#[derive(Deserialize)]
pub struct ObjectFiles {
    objects_bin: PathBuf,
    ring_offsets_bin: PathBuf,
    ring_vertices_bin: PathBuf,
}

pub struct Objects {
    pub records: Vec<ObjectRecord>,
    ring_offsets: Vec<u64>,
    vertices: Vec<[f64; 2]>,
}

impl Objects {
    fn load(files: &ObjectFiles) -> Result<Self, AnyError> {
        let bytes = fs::read(&files.objects_bin)?;
        if bytes.len() % 32 != 0 {
            return Err("objects.bin must hold 32-byte records".into());
        }
        let records = bytes
            .chunks_exact(32)
            .map(|c| ObjectRecord {
                kind: u16::from_le_bytes(c[0..2].try_into().unwrap()),
                flags: c[2],
                label: u32::from_le_bytes(c[4..8].try_into().unwrap()),
                x: f64::from_le_bytes(c[8..16].try_into().unwrap()),
                y: f64::from_le_bytes(c[16..24].try_into().unwrap()),
                first_ring: u32::from_le_bytes(c[24..28].try_into().unwrap()),
                ring_count: u32::from_le_bytes(c[28..32].try_into().unwrap()),
            })
            .collect();
        let ring_offsets: Vec<u64> = read_values(&files.ring_offsets_bin, u64::from_le_bytes)?;
        let raw: Vec<f64> = read_values(&files.ring_vertices_bin, f64::from_le_bytes)?;
        let vertices = raw.chunks_exact(2).map(|pair| [pair[0], pair[1]]).collect();
        Ok(Self { records, ring_offsets, vertices })
    }

    fn rings(&self, record: &ObjectRecord) -> Vec<&[[f64; 2]]> {
        (record.first_ring..record.first_ring + record.ring_count)
            .map(|ring| {
                let start = self.ring_offsets[ring as usize] as usize;
                let end = self.ring_offsets[ring as usize + 1] as usize;
                &self.vertices[start..end]
            })
            .collect()
    }

    fn bbox(&self, record: &ObjectRecord) -> [f64; 4] {
        if record.flags & OBJECT_FLAG_FOOTPRINT == 0 {
            return [record.x, record.y, record.x, record.y];
        }
        let mut bbox = [f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY];
        for ring in self.rings(record) {
            for vertex in ring {
                bbox = [bbox[0].min(vertex[0]), bbox[1].min(vertex[1]), bbox[2].max(vertex[0]), bbox[3].max(vertex[1])];
            }
        }
        bbox
    }

    fn entries(&self, context: &EntryContext<'_>, record: &ObjectRecord) -> (snap::Entries, u8) {
        if record.flags & OBJECT_FLAG_ON_LAND == 0 {
            return (snap::Entries::default(), REASON_OUTSIDE_LAND_MASK);
        }
        let interior = record.flags & OBJECT_FLAG_INTERIOR != 0;
        if record.flags & OBJECT_FLAG_FOOTPRINT != 0 {
            context.footprint(&self.rings(record), interior)
        } else {
            context.point(record.x, record.y, interior)
        }
    }
}

// --- attach -----------------------------------------------------------------

pub struct AttachArgs {
    pub graph_dir: PathBuf,
    pub mask_geojson: PathBuf,
    pub cells_bin: PathBuf,
    pub groups_bin: Option<PathBuf>,
    pub out_dir: PathBuf,
    pub radius_m: f64,
    pub merge_m: f64,
}

pub fn run_attach(args: AttachArgs) -> Result<(), AnyError> {
    let cells = read_cells(&args.cells_bin)?;
    let groups: Vec<u32> = match &args.groups_bin {
        Some(path) => read_values(path, u32::from_le_bytes)?,
        None => vec![0; cells.len()],
    };
    if groups.len() != cells.len() {
        return Err("groups.bin and cells.bin disagree".into());
    }
    let network = Network::load(&args.graph_dir)?;
    let mask = load_study_area(&args.mask_geojson).map_err(|err| err.to_string())?;
    let boundary = BoundaryIndex::from_multipolygon(&mask, args.radius_m);
    let grid = EdgeGrid::new(&network, cells_extent(&cells, args.radius_m), GRID_CELL_M);
    let context = EntryContext { network: &network, grid: &grid, boundary: &boundary, radius_m: args.radius_m, merge_m: args.merge_m };
    let per_cell: Vec<Option<Vec<u32>>> = cells
        .par_iter()
        .map(|&(x, y, flags)| {
            if flags & CELL_FLAG_ON_LAND == 0 {
                return None;
            }
            let (entries, _) = context.point(x, y, flags & CELL_FLAG_INTERIOR != 0);
            entries.has_network().then(|| entries.node_costs.iter().map(|item| item.0).collect())
        })
        .collect();

    let group_count = groups.iter().copied().max().map_or(0, |max| max as usize + 1);
    let mut by_group: Vec<Vec<u32>> = vec![Vec::new(); group_count];
    let mut flags_out = Vec::with_capacity(cells.len());
    for (cell, nodes) in per_cell.into_iter().enumerate() {
        flags_out.push(u8::from(nodes.is_some()));
        if let Some(nodes) = nodes {
            by_group[groups[cell] as usize].extend(nodes);
        }
    }
    let mut offsets = vec![0_u64];
    let mut group_nodes = Vec::new();
    for nodes in &mut by_group {
        nodes.sort_unstable();
        nodes.dedup();
        group_nodes.extend_from_slice(nodes);
        offsets.push(group_nodes.len() as u64);
    }
    let mut origins = group_nodes.clone();
    origins.sort_unstable();
    origins.dedup();
    fs::create_dir_all(&args.out_dir)?;
    fs::write(args.out_dir.join("entry_flags.bin"), &flags_out)?;
    write_values(&args.out_dir.join("group_offsets.bin"), &offsets, u64::to_le_bytes)?;
    write_values(&args.out_dir.join("group_nodes.bin"), &group_nodes, u32::to_le_bytes)?;
    write_values(&args.out_dir.join("origins.bin"), &origins, u32::to_le_bytes)?;
    eprintln!(
        "daily-access-attach: cells={} with_network={} origins={}",
        cells.len(),
        flags_out.iter().filter(|&&flag| flag == 1).count(),
        origins.len()
    );
    Ok(())
}

// --- objects ------------------------------------------------------------------

#[derive(Deserialize)]
struct ObjectsConfig {
    mask_geojson: PathBuf,
    objects: ObjectFiles,
    kind_count: usize,
    radius_m: f64,
    merge_m: f64,
    out_dir: PathBuf,
}

pub fn run_objects(graph_dir: &Path, config_json: &Path) -> Result<(), AnyError> {
    let config: ObjectsConfig = serde_json::from_str(&fs::read_to_string(config_json)?)?;
    let objects = Objects::load(&config.objects)?;
    let network = Network::load(graph_dir)?;
    let mask = load_study_area(&config.mask_geojson).map_err(|err| err.to_string())?;
    let boundary = BoundaryIndex::from_multipolygon(&mask, config.radius_m);
    let mut extent = [f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY];
    for record in &objects.records {
        let bbox = objects.bbox(record);
        extent = [extent[0].min(bbox[0]), extent[1].min(bbox[1]), extent[2].max(bbox[2]), extent[3].max(bbox[3])];
    }
    if !extent[0].is_finite() {
        extent = [0.0; 4];
    }
    let pad = config.radius_m;
    let grid = EdgeGrid::new(&network, [extent[0] - pad, extent[1] - pad, extent[2] + pad, extent[3] + pad], GRID_CELL_M);
    let context = EntryContext { network: &network, grid: &grid, boundary: &boundary, radius_m: config.radius_m, merge_m: config.merge_m };
    let results: Vec<(Vec<(u32, f64)>, u8)> = objects
        .records
        .par_iter()
        .map(|record| {
            let (entries, reason) = objects.entries(&context, record);
            (entries.node_costs, reason)
        })
        .collect();

    let mut seeds: Vec<Vec<(u32, u32, f64)>> = vec![Vec::new(); config.kind_count];
    let mut reasons = Vec::with_capacity(results.len());
    for (record, (node_costs, reason)) in objects.records.iter().zip(results) {
        reasons.push(reason);
        let kind = &mut seeds[record.kind as usize];
        kind.extend(node_costs.into_iter().map(|(node, cost)| (node, record.label, cost)));
    }
    fs::create_dir_all(&config.out_dir)?;
    for (kind, list) in seeds.iter_mut().enumerate() {
        list.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.total_cmp(&b.2)));
        list.dedup_by(|later, earlier| later.0 == earlier.0 && later.1 == earlier.1);
        let mut bytes = Vec::with_capacity(list.len() * 16);
        for &(node, label, offset) in list.iter() {
            bytes.extend_from_slice(&node.to_le_bytes());
            bytes.extend_from_slice(&label.to_le_bytes());
            bytes.extend_from_slice(&offset.to_le_bytes());
        }
        fs::write(config.out_dir.join(format!("seeds_{kind}.bin")), bytes)?;
    }
    fs::write(config.out_dir.join("reasons.bin"), &reasons)?;
    eprintln!(
        "daily-access-objects: objects={} attached={}",
        reasons.len(),
        reasons.iter().filter(|&&reason| reason == REASON_ATTACHED).count()
    );
    Ok(())
}

// --- reach ------------------------------------------------------------------

#[derive(Deserialize)]
struct ReachKindConfig {
    kind: String,
    seeds_bin: PathBuf,
    k: usize,
    cutoff_m: f64,
    out_distances: PathBuf,
    out_labels: PathBuf,
}

#[derive(Deserialize)]
struct ReachConfig {
    origins_bin: PathBuf,
    kinds: Vec<ReachKindConfig>,
}

pub fn run_reach(graph_dir: &Path, config_json: &Path) -> Result<(), AnyError> {
    let config: ReachConfig = serde_json::from_str(&fs::read_to_string(config_json)?)?;
    let csr = load_graph_csr(graph_dir)?;
    let origins: Vec<u32> = read_values(&config.origins_bin, u32::from_le_bytes)?;
    if origins.iter().any(|&node| node as usize >= csr.node_count) {
        return Err("origin node out of range".into());
    }
    for kind in &config.kinds {
        let bytes = fs::read(&kind.seeds_bin)?;
        if bytes.len() % 16 != 0 {
            return Err(format!("{}: seeds must be (u32 node, u32 label, f64 offset)", kind.kind).into());
        }
        let seeds: Vec<Seed> = bytes
            .chunks_exact(16)
            .map(|chunk| Seed {
                node: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                label: u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
                offset_m: f64::from_le_bytes(chunk[8..16].try_into().unwrap()),
            })
            .collect();
        if seeds.iter().any(|seed| seed.node as usize >= csr.node_count) {
            return Err(format!("{}: seed node out of range", kind.kind).into());
        }
        let result = k_nearest_labels(&csr, &seeds, kind.k, kind.cutoff_m, &origins);
        write_values(&kind.out_distances, &result.distance_codes, u16::to_le_bytes)?;
        write_values(&kind.out_labels, &result.labels, u32::to_le_bytes)?;
        eprintln!("daily-access-reach: {} seeds={} origins={}", kind.kind, seeds.len(), origins.len());
    }
    Ok(())
}

// --- score ------------------------------------------------------------------

#[derive(Deserialize)]
struct ScoreKindConfig {
    /// Index into the kind list shared with the objects / intervals files.
    kind_index: u16,
    need: String,
    quality: f64,
    k: usize,
    cutoff_m: f64,
    distances: Option<PathBuf>,
    labels: Option<PathBuf>,
    #[serde(default)]
    station: Option<usize>,
    #[serde(default)]
    station_quality: Vec<f64>,
}

#[derive(Deserialize)]
struct ContextParams {
    station: HashMap<String, [f64; 5]>,
    corridor: [f64; 2],
    penalty_cap: f64,
}

#[derive(Deserialize)]
struct ChannelSchema {
    version: u32,
    components: Vec<String>,
    penalties: Vec<String>,
    bonuses: Vec<String>,
}

#[derive(Deserialize)]
struct ScoreConfig {
    params: ScoreParams,
    cells_bin: PathBuf,
    mask_geojson: PathBuf,
    radius_m: f64,
    merge_m: f64,
    same_edge_iterations: usize,
    objects: Option<ObjectFiles>,
    /// Zero-cost in-park intervals: `kind u16, pad u16, label u32, edge u32,
    /// pad u32, lo f64, hi f64` (32 bytes).
    park_intervals_bin: Option<PathBuf>,
    origins_bin: PathBuf,
    kinds: Vec<ScoreKindConfig>,
    /// `f32 railway, f32 road` per cell.
    penalties_bin: Option<PathBuf>,
    #[serde(default)]
    context_bin: Option<PathBuf>,
    #[serde(default)]
    shared_bonuses_bin: Option<PathBuf>,
    #[serde(default)]
    context_params: Option<ContextParams>,
    #[serde(default)]
    channel_schema: Option<ChannelSchema>,
    out_dir: PathBuf,
}

struct KindReachRows {
    need_index: usize,
    quality: f64,
    green: bool,
    kind_index: u16,
    k: usize,
    cutoff_m: f64,
    distance_codes: Vec<u16>,
    labels: Vec<u32>,
    station: Option<usize>,
    station_quality: Vec<f64>,
}

type Target = (u16, u32, Piece, f64);

fn piece_floor(piece: &Piece) -> f64 {
    match piece.kind {
        PIECE_POINT => (piece.a.max(piece.lo).min(piece.hi) - piece.a).hypot(piece.b),
        PIECE_LINE => {
            if piece.a != 0.0 {
                let kink = -piece.b / piece.a;
                if piece.lo <= kink && kink <= piece.hi {
                    return 0.0;
                }
            }
            piece.value(piece.lo).min(piece.value(piece.hi))
        }
        _ => 0.0,
    }
}

fn kth_distance(best: &[(u32, f64)], k: usize, cutoff: f64) -> f64 {
    if best.len() < k {
        return cutoff;
    }
    let mut values: Vec<f64> = best.iter().map(|item| item.1).collect();
    values.sort_by(|a, b| a.total_cmp(b));
    values[k - 1].min(cutoff)
}

pub fn run_score(graph_dir: &Path, config_json: &Path) -> Result<(), AnyError> {
    let config: ScoreConfig = serde_json::from_str(&fs::read_to_string(config_json)?)?;
    if let Some(schema) = &config.channel_schema {
        if schema.version != 1 || schema.components != score::CATEGORIES
            || schema.penalties != ["railway_proximity", "road_proximity", "industrial_context", "over_concentration", "pitch_noise"]
            || schema.bonuses != ["rail_station_access", "green_buffer"] {
            return Err("unsupported score channel schema".into());
        }
    }
    let scorer = Scorer::new(&config.params)?;
    let cells = read_cells(&config.cells_bin)?;
    let origins: Vec<u32> = read_values(&config.origins_bin, u32::from_le_bytes)?;
    let penalties: Vec<f32> = match &config.penalties_bin {
        Some(path) => read_values(path, f32::from_le_bytes)?,
        None => vec![0.0; cells.len() * 2],
    };
    if penalties.len() != cells.len() * 2 {
        return Err("penalties.bin must hold two f32 per cell".into());
    }
    let context_values: Vec<f32> = match &config.context_bin {
        Some(path) => read_values(path, f32::from_le_bytes)?,
        None => vec![0.0; cells.len() * 5],
    };
    let shared_bonuses: Option<Vec<f32>> = config.shared_bonuses_bin.as_ref()
        .map(|path| read_values(path, f32::from_le_bytes)).transpose()?;
    if context_values.len() != cells.len() * 5 || shared_bonuses.as_ref().is_some_and(|v| v.len() != cells.len() * 2) {
        return Err("invalid context/bonus array dimensions".into());
    }
    let mut kinds = Vec::new();
    for kind in &config.kinds {
        let need_index = NEEDS
            .iter()
            .position(|need| *need == kind.need)
            .ok_or_else(|| format!("unknown need {}", kind.need))?;
        let (distance_codes, labels) = match (&kind.distances, &kind.labels) {
            (Some(distances), Some(labels)) => {
                (read_values(distances, u16::from_le_bytes)?, read_values(labels, u32::from_le_bytes)?)
            }
            _ => (vec![UINT16_MISSING; origins.len() * kind.k], vec![u32::MAX; origins.len() * kind.k]),
        };
        if distance_codes.len() != origins.len() * kind.k || labels.len() != distance_codes.len() {
            return Err(format!("{}: reach arrays do not match origins", kind.need).into());
        }
        kinds.push(KindReachRows {
            need_index,
            quality: kind.quality,
            green: kind.need == "green",
            kind_index: kind.kind_index,
            k: kind.k,
            cutoff_m: kind.cutoff_m,
            distance_codes,
            labels,
            station: kind.station,
            station_quality: kind.station_quality.clone(),
        });
    }
    let position_of_kind: HashMap<u16, usize> =
        kinds.iter().enumerate().map(|(position, kind)| (kind.kind_index, position)).collect();

    let network = Network::load(graph_dir)?;
    let mask = load_study_area(&config.mask_geojson).map_err(|err| err.to_string())?;
    let boundary = BoundaryIndex::from_multipolygon(&mask, config.radius_m);
    // Objects up to 2R from a cell can share an edge with it; their own
    // candidate edges reach one more R.
    let block = cells_extent(&cells, 0.0);
    let radius = config.radius_m;
    let grid = EdgeGrid::new(
        &network,
        [block[0] - 3.0 * radius, block[1] - 3.0 * radius, block[2] + 3.0 * radius, block[3] + 3.0 * radius],
        GRID_CELL_M,
    );
    let context = EntryContext { network: &network, grid: &grid, boundary: &boundary, radius_m: radius, merge_m: config.merge_m };

    // Same-edge targets of the block: edge -> (kind, label, piece, offset).
    let mut targets: HashMap<u32, Vec<Target>> = HashMap::new();
    if let Some(files) = &config.objects {
        let objects = Objects::load(files)?;
        let reach = [block[0] - 2.0 * radius, block[1] - 2.0 * radius, block[2] + 2.0 * radius, block[3] + 2.0 * radius];
        let near: Vec<&ObjectRecord> = objects
            .records
            .iter()
            .filter(|record| {
                let bbox = objects.bbox(record);
                position_of_kind.contains_key(&record.kind)
                    && bbox[2] >= reach[0]
                    && bbox[0] <= reach[2]
                    && bbox[3] >= reach[1]
                    && bbox[1] <= reach[3]
            })
            .collect();
        let pieces: Vec<Vec<(u32, Piece)>> =
            near.par_iter().map(|record| objects.entries(&context, record).0.pieces).collect();
        for (record, list) in near.into_iter().zip(pieces) {
            for (edge, piece) in list {
                targets.entry(edge).or_default().push((record.kind, record.label, piece, 0.0));
            }
        }
    }
    if let Some(path) = &config.park_intervals_bin {
        let bytes = fs::read(path)?;
        if bytes.len() % 32 != 0 {
            return Err("park_intervals.bin must hold 32-byte records".into());
        }
        for c in bytes.chunks_exact(32) {
            let kind = u16::from_le_bytes(c[0..2].try_into().unwrap());
            if !position_of_kind.contains_key(&kind) {
                continue;
            }
            let label = u32::from_le_bytes(c[4..8].try_into().unwrap());
            let edge = u32::from_le_bytes(c[8..12].try_into().unwrap());
            let lo = f64::from_le_bytes(c[16..24].try_into().unwrap());
            let hi = f64::from_le_bytes(c[24..32].try_into().unwrap());
            targets.entry(edge).or_default().push((kind, label, Piece { kind: PIECE_ZERO, lo, hi, a: 0.0, b: 0.0 }, 0.0));
        }
    }

    let row_of = |node: u32| origins.binary_search(&node).ok();
    let results: Vec<(u8, score::CellScore)> = cells
        .par_iter()
        .enumerate()
        .map(|(cell, &(x, y, flags))| {
            let railway = f64::from(penalties[cell * 2]);
            let road = f64::from(penalties[cell * 2 + 1]);
            if flags & CELL_FLAG_ON_LAND == 0 {
                return (NETWORK_OUTSIDE_MASK, score::CellScore::zero());
            }
            let (entries, _) = context.point(x, y, flags & CELL_FLAG_INTERIOR != 0);
            if !entries.has_network() {
                let mut result = scorer.score([0.0; 6], railway, road);
                score::apply_context(&mut result, [f64::from(context_values[cell * 5]), f64::from(context_values[cell * 5 + 1]), f64::from(context_values[cell * 5 + 2])], [0.0; 2], 20.0, false);
                return (NETWORK_NO_WALKING_NETWORK, result);
            }
            // Rows do not depend on the kind: look each node up once per cell.
            let rows: Vec<(usize, f64)> = entries
                .node_costs
                .iter()
                .filter_map(|&(node, cost)| row_of(node).map(|row| (row, cost)))
                .collect();
            let mut best: Vec<Vec<(u32, f64)>> = vec![Vec::new(); kinds.len()];
            for (position, kind) in kinds.iter().enumerate() {
                let list = &mut best[position];
                for &(row, cost) in &rows {
                    for column in 0..kind.k {
                        let label = kind.labels[row * kind.k + column];
                        if label == u32::MAX {
                            continue;
                        }
                        let code = kind.distance_codes[row * kind.k + column];
                        if code == UINT16_MISSING {
                            continue;
                        }
                        list.push((label, f64::from(code) / 10.0 + cost));
                    }
                }
                list.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
                list.dedup_by_key(|item| item.0);
            }
            // Same edge: exact direct walk; skip what cannot enter a kind's top k.
            let thresholds: Vec<f64> =
                kinds.iter().enumerate().map(|(position, kind)| kth_distance(&best[position], kind.k, kind.cutoff_m)).collect();
            for &(edge, cell_piece) in &entries.pieces {
                let Some(list) = targets.get(&edge) else {
                    continue;
                };
                let frame = network.frame(edge);
                let cell_floor = piece_floor(&cell_piece);
                for (kind, label, piece, offset) in list {
                    let position = position_of_kind[kind];
                    let floor = cell_floor + piece_floor(piece) + offset;
                    if floor > thresholds[position] {
                        continue;
                    }
                    let current = best[position].binary_search_by_key(label, |item| item.0);
                    if let Ok(index) = current {
                        if best[position][index].1 <= floor {
                            continue;
                        }
                    }
                    let distance = same_edge_distance(&frame, &cell_piece, piece, *offset, config.same_edge_iterations);
                    match current {
                        Ok(index) => {
                            if distance < best[position][index].1 {
                                best[position][index].1 = distance;
                            }
                        }
                        Err(index) => best[position].insert(index, (*label, distance)),
                    }
                }
            }
            let views: Vec<KindDistances<'_>> = kinds
                .iter()
                .enumerate()
                .filter(|(_, kind)| kind.station.is_none())
                .map(|(position, kind)| KindDistances {
                    need_index: kind.need_index,
                    quality: kind.quality,
                    green: kind.green,
                    kind_index: u32::from(kind.kind_index),
                    distances: &best[position],
                })
                .collect();
            let access = scorer.access_from_distances(&views);
            let mut result = scorer.score(access, railway, road);
            let mut station_value: f64 = 0.0;
            for (position, kind) in kinds.iter().enumerate() {
                if let Some(slot) = kind.station {
                    if let Some(&(label, distance)) = best[position].iter().filter(|(_, d)| *d <= kind.cutoff_m)
                        .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0))) {
                        result.stations[slot] = (label, distance);
                        if let Some(params) = &config.context_params {
                            let quality = kind.station_quality.get(label as usize).copied().unwrap_or(0.0);
                            let name = if slot == 0 { "rail" } else { "tram" };
                            station_value = station_value.max(score::station_bonus(distance, quality, f64::from(context_values[cell * 5 + 4]), params.station[name], params.corridor));
                        }
                    }
                }
            }
            let bonuses = match &shared_bonuses {
                Some(values) => [f64::from(values[cell * 2]), f64::from(values[cell * 2 + 1])],
                None => [station_value, f64::from(context_values[cell * 5 + 3]) * access[4]],
            };
            let cap = config.context_params.as_ref().map_or(20.0, |p| p.penalty_cap);
            score::apply_context(&mut result, [f64::from(context_values[cell * 5]), f64::from(context_values[cell * 5 + 1]), f64::from(context_values[cell * 5 + 2])], bonuses, cap, true);
            (NETWORK_OK, result)
        })
        .collect();

    let cell_count = cells.len();
    let mut status = Vec::with_capacity(cell_count);
    let mut total = Vec::with_capacity(cell_count);
    let mut component_codes = Vec::with_capacity(cell_count * 4);
    let mut penalty_codes = Vec::with_capacity(cell_count * 5);
    let mut bonus_codes = Vec::with_capacity(cell_count * 2);
    let mut station_labels = Vec::with_capacity(cell_count * 2);
    let mut station_distances = Vec::with_capacity(cell_count * 2);
    let mut access_codes = Vec::with_capacity(cell_count * 6);
    for (cell_status, cell_score) in &results {
        status.push(*cell_status);
        station_labels.extend(cell_score.stations.iter().map(|s| s.0));
        station_distances.extend(cell_score.stations.iter().map(|s| s.1));
        if *cell_status == NETWORK_OUTSIDE_MASK {
            total.push(UINT16_MISSING);
            component_codes.extend([UINT16_MISSING; 4]);
            penalty_codes.extend([UINT16_MISSING; 5]);
            bonus_codes.extend([UINT16_MISSING; 2]);
            access_codes.extend([UINT16_MISSING; 6]);
            continue;
        }
        total.push(quantize_score(cell_score.total));
        component_codes.extend(cell_score.components.iter().map(|value| quantize_score(*value)));
        penalty_codes.extend(cell_score.penalties.iter().map(|value| quantize_score(*value)));
        bonus_codes.extend(cell_score.bonuses.iter().map(|value| quantize_score(*value)));
        access_codes.extend(cell_score.access.iter().map(|value| quantize_access(*value)));
    }
    fs::create_dir_all(&config.out_dir)?;
    fs::write(config.out_dir.join("network_status.bin"), &status)?;
    write_values(&config.out_dir.join("total.bin"), &total, u16::to_le_bytes)?;
    write_values(&config.out_dir.join("components.bin"), &component_codes, u16::to_le_bytes)?;
    write_values(&config.out_dir.join("penalties.bin"), &penalty_codes, u16::to_le_bytes)?;
    write_values(&config.out_dir.join("bonuses.bin"), &bonus_codes, u16::to_le_bytes)?;
    write_values(&config.out_dir.join("station_labels.bin"), &station_labels, u32::to_le_bytes)?;
    write_values(&config.out_dir.join("station_distances.bin"), &station_distances, f64::to_le_bytes)?;
    write_values(&config.out_dir.join("service_access.bin"), &access_codes, u16::to_le_bytes)?;
    eprintln!("daily-access-score: cells={cell_count} same_edge_edges={}", targets.len());
    Ok(())
}
