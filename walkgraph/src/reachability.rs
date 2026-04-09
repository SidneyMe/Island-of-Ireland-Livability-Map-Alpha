use crate::serialize::{graph_paths_for_dir, GraphMeta};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const MISSING_WEIGHT_ROW: u32 = u32::MAX;

#[derive(Debug)]
pub struct GraphCsr {
    pub node_count: usize,
    pub edge_count: usize,
    pub offsets: Vec<u64>,
    pub targets: Vec<u32>,
    pub lengths: Vec<f32>,
}

#[derive(Debug)]
pub struct AmenityWeights {
    pub category_count: usize,
    pub node_to_row: Vec<u32>,
    pub counts_flat: Vec<u32>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
struct State {
    distance: f32,
    node: u32,
}

impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn read_u32_sidecar(path: &Path) -> Result<Vec<u32>, Box<dyn Error>> {
    let length = fs::metadata(path)?.len();
    if length % 4 != 0 {
        return Err(format!("u32 sidecar has invalid byte length: {}", path.display()).into());
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut values = Vec::with_capacity((length / 4) as usize);
    let mut buffer = [0_u8; 4];
    while values.len() < values.capacity() {
        reader.read_exact(&mut buffer)?;
        values.push(u32::from_le_bytes(buffer));
    }
    Ok(values)
}

fn read_u64_sidecar(path: &Path) -> Result<Vec<u64>, Box<dyn Error>> {
    let length = fs::metadata(path)?.len();
    if length % 8 != 0 {
        return Err(format!("u64 sidecar has invalid byte length: {}", path.display()).into());
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut values = Vec::with_capacity((length / 8) as usize);
    let mut buffer = [0_u8; 8];
    while values.len() < values.capacity() {
        reader.read_exact(&mut buffer)?;
        values.push(u64::from_le_bytes(buffer));
    }
    Ok(values)
}

fn read_f32_sidecar(path: &Path) -> Result<Vec<f32>, Box<dyn Error>> {
    let length = fs::metadata(path)?.len();
    if length % 4 != 0 {
        return Err(format!("f32 sidecar has invalid byte length: {}", path.display()).into());
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut values = Vec::with_capacity((length / 4) as usize);
    let mut buffer = [0_u8; 4];
    while values.len() < values.capacity() {
        reader.read_exact(&mut buffer)?;
        values.push(f32::from_le_bytes(buffer));
    }
    Ok(values)
}

pub fn load_graph_meta(graph_dir: &Path) -> Result<GraphMeta, Box<dyn Error>> {
    let payload = fs::read_to_string(graph_dir.join("walk_graph.meta.json"))?;
    Ok(serde_json::from_str(&payload)?)
}

pub fn load_graph_csr(graph_dir: &Path) -> Result<GraphCsr, Box<dyn Error>> {
    let meta = load_graph_meta(graph_dir)?;
    let paths = graph_paths_for_dir(graph_dir);
    let offsets = read_u64_sidecar(&paths.adjacency_offsets_bin)?;
    let targets = read_u32_sidecar(&paths.adjacency_targets_bin)?;
    let lengths = read_f32_sidecar(&paths.adjacency_lengths_bin)?;
    let node_count = meta.node_count as usize;
    let edge_count = meta.edge_count as usize;

    if offsets.len() != node_count + 1 {
        return Err(format!(
            "adjacency offsets length mismatch: expected {}, found {}",
            node_count + 1,
            offsets.len()
        )
        .into());
    }
    if targets.len() != edge_count || lengths.len() != edge_count {
        return Err(format!(
            "adjacency edge length mismatch: expected {}, found targets={} lengths={}",
            edge_count,
            targets.len(),
            lengths.len()
        )
        .into());
    }

    Ok(GraphCsr {
        node_count,
        edge_count,
        offsets,
        targets,
        lengths,
    })
}

pub fn load_origins(path: &Path, node_count: usize) -> Result<Vec<u32>, Box<dyn Error>> {
    let origins = read_u32_sidecar(path)?;
    if origins
        .iter()
        .any(|origin| (*origin as usize) >= node_count)
    {
        return Err("origin node id exceeds graph node count".into());
    }
    Ok(origins)
}

pub fn load_amenity_weights(
    path: &Path,
    category_count: usize,
    node_count: usize,
) -> Result<AmenityWeights, Box<dyn Error>> {
    let record_len = 4_u64 * (1_u64 + category_count as u64);
    let total_bytes = fs::metadata(path)?.len();
    if total_bytes % record_len != 0 {
        return Err("amenity weights sidecar has invalid byte length".into());
    }
    let row_count = (total_bytes / record_len) as usize;
    let mut reader = BufReader::new(File::open(path)?);
    let mut node_to_row = vec![MISSING_WEIGHT_ROW; node_count];
    let mut counts_flat = Vec::with_capacity(row_count * category_count);

    for row_index in 0..row_count {
        let mut node_bytes = [0_u8; 4];
        reader.read_exact(&mut node_bytes)?;
        let node_id = u32::from_le_bytes(node_bytes);
        if (node_id as usize) >= node_count {
            return Err("amenity weight node id exceeds graph node count".into());
        }
        node_to_row[node_id as usize] = row_index as u32;
        for _ in 0..category_count {
            let mut count_bytes = [0_u8; 4];
            reader.read_exact(&mut count_bytes)?;
            counts_flat.push(u32::from_le_bytes(count_bytes));
        }
    }

    Ok(AmenityWeights {
        category_count,
        node_to_row,
        counts_flat,
    })
}

fn counts_for_origin(
    graph: &GraphCsr,
    weights: &AmenityWeights,
    origin: u32,
    cutoff_m: f32,
) -> Vec<u32> {
    let mut counts = vec![0_u32; weights.category_count];
    let mut heap = BinaryHeap::new();
    let mut best = HashMap::<u32, f32>::new();

    heap.push(State {
        distance: 0.0,
        node: origin,
    });
    best.insert(origin, 0.0);

    while let Some(State { distance, node }) = heap.pop() {
        let Some(&known_distance) = best.get(&node) else {
            continue;
        };
        if distance > known_distance {
            continue;
        }
        if distance > cutoff_m {
            continue;
        }

        let row_index = weights.node_to_row[node as usize];
        if row_index != MISSING_WEIGHT_ROW {
            let offset = row_index as usize * weights.category_count;
            for category_index in 0..weights.category_count {
                counts[category_index] += weights.counts_flat[offset + category_index];
            }
        }

        let edge_start = graph.offsets[node as usize] as usize;
        let edge_end = graph.offsets[node as usize + 1] as usize;
        for edge_index in edge_start..edge_end {
            let next_node = graph.targets[edge_index];
            let next_distance = distance + graph.lengths[edge_index];
            if next_distance > cutoff_m {
                continue;
            }
            let should_visit = match best.get(&next_node) {
                Some(existing) => next_distance < *existing,
                None => true,
            };
            if should_visit {
                best.insert(next_node, next_distance);
                heap.push(State {
                    distance: next_distance,
                    node: next_node,
                });
            }
        }
    }

    counts
}

pub fn run_reachability(
    graph_dir: &Path,
    origins_bin: &Path,
    amenity_weights_bin: &Path,
    category_count: usize,
    cutoff_m: f32,
    output_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let graph = load_graph_csr(graph_dir)?;
    let origins = load_origins(origins_bin, graph.node_count)?;
    let weights = load_amenity_weights(amenity_weights_bin, category_count, graph.node_count)?;

    let results: Vec<Vec<u32>> = origins
        .par_iter()
        .map(|origin| counts_for_origin(&graph, &weights, *origin, cutoff_m))
        .collect();

    let mut writer = BufWriter::new(File::create(output_path)?);
    for row in results {
        for value in row {
            writer.write_all(&value.to_le_bytes())?;
        }
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests;
