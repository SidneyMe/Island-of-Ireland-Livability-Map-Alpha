use crate::haversine::haversine_m;
use crate::pbf::{open_spool_reader, read_spooled_edge_pair, RetainedNodes};
use crate::serialize::GraphPaths;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const EDGE_RECORD_BYTES: usize = 12;

#[derive(Debug)]
pub struct CompactNodeIndex {
    pub osm_ids: Vec<i64>,
    pub coords: Vec<(f32, f32)>,
    pub by_osm_id: HashMap<i64, u32>,
}

#[derive(Debug, Default)]
pub struct EdgeBuildStats {
    pub emitted_edges: u64,
    pub skipped_missing_nodes: u64,
    pub skipped_zero_length: u64,
}

pub fn build_compact_node_index(nodes: RetainedNodes) -> Result<CompactNodeIndex, Box<dyn Error>> {
    if nodes.osm_ids.len() != nodes.coords.len() {
        return Err("retained node ids and coords length mismatch".into());
    }
    if nodes.osm_ids.len() > u32::MAX as usize {
        return Err("graph has more than u32::MAX retained nodes".into());
    }

    let mut by_osm_id = HashMap::with_capacity(nodes.osm_ids.len());
    for (index, osm_id) in nodes.osm_ids.iter().copied().enumerate() {
        by_osm_id.insert(osm_id, index as u32);
    }

    Ok(CompactNodeIndex {
        osm_ids: nodes.osm_ids,
        coords: nodes.coords,
        by_osm_id,
    })
}

pub fn emit_edge_sidecar(
    spool_file: &File,
    node_index: &CompactNodeIndex,
    output_path: &Path,
) -> Result<EdgeBuildStats, Box<dyn Error>> {
    emit_edge_sidecar_with_flags(spool_file, None, node_index, output_path, None)
}

/// Like `emit_edge_sidecar`, also writing one flag byte per *emitted* edge
/// (edge order) to `flags_output_path` from the parallel flag spool.
pub fn emit_edge_sidecar_with_flags(
    spool_file: &File,
    flag_spool_file: Option<&File>,
    node_index: &CompactNodeIndex,
    output_path: &Path,
    flags_output_path: Option<&Path>,
) -> Result<EdgeBuildStats, Box<dyn Error>> {
    let mut reader = open_spool_reader(spool_file)?;
    let mut writer = BufWriter::new(File::create(output_path)?);
    let mut flag_reader = flag_spool_file.map(open_spool_reader).transpose()?;
    let mut flag_writer = flags_output_path
        .map(|path| File::create(path).map(BufWriter::new))
        .transpose()?;
    let mut stats = EdgeBuildStats::default();

    while let Some((from_osm, to_osm)) = read_spooled_edge_pair(&mut reader)? {
        let mut flag = [0_u8; 1];
        if let Some(flag_reader) = flag_reader.as_mut() {
            flag_reader.read_exact(&mut flag)?;
        }
        let Some(&from_index) = node_index.by_osm_id.get(&from_osm) else {
            stats.skipped_missing_nodes += 1;
            continue;
        };
        let Some(&to_index) = node_index.by_osm_id.get(&to_osm) else {
            stats.skipped_missing_nodes += 1;
            continue;
        };

        let (from_lat, from_lon) = node_index.coords[from_index as usize];
        let (to_lat, to_lon) = node_index.coords[to_index as usize];
        let length_m = haversine_m(from_lat, from_lon, to_lat, to_lon);
        if !length_m.is_finite() || length_m <= 0.0 {
            stats.skipped_zero_length += 1;
            continue;
        }

        writer.write_all(&from_index.to_le_bytes())?;
        writer.write_all(&to_index.to_le_bytes())?;
        writer.write_all(&length_m.to_le_bytes())?;
        if let Some(flag_writer) = flag_writer.as_mut() {
            flag_writer.write_all(&flag)?;
        }
        stats.emitted_edges += 1;
    }

    writer.flush()?;
    if let Some(flag_writer) = flag_writer.as_mut() {
        flag_writer.flush()?;
    }
    Ok(stats)
}

fn read_edge_record(reader: &mut dyn Read) -> Result<Option<(u32, u32, f32)>, Box<dyn Error>> {
    let mut buffer = [0_u8; EDGE_RECORD_BYTES];
    match reader.read_exact(&mut buffer) {
        Ok(()) => {
            let mut src_bytes = [0_u8; 4];
            let mut dst_bytes = [0_u8; 4];
            let mut length_bytes = [0_u8; 4];
            src_bytes.copy_from_slice(&buffer[..4]);
            dst_bytes.copy_from_slice(&buffer[4..8]);
            length_bytes.copy_from_slice(&buffer[8..]);
            Ok(Some((
                u32::from_le_bytes(src_bytes),
                u32::from_le_bytes(dst_bytes),
                f32::from_le_bytes(length_bytes),
            )))
        }
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn write_u64_sidecar(path: &Path, values: &[u64]) -> Result<(), Box<dyn Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    for value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

fn write_u32_sidecar(path: &Path, values: &[u32]) -> Result<(), Box<dyn Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    for value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

fn write_f32_sidecar(path: &Path, values: &[f32]) -> Result<(), Box<dyn Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    for value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

pub fn emit_adjacency_sidecars(
    edge_sidecar_path: &Path,
    node_count: usize,
    paths: &GraphPaths,
) -> Result<(), Box<dyn Error>> {
    let mut offsets = vec![0_u64; node_count + 1];
    let mut reader = BufReader::new(File::open(edge_sidecar_path)?);
    while let Some((src, _dst, _length_m)) = read_edge_record(&mut reader)? {
        offsets[(src as usize) + 1] += 1;
    }

    for index in 0..node_count {
        offsets[index + 1] += offsets[index];
    }
    let edge_count = offsets[node_count] as usize;
    let mut positions = offsets[..node_count].to_vec();
    let mut targets = vec![0_u32; edge_count];
    let mut lengths = vec![0_f32; edge_count];

    let mut reader = BufReader::new(File::open(edge_sidecar_path)?);
    while let Some((src, dst, length_m)) = read_edge_record(&mut reader)? {
        let slot = positions[src as usize] as usize;
        targets[slot] = dst;
        lengths[slot] = length_m;
        positions[src as usize] += 1;
    }

    write_u64_sidecar(&paths.adjacency_offsets_bin, &offsets)?;
    write_u32_sidecar(&paths.adjacency_targets_bin, &targets)?;
    write_f32_sidecar(&paths.adjacency_lengths_bin, &lengths)?;
    Ok(())
}

/// Reorder per-edge flags (edge order, see `emit_edge_sidecar_with_flags`)
/// into CSR slot order and write `walk_graph.edge_flags.bin`.
pub fn emit_edge_flag_sidecar(
    edge_sidecar_path: &Path,
    edge_flags_by_edge_path: &Path,
    node_count: usize,
    output_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let mut offsets = vec![0_u64; node_count + 1];
    let mut reader = BufReader::new(File::open(edge_sidecar_path)?);
    while let Some((src, _dst, _length_m)) = read_edge_record(&mut reader)? {
        offsets[(src as usize) + 1] += 1;
    }
    for index in 0..node_count {
        offsets[index + 1] += offsets[index];
    }
    let mut positions = offsets[..node_count].to_vec();
    let mut flags = vec![0_u8; offsets[node_count] as usize];

    let mut reader = BufReader::new(File::open(edge_sidecar_path)?);
    let mut flag_reader = BufReader::new(File::open(edge_flags_by_edge_path)?);
    while let Some((src, _dst, _length_m)) = read_edge_record(&mut reader)? {
        let mut flag = [0_u8; 1];
        flag_reader.read_exact(&mut flag)?;
        let slot = positions[src as usize] as usize;
        flags[slot] = flag[0];
        positions[src as usize] += 1;
    }
    std::fs::write(output_path, &flags)?;
    Ok(())
}

/// Undirected component id per node, numbered by each component's lowest
/// node index (node 0 is always in component 0).
pub fn node_components(node_count: usize, edges: impl Iterator<Item = (u32, u32)>) -> Vec<u32> {
    let mut parent: Vec<u32> = (0..node_count as u32).collect();
    fn find(parent: &mut [u32], mut node: u32) -> u32 {
        while parent[node as usize] != node {
            let grand = parent[parent[node as usize] as usize];
            parent[node as usize] = grand;
            node = grand;
        }
        node
    }
    for (a, b) in edges {
        let root_a = find(&mut parent, a);
        let root_b = find(&mut parent, b);
        if root_a != root_b {
            let (low, high) = if root_a < root_b { (root_a, root_b) } else { (root_b, root_a) };
            parent[high as usize] = low;
        }
    }
    let mut label_of_root = vec![u32::MAX; node_count];
    let mut components = vec![0_u32; node_count];
    let mut next = 0_u32;
    for node in 0..node_count as u32 {
        let root = find(&mut parent, node) as usize;
        if label_of_root[root] == u32::MAX {
            label_of_root[root] = next;
            next += 1;
        }
        components[node as usize] = label_of_root[root];
    }
    components
}

/// Write `walk_graph.components.bin` (u32 per node) from `walk_graph.edges.bin`.
pub fn emit_component_sidecar(
    edge_sidecar_path: &Path,
    node_count: usize,
    output_path: &Path,
) -> Result<u32, Box<dyn Error>> {
    let mut edges = Vec::new();
    let mut reader = BufReader::new(File::open(edge_sidecar_path)?);
    while let Some((src, dst, _length_m)) = read_edge_record(&mut reader)? {
        edges.push((src, dst));
    }
    let components = node_components(node_count, edges.into_iter());
    write_u32_sidecar(output_path, &components)?;
    Ok(components.iter().copied().max().map_or(0, |max| max + 1))
}

#[cfg(test)]
mod tests;
