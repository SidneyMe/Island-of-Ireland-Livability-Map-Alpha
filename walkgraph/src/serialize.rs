use crate::pbf::Bbox;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct GraphPaths {
    pub nodes_bin: PathBuf,
    pub edges_bin: PathBuf,
    pub osmids_bin: PathBuf,
    pub adjacency_offsets_bin: PathBuf,
    pub adjacency_targets_bin: PathBuf,
    pub adjacency_lengths_bin: PathBuf,
    pub meta_json: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GraphMeta {
    pub format_version: u32,
    pub extract_fingerprint: Option<String>,
    pub pbf_path: String,
    pub pbf_size: u64,
    pub pbf_mtime_ns: i128,
    pub bbox: Option<Bbox>,
    pub bbox_padding_m: f64,
    pub node_count: u64,
    pub edge_count: u64,
    pub created_utc: String,
}

pub fn graph_paths_for_dir(output_dir: &Path) -> GraphPaths {
    let dir = output_dir.to_path_buf();
    GraphPaths {
        nodes_bin: dir.join("walk_graph.nodes.bin"),
        edges_bin: dir.join("walk_graph.edges.bin"),
        osmids_bin: dir.join("walk_graph.osmids.bin"),
        adjacency_offsets_bin: dir.join("walk_graph.adjacency_offsets.bin"),
        adjacency_targets_bin: dir.join("walk_graph.adjacency_targets.bin"),
        adjacency_lengths_bin: dir.join("walk_graph.adjacency_lengths.bin"),
        meta_json: dir.join("walk_graph.meta.json"),
    }
}

pub fn prepare_output_dir(output_dir: &Path) -> Result<GraphPaths, Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;
    Ok(graph_paths_for_dir(output_dir))
}

pub fn write_node_sidecars(
    coords: &[(f32, f32)],
    osm_ids: &[i64],
    paths: &GraphPaths,
) -> Result<(), Box<dyn Error>> {
    if coords.len() != osm_ids.len() {
        return Err("coords and osm ids length mismatch".into());
    }

    let mut node_writer = BufWriter::new(File::create(&paths.nodes_bin)?);
    for (lat, lon) in coords {
        node_writer.write_all(&lat.to_le_bytes())?;
        node_writer.write_all(&lon.to_le_bytes())?;
    }
    node_writer.flush()?;

    let mut osmid_writer = BufWriter::new(File::create(&paths.osmids_bin)?);
    for osm_id in osm_ids {
        osmid_writer.write_all(&osm_id.to_le_bytes())?;
    }
    osmid_writer.flush()?;
    Ok(())
}

pub fn write_meta_json(meta: &GraphMeta, path: &Path) -> Result<(), Box<dyn Error>> {
    let payload = serde_json::to_string_pretty(meta)?;
    fs::write(path, payload)?;
    Ok(())
}

pub fn now_utc_rfc3339() -> Result<String, Box<dyn Error>> {
    Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
}

#[cfg(test)]
mod tests;
