use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::process::Command;

use tempfile::{NamedTempFile, TempDir};
use walkgraph::serialize::{graph_paths_for_dir, write_meta_json, GraphMeta};

fn walkgraph_bin() -> &'static str {
    env!("CARGO_BIN_EXE_walkgraph")
}

fn write_u32s(path: &Path, values: &[u32]) {
    let mut file = File::create(path).expect("create u32 sidecar");
    for value in values {
        file.write_all(&value.to_le_bytes())
            .expect("write u32 sidecar");
    }
}

fn write_u64s(path: &Path, values: &[u64]) {
    let mut file = File::create(path).expect("create u64 sidecar");
    for value in values {
        file.write_all(&value.to_le_bytes())
            .expect("write u64 sidecar");
    }
}

fn write_f32s(path: &Path, values: &[f32]) {
    let mut file = File::create(path).expect("create f32 sidecar");
    for value in values {
        file.write_all(&value.to_le_bytes())
            .expect("write f32 sidecar");
    }
}

fn read_u32s(path: &Path) -> Vec<u32> {
    let mut bytes = Vec::new();
    File::open(path)
        .expect("open output")
        .read_to_end(&mut bytes)
        .expect("read output");
    bytes
        .chunks_exact(4)
        .map(|chunk| {
            let mut value = [0_u8; 4];
            value.copy_from_slice(chunk);
            u32::from_le_bytes(value)
        })
        .collect()
}

fn read_f32s(path: &Path) -> Vec<f32> {
    let mut bytes = Vec::new();
    File::open(path)
        .expect("open output")
        .read_to_end(&mut bytes)
        .expect("read output");
    bytes
        .chunks_exact(4)
        .map(|chunk| {
            let mut value = [0_u8; 4];
            value.copy_from_slice(chunk);
            f32::from_le_bytes(value)
        })
        .collect()
}

fn write_tiny_graph(graph_dir: &Path) {
    let paths = graph_paths_for_dir(graph_dir);
    let meta = GraphMeta {
        format_version: 3,
        extract_fingerprint: Some("fixture".to_string()),
        pbf_path: "fixture.osm.pbf".to_string(),
        pbf_size: 123,
        pbf_mtime_ns: 456,
        bbox: None,
        bbox_padding_m: 0.0,
        node_count: 2,
        edge_count: 1,
        created_utc: "2026-01-01T00:00:00Z".to_string(),
    };
    write_meta_json(&meta, &paths.meta_json).expect("write metadata");
    write_u64s(&paths.adjacency_offsets_bin, &[0, 1, 1]);
    write_u32s(&paths.adjacency_targets_bin, &[1]);
    write_f32s(&paths.adjacency_lengths_bin, &[5.0]);
}

#[test]
fn reachability_command_writes_expected_output() {
    let graph_dir = TempDir::new().expect("graph dir");
    let origins = NamedTempFile::new().expect("origins");
    let weights = NamedTempFile::new().expect("weights");
    let output = NamedTempFile::new().expect("output");
    write_tiny_graph(graph_dir.path());
    write_u32s(origins.path(), &[0, 1]);
    write_u32s(weights.path(), &[0, 1, 0, 1, 0, 2]);

    let command_output = Command::new(walkgraph_bin())
        .arg("reachability")
        .arg("--graph-dir")
        .arg(graph_dir.path())
        .arg("--origins-bin")
        .arg(origins.path())
        .arg("--amenity-weights-bin")
        .arg(weights.path())
        .arg("--category-count")
        .arg("2")
        .arg("--cutoff-m")
        .arg("10")
        .arg("--output-mode")
        .arg("counts")
        .arg("--out")
        .arg(output.path())
        .output()
        .expect("run walkgraph reachability");

    assert!(
        command_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&command_output.stderr)
    );
    assert_eq!(read_u32s(output.path()), vec![1, 2, 0, 2]);
}

#[test]
fn reachability_command_writes_decayed_unit_output() {
    let graph_dir = TempDir::new().expect("graph dir");
    let origins = NamedTempFile::new().expect("origins");
    let weights = NamedTempFile::new().expect("weights");
    let output = NamedTempFile::new().expect("output");
    write_tiny_graph(graph_dir.path());
    write_u32s(origins.path(), &[0, 1]);
    write_u32s(weights.path(), &[1, 1, 2]);

    let command_output = Command::new(walkgraph_bin())
        .arg("reachability")
        .arg("--graph-dir")
        .arg(graph_dir.path())
        .arg("--origins-bin")
        .arg(origins.path())
        .arg("--amenity-weights-bin")
        .arg(weights.path())
        .arg("--category-count")
        .arg("2")
        .arg("--cutoff-m")
        .arg("500")
        .arg("--output-mode")
        .arg("decayed-units")
        .arg("--half-distances-m")
        .arg("150,150")
        .arg("--out")
        .arg(output.path())
        .output()
        .expect("run walkgraph reachability");

    assert!(
        command_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&command_output.stderr)
    );

    let values = read_f32s(output.path());
    assert_eq!(values.len(), 4);
    let decayed = 0.5_f32.powf(5.0 / 150.0);
    assert!((values[0] - decayed).abs() < 1e-6);
    assert!((values[1] - (2.0 * decayed)).abs() < 1e-6);
    assert!((values[2] - 1.0).abs() < 1e-6);
    assert!((values[3] - 2.0).abs() < 1e-6);
}

#[test]
fn stats_command_rejects_malformed_bbox_before_reading_pbf() {
    let command_output = Command::new(walkgraph_bin())
        .arg("stats")
        .arg("--pbf")
        .arg("missing.osm.pbf")
        .arg("--bbox")
        .arg("55.0,-5.0,51.0,-10.0")
        .output()
        .expect("run walkgraph stats");

    assert!(!command_output.status.success());
    let stderr = String::from_utf8_lossy(&command_output.stderr);
    assert!(
        stderr.contains("bbox minimums must be <= maximums"),
        "stderr: {stderr}"
    );
}

#[test]
fn missing_required_cli_arguments_fail() {
    let command_output = Command::new(walkgraph_bin())
        .arg("reachability")
        .output()
        .expect("run walkgraph reachability");

    assert!(!command_output.status.success());
    let stderr = String::from_utf8_lossy(&command_output.stderr);
    assert!(
        stderr.contains("required") || stderr.contains("Usage"),
        "stderr: {stderr}"
    );
}
