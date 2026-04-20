use super::{
    counts_for_origin, load_amenity_weights, load_graph_csr, load_origins, run_reachability,
    AmenityWeights, GraphCsr, MISSING_WEIGHT_ROW,
};
use crate::serialize::{graph_paths_for_dir, write_meta_json, GraphMeta};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};

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

fn write_graph_meta(graph_dir: &Path, node_count: u64, edge_count: u64) {
    let meta = GraphMeta {
        format_version: 3,
        extract_fingerprint: Some("fixture".to_string()),
        pbf_path: "fixture.osm.pbf".to_string(),
        pbf_size: 123,
        pbf_mtime_ns: 456,
        bbox: None,
        bbox_padding_m: 0.0,
        node_count,
        edge_count,
        created_utc: "2026-01-01T00:00:00Z".to_string(),
    };
    write_meta_json(&meta, &graph_dir.join("walk_graph.meta.json")).expect("write graph metadata");
}

fn write_graph_sidecars(
    graph_dir: &Path,
    offsets: &[u64],
    targets: &[u32],
    lengths: &[f32],
    node_count: u64,
    edge_count: u64,
) {
    let paths = graph_paths_for_dir(graph_dir);
    write_graph_meta(graph_dir, node_count, edge_count);
    write_u64s(&paths.adjacency_offsets_bin, offsets);
    write_u32s(&paths.adjacency_targets_bin, targets);
    write_f32s(&paths.adjacency_lengths_bin, lengths);
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

#[test]
fn counts_reachable_amenities_by_category_with_shortest_paths() {
    let graph = GraphCsr {
        node_count: 3,
        edge_count: 3,
        offsets: vec![0, 2, 3, 3],
        targets: vec![1, 2, 2],
        lengths: vec![5.0, 100.0, 5.0],
    };
    let weights = AmenityWeights {
        category_count: 2,
        node_to_row: vec![0, MISSING_WEIGHT_ROW, 1],
        counts_flat: vec![2, 0, 0, 3],
    };

    let counts = counts_for_origin(&graph, &weights, 0, 10.0);

    assert_eq!(counts, vec![2, 3]);
}

#[test]
fn counts_exclude_nodes_beyond_cutoff() {
    let graph = GraphCsr {
        node_count: 3,
        edge_count: 3,
        offsets: vec![0, 2, 3, 3],
        targets: vec![1, 2, 2],
        lengths: vec![5.0, 100.0, 5.0],
    };
    let weights = AmenityWeights {
        category_count: 2,
        node_to_row: vec![0, MISSING_WEIGHT_ROW, 1],
        counts_flat: vec![2, 0, 0, 3],
    };

    let counts = counts_for_origin(&graph, &weights, 0, 9.0);

    assert_eq!(counts, vec![2, 0]);
}

#[test]
fn load_origins_rejects_node_ids_outside_graph() {
    let origins = NamedTempFile::new().expect("origins");
    write_u32s(origins.path(), &[0, 3]);

    let err = load_origins(origins.path(), 3).expect_err("origin id should be rejected");

    assert!(err
        .to_string()
        .contains("origin node id exceeds graph node count"));
}

#[test]
fn load_amenity_weights_rejects_node_ids_outside_graph() {
    let weights = NamedTempFile::new().expect("weights");
    write_u32s(weights.path(), &[3, 1, 2]);

    let err = load_amenity_weights(weights.path(), 2, 3)
        .expect_err("amenity weight node should be rejected");

    assert!(err
        .to_string()
        .contains("amenity weight node id exceeds graph node count"));
}

#[test]
fn load_origins_rejects_invalid_sidecar_byte_length() {
    let origins = NamedTempFile::new().expect("origins");
    File::create(origins.path())
        .expect("create origins")
        .write_all(&[1, 2, 3])
        .expect("write invalid origins");

    let err = load_origins(origins.path(), 10).expect_err("invalid sidecar should fail");

    assert!(err
        .to_string()
        .contains("u32 sidecar has invalid byte length"));
}

#[test]
fn load_graph_csr_rejects_offset_length_mismatch() {
    let graph_dir = TempDir::new().expect("graph dir");
    write_graph_sidecars(graph_dir.path(), &[0, 0], &[], &[], 2, 0);

    let err = load_graph_csr(graph_dir.path()).expect_err("offset mismatch should fail");

    assert!(err
        .to_string()
        .contains("adjacency offsets length mismatch"));
}

#[test]
fn load_graph_csr_rejects_target_or_length_count_mismatch() {
    let graph_dir = TempDir::new().expect("graph dir");
    write_graph_sidecars(graph_dir.path(), &[0, 1], &[0], &[1.0, 2.0], 1, 2);

    let err = load_graph_csr(graph_dir.path()).expect_err("edge mismatch should fail");

    assert!(err.to_string().contains("adjacency edge length mismatch"));
}

#[test]
fn load_graph_csr_rejects_invalid_sidecar_byte_length() {
    let graph_dir = TempDir::new().expect("graph dir");
    let paths = graph_paths_for_dir(graph_dir.path());
    write_graph_meta(graph_dir.path(), 1, 0);
    File::create(&paths.adjacency_offsets_bin)
        .expect("create offsets")
        .write_all(&[1, 2, 3])
        .expect("write invalid offsets");
    write_u32s(&paths.adjacency_targets_bin, &[]);
    write_f32s(&paths.adjacency_lengths_bin, &[]);

    let err = load_graph_csr(graph_dir.path()).expect_err("invalid sidecar should fail");

    assert!(err
        .to_string()
        .contains("u64 sidecar has invalid byte length"));
}

#[test]
fn run_reachability_writes_little_endian_rows_for_multiple_origins() {
    let graph_dir = TempDir::new().expect("graph dir");
    write_graph_sidecars(graph_dir.path(), &[0, 1, 1], &[1], &[5.0], 2, 1);
    let origins = NamedTempFile::new().expect("origins");
    let weights = NamedTempFile::new().expect("weights");
    let output = NamedTempFile::new().expect("output");
    write_u32s(origins.path(), &[0, 1]);
    write_u32s(weights.path(), &[0, 1, 0, 1, 0, 2]);

    run_reachability(
        graph_dir.path(),
        origins.path(),
        weights.path(),
        2,
        10.0,
        "counts",
        &[],
        output.path(),
    )
    .expect("run reachability");

    assert_eq!(read_u32s(output.path()), vec![1, 2, 0, 2]);
}

#[test]
fn run_reachability_writes_decayed_unit_rows() {
    let graph_dir = TempDir::new().expect("graph dir");
    write_graph_sidecars(graph_dir.path(), &[0, 1, 1], &[1], &[150.0], 2, 1);
    let origins = NamedTempFile::new().expect("origins");
    let weights = NamedTempFile::new().expect("weights");
    let output = NamedTempFile::new().expect("output");
    write_u32s(origins.path(), &[0, 1]);
    write_u32s(weights.path(), &[1, 1, 2]);

    run_reachability(
        graph_dir.path(),
        origins.path(),
        weights.path(),
        2,
        500.0,
        "decayed-units",
        &[150.0, 150.0],
        output.path(),
    )
    .expect("run reachability");

    let values = read_f32s(output.path());
    assert_eq!(values.len(), 4);
    assert!((values[0] - 0.5).abs() < 1e-6);
    assert!((values[1] - 1.0).abs() < 1e-6);
    assert!((values[2] - 1.0).abs() < 1e-6);
    assert!((values[3] - 2.0).abs() < 1e-6);
}
