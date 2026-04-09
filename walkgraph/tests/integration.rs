use std::fs;
use std::io::Write;

use tempfile::NamedTempFile;
use tempfile::TempDir;
use walkgraph::graph::{build_compact_node_index, emit_edge_sidecar};
use walkgraph::haversine::haversine_m;
use walkgraph::pbf::{is_walkable_tags, parse_bbox, RetainedNodes};
use walkgraph::serialize::{prepare_output_dir, write_node_sidecars};

#[test]
fn test_haversine_known_distance() {
    let distance = haversine_m(53.3498, -6.2603, 54.5973, -5.9301);
    assert!((distance - 140_400.0).abs() < 1_000.0);
}

#[test]
fn test_walkability_filter() {
    assert!(is_walkable_tags([("highway", "residential")]));
    assert!(!is_walkable_tags([("highway", "motorway")]));
    assert!(!is_walkable_tags([
        ("highway", "residential"),
        ("access", "private")
    ]));
}

#[test]
fn test_graph_bidirectional() {
    let mut spool = NamedTempFile::new().expect("spool");
    spool
        .write_all(&1_i64.to_le_bytes())
        .and_then(|_| spool.write_all(&2_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&2_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&1_i64.to_le_bytes()))
        .expect("spool write");

    let node_index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![1, 2],
        coords: vec![(53.0, -6.0), (53.001, -6.001)],
    })
    .expect("index");
    let output = NamedTempFile::new().expect("output");

    let stats = emit_edge_sidecar(
        &fs::File::open(spool.path()).expect("spool open"),
        &node_index,
        output.path(),
    )
    .expect("emit");

    assert_eq!(stats.emitted_edges, 2);
}

#[test]
fn test_zero_length_segments_skipped() {
    let mut spool = NamedTempFile::new().expect("spool");
    spool
        .write_all(&1_i64.to_le_bytes())
        .and_then(|_| spool.write_all(&2_i64.to_le_bytes()))
        .expect("spool write");

    let node_index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![1, 2],
        coords: vec![(53.0, -6.0), (53.0, -6.0)],
    })
    .expect("index");
    let output = NamedTempFile::new().expect("output");

    let stats = emit_edge_sidecar(
        &fs::File::open(spool.path()).expect("spool open"),
        &node_index,
        output.path(),
    )
    .expect("emit");

    assert_eq!(stats.emitted_edges, 0);
    assert_eq!(stats.skipped_zero_length, 1);
}

#[test]
fn test_sidecar_layout_and_bbox_parse() {
    let dir = TempDir::new().expect("tempdir");
    let paths = prepare_output_dir(dir.path()).expect("paths");
    write_node_sidecars(&[(53.0, -6.0)], &[100], &paths).expect("write");

    assert_eq!(
        parse_bbox("51.4,-10.5,55.4,-5.3").expect("bbox").min_lat,
        51.4
    );
    assert_eq!(
        fs::metadata(paths.nodes_bin).expect("nodes metadata").len(),
        8
    );
    assert_eq!(
        fs::metadata(paths.osmids_bin)
            .expect("osmids metadata")
            .len(),
        8
    );
}
