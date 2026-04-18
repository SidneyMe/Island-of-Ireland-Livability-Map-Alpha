use super::{now_utc_rfc3339, prepare_output_dir, write_meta_json, write_node_sidecars, GraphMeta};
use crate::pbf::Bbox;
use std::fs;
use tempfile::TempDir;

#[test]
fn sidecars_write_fixed_width_records() {
    let dir = TempDir::new().expect("tempdir");
    let paths = prepare_output_dir(dir.path()).expect("paths");
    write_node_sidecars(&[(53.0, -6.0), (54.0, -5.0)], &[1, 2], &paths).expect("write");

    let nodes_len = fs::metadata(paths.nodes_bin).expect("nodes metadata").len();
    let osmids_len = fs::metadata(paths.osmids_bin)
        .expect("osmids metadata")
        .len();
    assert_eq!(nodes_len, 16);
    assert_eq!(osmids_len, 16);
}

#[test]
fn node_sidecars_reject_length_mismatch() {
    let dir = TempDir::new().expect("tempdir");
    let paths = prepare_output_dir(dir.path()).expect("paths");

    let err = write_node_sidecars(&[(53.0, -6.0)], &[1, 2], &paths)
        .expect_err("mismatched sidecars should fail");

    assert!(err
        .to_string()
        .contains("coords and osm ids length mismatch"));
}

#[test]
fn metadata_is_serializable() {
    let created_utc = now_utc_rfc3339().expect("timestamp");
    let meta = GraphMeta {
        format_version: 1,
        extract_fingerprint: Some("abc".to_string()),
        pbf_path: "sample.osm.pbf".to_string(),
        pbf_size: 123,
        pbf_mtime_ns: 456,
        bbox: Some(Bbox {
            min_lat: 1.0,
            min_lon: 2.0,
            max_lat: 3.0,
            max_lon: 4.0,
        }),
        bbox_padding_m: 500.0,
        node_count: 2,
        edge_count: 4,
        created_utc,
    };
    let payload = serde_json::to_string(&meta).expect("json");
    assert!(payload.contains("\"format_version\":1"));
}

#[test]
fn metadata_json_writes_and_round_trips() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("walk_graph.meta.json");
    let meta = GraphMeta {
        format_version: 3,
        extract_fingerprint: Some("extract-fp".to_string()),
        pbf_path: "sample.osm.pbf".to_string(),
        pbf_size: 123,
        pbf_mtime_ns: 456,
        bbox: Some(Bbox {
            min_lat: 51.0,
            min_lon: -10.0,
            max_lat: 55.0,
            max_lon: -5.0,
        }),
        bbox_padding_m: 500.0,
        node_count: 2,
        edge_count: 4,
        created_utc: "2026-01-01T00:00:00Z".to_string(),
    };

    write_meta_json(&meta, &path).expect("write metadata");

    let payload = fs::read_to_string(path).expect("read metadata");
    let parsed: GraphMeta = serde_json::from_str(&payload).expect("parse metadata");
    assert_eq!(parsed.format_version, 3);
    assert_eq!(parsed.extract_fingerprint.as_deref(), Some("extract-fp"));
    assert_eq!(parsed.bbox.expect("bbox").min_lon, -10.0);
    assert_eq!(parsed.edge_count, 4);
}
