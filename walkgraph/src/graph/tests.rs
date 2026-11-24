use super::{build_compact_node_index, emit_adjacency_sidecars, emit_edge_sidecar};
use crate::pbf::RetainedNodes;
use crate::serialize::prepare_output_dir;
use std::fs::{self, File};
use std::io::{Read, Write};
use tempfile::{NamedTempFile, TempDir};

#[test]
fn compacts_nodes_into_u32_indexes() {
    let index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![10, 20],
        coords: vec![(53.0, -6.0), (53.1, -6.1)],
    })
    .expect("compact index");

    assert_eq!(index.by_osm_id[&10], 0);
    assert_eq!(index.by_osm_id[&20], 1);
}

#[test]
fn compact_node_index_rejects_length_mismatch() {
    let err = build_compact_node_index(RetainedNodes {
        osm_ids: vec![10, 20],
        coords: vec![(53.0, -6.0)],
    })
    .expect_err("mismatched retained nodes should fail");

    assert!(err
        .to_string()
        .contains("retained node ids and coords length mismatch"));
}

#[test]
fn edge_sidecar_uses_expected_layout() {
    let mut spool = NamedTempFile::new().expect("spool");
    spool
        .write_all(&10_i64.to_le_bytes())
        .and_then(|_| spool.write_all(&20_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&20_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&10_i64.to_le_bytes()))
        .expect("spool bytes");

    let node_index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![10, 20],
        coords: vec![(53.0, -6.0), (53.001, -6.001)],
    })
    .expect("compact index");
    let output = NamedTempFile::new().expect("output");

    let stats = emit_edge_sidecar(
        &File::open(spool.path()).expect("spool open"),
        &node_index,
        output.path(),
    )
    .expect("emit");

    let mut bytes = Vec::new();
    File::open(output.path())
        .expect("edge open")
        .read_to_end(&mut bytes)
        .expect("read");

    assert_eq!(stats.emitted_edges, 2);
    assert_eq!(bytes.len(), 24);
}

#[test]
fn edge_sidecar_counts_missing_and_zero_length_edges() {
    let mut spool = NamedTempFile::new().expect("spool");
    spool
        .write_all(&10_i64.to_le_bytes())
        .and_then(|_| spool.write_all(&20_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&10_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&30_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&10_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&10_i64.to_le_bytes()))
        .expect("spool bytes");

    let node_index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![10, 20],
        coords: vec![(53.0, -6.0), (53.001, -6.001)],
    })
    .expect("compact index");
    let output = NamedTempFile::new().expect("output");

    let stats = emit_edge_sidecar(
        &File::open(spool.path()).expect("spool open"),
        &node_index,
        output.path(),
    )
    .expect("emit");

    assert_eq!(stats.emitted_edges, 1);
    assert_eq!(stats.skipped_missing_nodes, 1);
    assert_eq!(stats.skipped_zero_length, 1);
    assert_eq!(
        fs::metadata(output.path()).expect("edge metadata").len(),
        12
    );
}

#[test]
fn adjacency_sidecars_match_edge_sidecar() {
    let mut spool = NamedTempFile::new().expect("spool");
    spool
        .write_all(&10_i64.to_le_bytes())
        .and_then(|_| spool.write_all(&20_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&20_i64.to_le_bytes()))
        .and_then(|_| spool.write_all(&10_i64.to_le_bytes()))
        .expect("spool bytes");

    let node_index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![10, 20],
        coords: vec![(53.0, -6.0), (53.001, -6.001)],
    })
    .expect("compact index");
    let temp_dir = TempDir::new().expect("tempdir");
    let paths = prepare_output_dir(temp_dir.path()).expect("paths");

    emit_edge_sidecar(
        &File::open(spool.path()).expect("spool open"),
        &node_index,
        &paths.edges_bin,
    )
    .expect("emit edges");
    emit_adjacency_sidecars(&paths.edges_bin, node_index.osm_ids.len(), &paths)
        .expect("emit adjacency");

    let offsets_len = fs::metadata(&paths.adjacency_offsets_bin)
        .expect("offset metadata")
        .len();
    let targets_len = fs::metadata(&paths.adjacency_targets_bin)
        .expect("targets metadata")
        .len();
    let lengths_len = fs::metadata(&paths.adjacency_lengths_bin)
        .expect("lengths metadata")
        .len();
    assert_eq!(offsets_len, 24);
    assert_eq!(targets_len, 8);
    assert_eq!(lengths_len, 8);
}

#[test]
fn components_are_numbered_by_lowest_node() {
    let components = super::node_components(6, [(4, 5), (1, 2), (2, 0)].into_iter());
    assert_eq!(components, vec![0, 0, 0, 1, 2, 2]);
}

#[test]
fn edge_flags_follow_csr_slot_order() {
    let index = build_compact_node_index(RetainedNodes {
        osm_ids: vec![10, 20, 30],
        coords: vec![(53.0, -6.0), (53.001, -6.0), (53.002, -6.0)],
    })
    .expect("compact index");
    // Spooled directed pairs: 20->10 (flag 1), 10->20 (flag 1), 10->30 (flag 0).
    let mut spool = NamedTempFile::new().expect("spool");
    let mut flags = NamedTempFile::new().expect("flags");
    for (from, to, flag) in [(20_i64, 10_i64, 1_u8), (10, 20, 1), (10, 30, 0)] {
        spool.write_all(&from.to_le_bytes()).unwrap();
        spool.write_all(&to.to_le_bytes()).unwrap();
        flags.write_all(&[flag]).unwrap();
    }
    spool.flush().unwrap();
    flags.flush().unwrap();
    let dir = TempDir::new().expect("dir");
    let paths = prepare_output_dir(dir.path()).expect("paths");
    let by_edge = dir.path().join("flags_by_edge.tmp");
    super::emit_edge_sidecar_with_flags(spool.as_file(), Some(flags.as_file()), &index, &paths.edges_bin, Some(&by_edge))
        .expect("edges");
    emit_adjacency_sidecars(&paths.edges_bin, 3, &paths).expect("adjacency");
    super::emit_edge_flag_sidecar(&paths.edges_bin, &by_edge, 3, &paths.edge_flags_bin).expect("flags");
    // CSR: node 0 -> [1 (flag 1), 2 (flag 0)], node 1 -> [0 (flag 1)].
    assert_eq!(fs::read(&paths.edge_flags_bin).unwrap(), vec![1, 0, 1]);
    let count = super::emit_component_sidecar(&paths.edges_bin, 3, &paths.components_bin).expect("components");
    assert_eq!(count, 1);
}
