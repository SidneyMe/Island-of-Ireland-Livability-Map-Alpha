use super::{
    is_walkable_tags, open_spool_reader, parse_bbox, read_spooled_edge_pair, Bbox,
    PRIVATE_VALUES, WALK_EXCLUDED,
};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn bbox_parser_accepts_valid_input() {
    let bbox = parse_bbox("51.4,-10.5,55.4,-5.3").expect("bbox");
    assert_eq!(
        bbox,
        Bbox {
            min_lat: 51.4,
            min_lon: -10.5,
            max_lat: 55.4,
            max_lon: -5.3,
        }
    );
}

#[test]
fn bbox_parser_trims_whitespace() {
    let bbox = parse_bbox(" 51.4, -10.5, 55.4, -5.3 ").expect("bbox");

    assert_eq!(bbox.min_lat, 51.4);
    assert_eq!(bbox.min_lon, -10.5);
    assert_eq!(bbox.max_lat, 55.4);
    assert_eq!(bbox.max_lon, -5.3);
}

#[test]
fn bbox_parser_rejects_malformed_inputs() {
    let cases = [
        ("1,2,3", "four comma-separated numbers"),
        ("1,2,3,4,5", "exactly four comma-separated numbers"),
        ("north,2,3,4", "latitude values must be numeric"),
        ("2,1,1,3", "minimums must be <= maximums"),
        ("1,3,2,2", "minimums must be <= maximums"),
    ];

    for (input, expected) in cases {
        let err = parse_bbox(input).expect_err("bbox should be rejected");
        assert!(
            err.contains(expected),
            "expected {input:?} error {err:?} to contain {expected:?}"
        );
    }
}

#[test]
fn bbox_contains_is_inclusive_and_expand_pads_outward() {
    let bbox = Bbox {
        min_lat: 51.0,
        min_lon: -10.0,
        max_lat: 55.0,
        max_lon: -5.0,
    };

    assert!(bbox.contains(51.0, -10.0));
    assert!(bbox.contains(55.0, -5.0));
    assert!(!bbox.contains(50.999, -10.0));
    assert_eq!(bbox.expand(0.0), bbox);

    let expanded = bbox.expand(1_000.0);
    assert!(expanded.min_lat < bbox.min_lat);
    assert!(expanded.min_lon < bbox.min_lon);
    assert!(expanded.max_lat > bbox.max_lat);
    assert!(expanded.max_lon > bbox.max_lon);
}

#[test]
fn walkability_matches_expected_sets() {
    assert_eq!(PRIVATE_VALUES, ["private", "no"]);
    assert_eq!(
        WALK_EXCLUDED,
        [
            "construction",
            "motor",
            "motorway",
            "motorway_link",
            "planned",
            "proposed",
            "raceway",
            "trunk",
            "trunk_link",
        ]
    );
    assert!(is_walkable_tags([("highway", "residential")]));
    assert!(!is_walkable_tags([("highway", "motorway")]));
    assert!(!is_walkable_tags([
        ("highway", "footway"),
        ("access", "private")
    ]));
    assert!(!is_walkable_tags([
        ("highway", "residential"),
        ("foot", "no")
    ]));
}

#[test]
fn spool_reader_rewinds_before_reading() {
    let mut spool = NamedTempFile::new().expect("spool");
    spool
        .write_all(&10_i64.to_le_bytes())
        .and_then(|_| spool.write_all(&20_i64.to_le_bytes()))
        .expect("write spool edge");

    let mut reader = open_spool_reader(spool.as_file()).expect("open reader");
    let edge = read_spooled_edge_pair(&mut reader)
        .expect("read edge")
        .expect("edge pair");

    assert_eq!(edge, (10, 20));
}
