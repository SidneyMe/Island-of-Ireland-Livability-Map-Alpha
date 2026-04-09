use super::haversine_m;

#[test]
fn known_distance_is_reasonable() {
    let distance = haversine_m(53.3498, -6.2603, 54.5973, -5.9301);
    assert!((distance - 140_400.0).abs() < 1_000.0);
}
