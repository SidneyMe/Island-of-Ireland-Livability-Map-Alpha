//! Pure-Rust forward projection for EPSG:2157 (Irish Transverse Mercator).
//!
//! Converts WGS84 geographic coordinates (degrees) to ITM easting/northing (metres)
//! using the Ordnance Survey Transverse Mercator series. No PROJ dependency.

// ── GRS80 ellipsoid ──────────────────────────────────────────────────────────
const A: f64 = 6_378_137.0; // semi-major axis (m)
const F: f64 = 1.0 / 298.257_222_101; // flattening
const B: f64 = 6_356_752.314_140_356; // semi-minor axis (m)

// ── ITM projection constants (EPSG:2157) ────────────────────────────────────
const K0: f64 = 0.999_820; // scale factor on central meridian
const LAT0_DEG: f64 = 53.5; // origin latitude (°N)
const LON0_DEG: f64 = -8.0; // central meridian (°E)
const E0: f64 = 600_000.0; // false easting  (m)
const N0: f64 = 750_000.0; // false northing (m)

// ── Derived constants (computed at compile time via lazy evaluation) ─────────
const E2: f64 = 2.0 * F - F * F;
const N: f64 = (A - B) / (A + B);
const N2: f64 = N * N;
const N3: f64 = N2 * N;

/// Meridian arc from latitude of origin `phi0` to latitude `phi` (radians).
fn meridional_arc(phi: f64, phi0: f64) -> f64 {
    let delta = phi - phi0;
    let sigma = phi + phi0;
    let a0 = 1.0 + N + (5.0 / 4.0) * (N2 + N3);
    let a2 = 3.0 * (N + N2) + (21.0 / 8.0) * N3;
    let a4 = (15.0 / 8.0) * (N2 + N3);
    let a6 = (35.0 / 24.0) * N3;

    B * K0
        * (a0 * delta - a2 * delta.sin() * sigma.cos()
            + a4 * (2.0 * delta).sin() * (2.0 * sigma).cos()
            - a6 * (3.0 * delta).sin() * (3.0 * sigma).cos())
}

/// Convert WGS84 degrees to ITM easting/northing (metres).
///
/// Returns `(easting_m, northing_m)`.
pub fn wgs84_to_itm(lat_deg: f64, lon_deg: f64) -> (f64, f64) {
    let phi = lat_deg.to_radians();
    let lam = lon_deg.to_radians();
    let lam0 = LON0_DEG.to_radians();
    let phi0 = LAT0_DEG.to_radians();

    let sin_phi = phi.sin();
    let cos_phi = phi.cos();
    let tan_phi = phi.tan();

    // Radii of curvature
    let nu = A * K0 / (1.0 - E2 * sin_phi * sin_phi).sqrt();
    let rho = A * K0 * (1.0 - E2) / (1.0 - E2 * sin_phi * sin_phi).powf(1.5);
    let eta2 = nu / rho - 1.0;

    let dl = lam - lam0;
    let dl2 = dl * dl;
    let dl3 = dl2 * dl;
    let dl4 = dl3 * dl;
    let dl5 = dl4 * dl;
    let dl6 = dl5 * dl;

    let tan2 = tan_phi * tan_phi;
    let tan4 = tan2 * tan2;

    let m = meridional_arc(phi, phi0);
    let i = N0 + m;
    let ii = nu / 2.0 * sin_phi * cos_phi;
    let iii = nu / 24.0 * sin_phi * cos_phi.powi(3) * (5.0 - tan2 + 9.0 * eta2);
    let iii_a = nu / 720.0 * sin_phi * cos_phi.powi(5) * (61.0 - 58.0 * tan2 + tan4);
    let iv = nu * cos_phi;
    let v = nu / 6.0 * cos_phi.powi(3) * (nu / rho - tan2);
    let vi = nu / 120.0
        * cos_phi.powi(5)
        * (5.0 - 18.0 * tan2 + tan4 + 14.0 * eta2 - 58.0 * tan2 * eta2);

    let northing = i + ii * dl2 + iii * dl4 + iii_a * dl6;
    let easting = E0 + iv * dl + v * dl3 + vi * dl5;

    (easting, northing)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cross-check against pyproj reference values for several Irish locations.
    /// Tolerance: 0.5 m (well within the grid resolution of 50 m).
    #[test]
    fn test_known_points() {
        let cases: &[(&str, f64, f64, f64, f64)] = &[
            // (name, lat, lon, expected_easting, expected_northing) from pyproj
            ("Dublin", 53.3498, -6.2603, 715_826.507, 734_697.593),
            ("Cork", 51.8985, -8.4756, 567_268.896, 571_923.132),
            ("Belfast", 54.5973, -5.9301, 733_751.158, 874_083.608),
            ("Galway", 53.2707, -9.0568, 529_507.764, 725_006.054),
            ("Sligo", 54.2766, -8.4694, 569_428.760, 836_523.952),
            ("Wexford", 52.3369, -6.4575, 705_116.614, 621_708.261),
        ];
        for &(name, lat, lon, exp_e, exp_n) in cases {
            let (e, n) = wgs84_to_itm(lat, lon);
            assert!(
                (e - exp_e).abs() < 0.5,
                "easting mismatch for {name} ({lat},{lon}): got {e:.3} expected {exp_e:.3}"
            );
            assert!(
                (n - exp_n).abs() < 0.5,
                "northing mismatch for {name} ({lat},{lon}): got {n:.3} expected {exp_n:.3}"
            );
        }
    }
}
