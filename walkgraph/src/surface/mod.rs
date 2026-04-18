//! `walkgraph surface` subcommand — parallel surface shard builder.
//!
//! Replaces the Python `ensure_surface_shell_cache` loop with a rayon-parallel
//! implementation that is ~10–20× faster and supports resume (skips existing shards).

pub mod itm;
pub mod kdtree;
pub mod npz;
pub mod shard;

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use geo::{Geometry, MultiPolygon, Polygon};
use geojson::GeoJson;
use rayon::prelude::*;
use serde_json::{json, Value};

use self::kdtree::NodeKdTree;
use self::npz::validate_shell_npz;
use self::shard::{process_shard, PolyIndex, ShardEntry};

// ── CLI args (used by main.rs) ───────────────────────────────────────────────

#[derive(Debug)]
pub struct SurfaceArgs {
    pub nodes_bin: PathBuf,
    pub study_area: PathBuf,
    pub shell_dir: PathBuf,
    pub surface_shell_hash: String,
    pub reach_hash: String,
    pub node_count: u64,
    pub shard_size_m: i64,
    pub resolution_m: i64,
    pub config_json: Option<PathBuf>,
    pub threads: Option<usize>,
}

// ── Geometry loading ─────────────────────────────────────────────────────────

fn geojson_geom_to_geo(
    gj_geom: geojson::Geometry,
) -> Result<Geometry<f64>, Box<dyn Error + Send + Sync>> {
    use geojson::Value as GjVal;
    fn coords1(c: Vec<Vec<f64>>) -> Vec<geo::Coord<f64>> {
        c.into_iter()
            .map(|v| geo::coord! { x: v[0], y: v[1] })
            .collect()
    }
    fn ring(c: Vec<Vec<f64>>) -> geo::LineString<f64> {
        geo::LineString(coords1(c))
    }
    fn polygon(rings: Vec<Vec<Vec<f64>>>) -> Polygon<f64> {
        let mut iter = rings.into_iter();
        let exterior = ring(iter.next().unwrap_or_default());
        let interiors: Vec<geo::LineString<f64>> = iter.map(ring).collect();
        Polygon::new(exterior, interiors)
    }
    match gj_geom.value {
        GjVal::Polygon(rings) => Ok(Geometry::Polygon(polygon(rings))),
        GjVal::MultiPolygon(mps) => Ok(Geometry::MultiPolygon(MultiPolygon::new(
            mps.into_iter().map(polygon).collect(),
        ))),
        GjVal::GeometryCollection(geoms) => {
            let mut polys: Vec<Polygon<f64>> = Vec::new();
            for g in geoms {
                match geojson_geom_to_geo(g)? {
                    Geometry::Polygon(p) => polys.push(p),
                    Geometry::MultiPolygon(mp) => polys.extend(mp.0),
                    _ => {}
                }
            }
            Ok(Geometry::MultiPolygon(MultiPolygon::new(polys)))
        }
        other => Err(format!("Unsupported GeoJSON geometry: {}", other.type_name()).into()),
    }
}

fn load_study_area(path: &Path) -> Result<MultiPolygon<f64>, Box<dyn Error + Send + Sync>> {
    let text = fs::read_to_string(path)?;
    let geojson: GeoJson = text.parse()?;

    let geom = match geojson {
        GeoJson::Feature(f) => {
            let gj_geom = f.geometry.ok_or("GeoJSON Feature has no geometry")?;
            geojson_geom_to_geo(gj_geom)?
        }
        GeoJson::Geometry(g) => geojson_geom_to_geo(g)?,
        GeoJson::FeatureCollection(fc) => {
            let first = fc
                .features
                .into_iter()
                .next()
                .ok_or("Empty FeatureCollection")?;
            let gj_geom = first.geometry.ok_or("GeoJSON Feature has no geometry")?;
            geojson_geom_to_geo(gj_geom)?
        }
    };

    match geom {
        Geometry::MultiPolygon(mp) => Ok(mp),
        Geometry::Polygon(p) => Ok(MultiPolygon::new(vec![p])),
        other => Err(format!(
            "Unexpected geometry type after conversion: {:?}",
            std::mem::discriminant(&other)
        )
        .into()),
    }
}

// ── Shard enumeration (mirrors Python's iter_shard_entries exactly) ──────────

fn floor_to(v: i64, step: i64) -> i64 {
    (v as f64 / step as f64).floor() as i64 * step
}

fn ceil_to(v: i64, step: i64) -> i64 {
    (v as f64 / step as f64).ceil() as i64 * step
}

fn enumerate_shards(
    study_mp: &MultiPolygon<f64>,
    shard_size_m: i64,
    resolution_m: i64,
) -> Vec<ShardEntry> {
    use geo::{BoundingRect, Intersects};

    let bbox = match study_mp.bounding_rect() {
        Some(b) => b,
        None => return vec![],
    };

    let x_start = floor_to(bbox.min().x as i64, shard_size_m);
    let y_start = floor_to(bbox.min().y as i64, shard_size_m);
    let x_end = ceil_to(bbox.max().x as i64, shard_size_m);
    let y_end = ceil_to(bbox.max().y as i64, shard_size_m);
    let cells = (shard_size_m / resolution_m) as usize;

    let mut entries = Vec::new();

    let mut y = y_start;
    while y < y_end {
        let mut x = x_start;
        while x < x_end {
            let shard_box = geo::Rect::new(
                geo::coord! { x: x as f64, y: y as f64 },
                geo::coord! { x: (x + shard_size_m) as f64, y: (y + shard_size_m) as f64 },
            )
            .to_polygon();
            if study_mp.intersects(&shard_box) {
                entries.push(ShardEntry {
                    shard_id: format!("{}_{}", x, y),
                    x_min_m: x,
                    y_min_m: y,
                    shard_size_m,
                    cells_per_side: cells,
                    resolution_m,
                });
            }
            x += shard_size_m;
        }
        y += shard_size_m;
    }

    entries
}

// ── Manifest helpers ─────────────────────────────────────────────────────────

fn read_config_json(path: &Path) -> Result<Value, Box<dyn Error + Send + Sync>> {
    let text = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&text)?)
}

fn manifest_inventory(shard_entries: &[ShardEntry]) -> Vec<Value> {
    shard_entries
        .iter()
        .map(|e| {
            json!({
                "shard_id": e.shard_id.clone(),
                "x_min_m": e.x_min_m,
                "y_min_m": e.y_min_m,
                "x_max_m": e.x_max_m(),
                "y_max_m": e.y_max_m(),
                "rows": e.cells_per_side,
                "cols": e.cells_per_side,
                "path": format!("shards/{}.npz", e.shard_id),
            })
        })
        .collect()
}

fn build_manifest(
    status: &str,
    args: &SurfaceArgs,
    config: &Value,
    inventory: &[Value],
    completed_shards: usize,
    total_shards: usize,
) -> Value {
    let default_zoom_breaks: Value = json!([
        [18, 50],
        [16, 100],
        [15, 250],
        [14, 500],
        [13, 1000],
        [12, 2500],
        [10, 5000],
        [8, 10000],
        [0, 20000]
    ]);
    let default_coarse: Value = json!([5000, 10000, 20000]);
    let default_fine: Value = json!([50, 100, 250, 500, 1000, 2500]);

    json!({
        "status": status,
        "schema_version": 1,
        "surface_shell_hash": args.surface_shell_hash,
        "reach_hash": args.reach_hash,
        "base_resolution_m": args.resolution_m,
        "coarse_vector_resolutions_m": config.get("coarse_vector_resolutions_m").unwrap_or(&default_coarse),
        "fine_resolutions_m": config.get("fine_resolutions_m").unwrap_or(&default_fine),
        "surface_zoom_breaks": config.get("surface_zoom_breaks").unwrap_or(&default_zoom_breaks),
        "shard_size_m": args.shard_size_m,
        "tile_size_px": config.get("tile_size_px").unwrap_or(&json!(256)),
        "node_count": args.node_count,
        "completed_shards": completed_shards,
        "total_shards": total_shards,
        "shard_inventory": inventory,
    })
}

fn write_manifest(shell_dir: &Path, manifest: &Value) -> Result<(), Box<dyn Error + Send + Sync>> {
    let tmp = shell_dir.join("manifest.json.tmp");
    fs::write(&tmp, serde_json::to_string_pretty(manifest)?)?;
    fs::rename(&tmp, shell_dir.join("manifest.json"))?;
    Ok(())
}

fn auto_thread_count_from_cpu_info(logical_cpus: usize, physical_cpus: usize) -> usize {
    let baseline = if physical_cpus > 0 {
        physical_cpus
    } else {
        (logical_cpus / 2).max(1)
    };
    baseline.clamp(2, 6).min(logical_cpus)
}

fn auto_thread_count() -> usize {
    auto_thread_count_from_cpu_info(num_cpus::get().max(1), num_cpus::get_physical())
}

fn selected_thread_count(requested_threads: Option<usize>) -> (usize, String) {
    match requested_threads {
        Some(requested) => (requested.max(1), "explicit override".to_string()),
        None => {
            let logical_cpus = num_cpus::get().max(1);
            let physical_cpus = num_cpus::get_physical();
            (
                auto_thread_count(),
                format!(
                    "auto (physical_cpus={}, logical_cpus={})",
                    physical_cpus, logical_cpus
                ),
            )
        }
    }
}

const MANIFEST_WRITE_INTERVAL: usize = 8;

// ── Main entry point ─────────────────────────────────────────────────────────

pub fn run_surface(args: SurfaceArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (num_threads, thread_mode) = selected_thread_count(args.threads);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap_or(());

    eprintln!(
        "surface: threads={} ({}) shard_size={}m resolution={}m",
        num_threads, thread_mode, args.shard_size_m, args.resolution_m
    );

    let config: Value = match &args.config_json {
        Some(p) => read_config_json(p)?,
        None => json!({}),
    };

    eprintln!("surface: loading study area geometry ...");
    let study_mp = load_study_area(&args.study_area)?;
    eprintln!(
        "surface: study area loaded ({} polygon(s))",
        study_mp.0.len()
    );

    eprintln!(
        "surface: building KD-tree from {} ...",
        args.nodes_bin.display()
    );
    let kd = Arc::new(NodeKdTree::build(&args.nodes_bin)?);
    eprintln!("surface: KD-tree ready ({} nodes)", kd.len());

    let shard_entries = enumerate_shards(&study_mp, args.shard_size_m, args.resolution_m);
    let total = shard_entries.len();
    let manifest_inventory = Arc::new(manifest_inventory(&shard_entries));
    eprintln!("surface: {} shard(s) in scope", total);

    eprintln!(
        "surface: building polygon index ({} polygons) ...",
        study_mp.0.len()
    );
    let poly_index = Arc::new(PolyIndex::build(&study_mp));
    eprintln!("surface: polygon index ready");

    fs::create_dir_all(&args.shell_dir)?;
    let shards_dir = args.shell_dir.join("shards");
    fs::create_dir_all(&shards_dir)?;

    let mut shards_to_build = Vec::new();
    let mut reusable_shards = 0usize;
    let mut invalid_shards = 0usize;
    for entry in &shard_entries {
        let out_path = shards_dir.join(format!("{}.npz", entry.shard_id));
        if !out_path.exists() {
            shards_to_build.push(entry);
            continue;
        }
        let x_min_i32 = i32::try_from(entry.x_min_m)
            .expect("shard x_min_m exceeds i32 range");
        let y_min_i32 = i32::try_from(entry.y_min_m)
            .expect("shard y_min_m exceeds i32 range");
        let is_valid = validate_shell_npz(
            &out_path,
            entry.cells_per_side,
            entry.cells_per_side,
            x_min_i32,
            y_min_i32,
        );
        if is_valid {
            reusable_shards += 1;
            continue;
        }
        invalid_shards += 1;
        fs::remove_file(&out_path)?;
        shards_to_build.push(entry);
    }

    if reusable_shards > 0 {
        eprintln!(
            "surface: reusing {}/{} validated shard(s)",
            reusable_shards, total
        );
    }
    if invalid_shards > 0 {
        eprintln!(
            "surface: rebuilding {} invalid or partial shard(s)",
            invalid_shards
        );
    }
    eprintln!(
        "surface: {} shard(s) need building in this run",
        shards_to_build.len()
    );

    let building_manifest = build_manifest(
        "building",
        &args,
        &config,
        manifest_inventory.as_slice(),
        reusable_shards,
        total,
    );
    write_manifest(&args.shell_dir, &building_manifest)?;

    if shards_to_build.is_empty() {
        let complete_manifest = build_manifest(
            "complete",
            &args,
            &config,
            manifest_inventory.as_slice(),
            total,
            total,
        );
        write_manifest(&args.shell_dir, &complete_manifest)?;
        eprintln!("surface: all {} shards complete", total);
        return Ok(());
    }

    let counter = Arc::new(AtomicUsize::new(reusable_shards));
    let last_manifest_write = Arc::new(Mutex::new(reusable_shards));

    shards_to_build.par_iter().try_for_each(|entry| {
        let out_path = shards_dir.join(format!("{}.npz", entry.shard_id));
        process_shard(entry, &poly_index, &kd, &out_path)?;

        let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
        eprintln!("surface: shard {} done ({}/{})", entry.shard_id, n, total);
        if n == total || n.saturating_sub(reusable_shards) % MANIFEST_WRITE_INTERVAL == 0 {
            let mut last_written = match last_manifest_write.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if n > *last_written
                && (n == total || n.saturating_sub(*last_written) >= MANIFEST_WRITE_INTERVAL)
            {
                let progress_manifest = build_manifest(
                    "building",
                    &args,
                    &config,
                    manifest_inventory.as_slice(),
                    n,
                    total,
                );
                write_manifest(&args.shell_dir, &progress_manifest)?;
                *last_written = n;
            }
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })?;

    let complete_manifest = build_manifest(
        "complete",
        &args,
        &config,
        manifest_inventory.as_slice(),
        total,
        total,
    );
    write_manifest(&args.shell_dir, &complete_manifest)?;

    eprintln!("surface: all {} shards complete", total);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tempfile::tempdir;

    fn write_nodes_bin(path: &Path, coords: &[(f64, f64)]) {
        let mut bytes = Vec::with_capacity(coords.len() * 8);
        for &(lat, lon) in coords {
            bytes.extend_from_slice(&(lat as f32).to_le_bytes());
            bytes.extend_from_slice(&(lon as f32).to_le_bytes());
        }
        fs::write(path, bytes).unwrap();
    }

    fn write_study_area(path: &Path, min_x: f64, min_y: f64, max_x: f64, max_y: f64) {
        let payload = json!({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [min_x, min_y],
                    [max_x, min_y],
                    [max_x, max_y],
                    [min_x, max_y],
                    [min_x, min_y]
                ]]
            },
            "properties": {}
        });
        fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
    }

    fn surface_args(nodes_bin: &Path, study_area: &Path, shell_dir: &Path) -> SurfaceArgs {
        SurfaceArgs {
            nodes_bin: nodes_bin.to_path_buf(),
            study_area: study_area.to_path_buf(),
            shell_dir: shell_dir.to_path_buf(),
            surface_shell_hash: "surface-shell-test".to_string(),
            reach_hash: "reach-test".to_string(),
            node_count: 2,
            shard_size_m: 100,
            resolution_m: 50,
            config_json: None,
            threads: Some(1),
        }
    }

    fn single_shard_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf, String) {
        let dir = tempdir().unwrap();
        let nodes_bin = dir.path().join("nodes.bin");
        let study_area = dir.path().join("study_area.geojson");
        let shell_dir = dir.path().join("shell");
        let (easting, northing) = itm::wgs84_to_itm(53.3498, -6.2603);
        let shard_x = ((easting / 100.0).floor() as i64) * 100;
        let shard_y = ((northing / 100.0).floor() as i64) * 100;
        let shard_id = format!("{}_{}", shard_x, shard_y);

        write_nodes_bin(&nodes_bin, &[(53.3498, -6.2603), (53.3500, -6.2590)]);
        write_study_area(
            &study_area,
            shard_x as f64 + 5.0,
            shard_y as f64 + 5.0,
            shard_x as f64 + 95.0,
            shard_y as f64 + 95.0,
        );
        (dir, nodes_bin, study_area, shell_dir, shard_id)
    }

    #[test]
    fn auto_thread_count_caps_and_prefers_physical_cores() {
        assert_eq!(auto_thread_count_from_cpu_info(16, 8), 6);
        assert_eq!(auto_thread_count_from_cpu_info(16, 4), 4);
        assert_eq!(auto_thread_count_from_cpu_info(16, 0), 6);
        assert_eq!(auto_thread_count_from_cpu_info(2, 1), 2);
        assert_eq!(auto_thread_count_from_cpu_info(1, 1), 1);
        assert_eq!(selected_thread_count(Some(0)).0, 1);
    }

    #[test]
    fn building_manifest_tracks_total_and_completed_shards() {
        let args = SurfaceArgs {
            nodes_bin: PathBuf::from("nodes.bin"),
            study_area: PathBuf::from("study_area.geojson"),
            shell_dir: PathBuf::from("shell"),
            surface_shell_hash: "shell-hash".to_string(),
            reach_hash: "reach-hash".to_string(),
            node_count: 10,
            shard_size_m: 100,
            resolution_m: 50,
            config_json: None,
            threads: None,
        };
        let entries = vec![
            ShardEntry {
                shard_id: "0_0".to_string(),
                x_min_m: 0,
                y_min_m: 0,
                shard_size_m: 100,
                cells_per_side: 2,
                resolution_m: 50,
            },
            ShardEntry {
                shard_id: "100_0".to_string(),
                x_min_m: 100,
                y_min_m: 0,
                shard_size_m: 100,
                cells_per_side: 2,
                resolution_m: 50,
            },
        ];
        let inventory = manifest_inventory(&entries);
        let manifest = build_manifest("building", &args, &json!({}), &inventory, 1, 2);

        assert_eq!(manifest["status"], "building");
        assert_eq!(manifest["completed_shards"], 1);
        assert_eq!(manifest["total_shards"], 2);
        assert_eq!(manifest["shard_inventory"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn run_surface_reuses_valid_existing_shards() {
        let (_dir, nodes_bin, study_area, shell_dir, shard_id) = single_shard_fixture();
        run_surface(surface_args(&nodes_bin, &study_area, &shell_dir)).unwrap();

        let shard_path = shell_dir.join("shards").join(format!("{}.npz", shard_id));
        let modified_before = fs::metadata(&shard_path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(1100));

        run_surface(surface_args(&nodes_bin, &study_area, &shell_dir)).unwrap();

        let modified_after = fs::metadata(&shard_path).unwrap().modified().unwrap();
        assert_eq!(modified_before, modified_after);

        let manifest: Value =
            serde_json::from_str(&fs::read_to_string(shell_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["status"], "complete");
        assert_eq!(manifest["completed_shards"], 1);
        assert_eq!(manifest["total_shards"], 1);
    }

    #[test]
    fn run_surface_rebuilds_invalid_existing_shards() {
        let (_dir, nodes_bin, study_area, shell_dir, shard_id) = single_shard_fixture();
        run_surface(surface_args(&nodes_bin, &study_area, &shell_dir)).unwrap();

        let shard_path = shell_dir.join("shards").join(format!("{}.npz", shard_id));
        fs::write(&shard_path, b"bad").unwrap();
        std::thread::sleep(Duration::from_millis(1100));

        run_surface(surface_args(&nodes_bin, &study_area, &shell_dir)).unwrap();

        let rebuilt = fs::read(&shard_path).unwrap();
        assert!(rebuilt.len() > 3);
        assert!(validate_shell_npz(&shard_path, 2, 2, 715_800, 734_600));
    }
}
