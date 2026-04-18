use clap::{Parser, Subcommand};
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;
use walkgraph::graph::{build_compact_node_index, emit_adjacency_sidecars, emit_edge_sidecar};
use walkgraph::gtfs::run_gtfs_refresh;
use walkgraph::pbf::{collect_retained_nodes, parse_bbox, scan_walkable_ways};
use walkgraph::reachability::run_reachability;
use walkgraph::serialize::{
    now_utc_rfc3339, prepare_output_dir, write_meta_json, write_node_sidecars, GraphMeta,
};
use walkgraph::surface::{run_surface, SurfaceArgs};

const FORMAT_VERSION: u32 = 3;

#[derive(Parser, Debug)]
#[command(name = "walkgraph")]
#[command(about = "Build a compact walk graph from an OSM PBF extract.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Build {
        #[arg(long)]
        pbf: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        bbox: Option<String>,
        #[arg(long, default_value_t = 0.0)]
        bbox_padding_m: f64,
        #[arg(long)]
        extract_fingerprint: Option<String>,
    },
    Reachability {
        #[arg(long)]
        graph_dir: PathBuf,
        #[arg(long)]
        origins_bin: PathBuf,
        #[arg(long)]
        amenity_weights_bin: PathBuf,
        #[arg(long)]
        category_count: usize,
        #[arg(long)]
        cutoff_m: f32,
        #[arg(long)]
        out: PathBuf,
    },
    Stats {
        #[arg(long)]
        pbf: PathBuf,
        #[arg(long)]
        bbox: Option<String>,
        #[arg(long, default_value_t = 0.0)]
        bbox_padding_m: f64,
        #[arg(long)]
        extract_fingerprint: Option<String>,
    },
    /// Build fine surface shard cache in parallel (replaces Python shard loop).
    Surface {
        /// Path to walk_graph.nodes.bin (packed f32 lat/lon pairs, LE).
        #[arg(long)]
        nodes_bin: PathBuf,
        /// Study area as GeoJSON file (coordinates in ITM / EPSG:2157 metres).
        #[arg(long)]
        study_area: PathBuf,
        /// Output directory for shards/ subdirectory and manifest.json.
        #[arg(long)]
        shell_dir: PathBuf,
        /// Opaque hash identifying the surface shell tier (written to manifest).
        #[arg(long)]
        surface_shell_hash: String,
        /// Opaque reach-tier hash (written to manifest).
        #[arg(long)]
        reach_hash: String,
        /// Total walk-graph node count (written to manifest).
        #[arg(long)]
        node_count: u64,
        /// Shard size in metres (default 20000).
        #[arg(long, default_value_t = 20_000)]
        shard_size_m: i64,
        /// Base resolution in metres — cell size (default 50).
        #[arg(long, default_value_t = 50)]
        resolution_m: i64,
        /// Optional JSON file with zoom_breaks, tile_size_px, resolution lists.
        #[arg(long)]
        config_json: Option<PathBuf>,
        /// Number of rayon threads (default: num_cpus).
        #[arg(long)]
        threads: Option<usize>,
    },
    /// Parse GTFS feeds, derive transit reality artifacts, and emit CSV sidecars.
    GtfsRefresh {
        #[arg(long)]
        config_json: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
    },
}

fn build_graph(
    pbf_path: &PathBuf,
    output_dir: &PathBuf,
    bbox: Option<String>,
    bbox_padding_m: f64,
    extract_fingerprint: Option<String>,
    write_output: bool,
) -> Result<GraphMeta, Box<dyn Error>> {
    let started_at = Instant::now();
    let requested_bbox = bbox.as_deref().map(parse_bbox).transpose()?;
    let effective_bbox = requested_bbox.map(|bounds| bounds.expand(bbox_padding_m));

    eprintln!("pass 1/2: scanning walkable ways");
    let pass1 = scan_walkable_ways(pbf_path)?;
    eprintln!(
        "walkable ways: {} | referenced nodes: {} | raw directed edges: {}",
        pass1.walkable_way_count,
        pass1.referenced_node_ids.len(),
        pass1.raw_directed_edge_pairs,
    );

    eprintln!("pass 2/2: collecting retained node coordinates");
    let retained_nodes =
        collect_retained_nodes(pbf_path, &pass1.referenced_node_ids, effective_bbox)?;
    eprintln!("retained nodes: {}", retained_nodes.osm_ids.len());

    let compact_nodes = build_compact_node_index(retained_nodes)?;

    if write_output {
        let paths = prepare_output_dir(output_dir)?;
        write_node_sidecars(&compact_nodes.coords, &compact_nodes.osm_ids, &paths)?;
        let edge_stats =
            emit_edge_sidecar(pass1.edge_spool.as_file(), &compact_nodes, &paths.edges_bin)?;
        emit_adjacency_sidecars(&paths.edges_bin, compact_nodes.osm_ids.len(), &paths)?;

        let metadata = std::fs::metadata(pbf_path)?;
        let modified = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos() as i128;

        let meta = GraphMeta {
            format_version: FORMAT_VERSION,
            extract_fingerprint,
            pbf_path: pbf_path.display().to_string(),
            pbf_size: metadata.len(),
            pbf_mtime_ns: modified,
            bbox: requested_bbox,
            bbox_padding_m,
            node_count: compact_nodes.osm_ids.len() as u64,
            edge_count: edge_stats.emitted_edges,
            created_utc: now_utc_rfc3339()?,
        };
        write_meta_json(&meta, &paths.meta_json)?;
        eprintln!(
            "edges emitted: {} | zero-length skipped: {} | missing-node skipped: {} | elapsed: {:.2?}",
            edge_stats.emitted_edges,
            edge_stats.skipped_zero_length,
            edge_stats.skipped_missing_nodes,
            started_at.elapsed(),
        );
        return Ok(meta);
    }

    let temp_paths = prepare_output_dir(output_dir)?;
    let edge_stats = emit_edge_sidecar(
        pass1.edge_spool.as_file(),
        &compact_nodes,
        &temp_paths.edges_bin,
    )?;
    let metadata = std::fs::metadata(pbf_path)?;
    let modified = metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as i128;

    Ok(GraphMeta {
        format_version: FORMAT_VERSION,
        extract_fingerprint,
        pbf_path: pbf_path.display().to_string(),
        pbf_size: metadata.len(),
        pbf_mtime_ns: modified,
        bbox: requested_bbox,
        bbox_padding_m,
        node_count: compact_nodes.osm_ids.len() as u64,
        edge_count: edge_stats.emitted_edges,
        created_utc: now_utc_rfc3339()?,
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build {
            pbf,
            out,
            bbox,
            bbox_padding_m,
            extract_fingerprint,
        } => {
            build_graph(&pbf, &out, bbox, bbox_padding_m, extract_fingerprint, true)?;
        }
        Commands::Reachability {
            graph_dir,
            origins_bin,
            amenity_weights_bin,
            category_count,
            cutoff_m,
            out,
        } => {
            let started_at = Instant::now();
            eprintln!(
                "reachability: loading graph and inputs | category_count={} | cutoff_m={:.2}",
                category_count, cutoff_m
            );
            run_reachability(
                &graph_dir,
                &origins_bin,
                &amenity_weights_bin,
                category_count,
                cutoff_m,
                &out,
            )?;
            eprintln!(
                "reachability: completed | elapsed: {:.2?}",
                started_at.elapsed()
            );
        }
        Commands::Stats {
            pbf,
            bbox,
            bbox_padding_m,
            extract_fingerprint,
        } => {
            let temp_dir = tempfile::tempdir()?;
            let meta = build_graph(
                &pbf,
                &temp_dir.path().to_path_buf(),
                bbox,
                bbox_padding_m,
                extract_fingerprint,
                false,
            )?;
            println!("nodes: {}", meta.node_count);
            println!("edges: {}", meta.edge_count);
        }
        Commands::Surface {
            nodes_bin,
            study_area,
            shell_dir,
            surface_shell_hash,
            reach_hash,
            node_count,
            shard_size_m,
            resolution_m,
            config_json,
            threads,
        } => {
            run_surface(SurfaceArgs {
                nodes_bin,
                study_area,
                shell_dir,
                surface_shell_hash,
                reach_hash,
                node_count,
                shard_size_m,
                resolution_m,
                config_json,
                threads,
            })
            .map_err(|e| -> Box<dyn Error> { Box::from(e.to_string()) })?;
        }
        Commands::GtfsRefresh {
            config_json,
            out_dir,
        } => {
            run_gtfs_refresh(&config_json, &out_dir)?;
        }
    }
    Ok(())
}
