use clap::{Parser, Subcommand};
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;
use walkgraph::graph::{build_compact_node_index, emit_adjacency_sidecars, emit_edge_sidecar};
use walkgraph::pbf::{collect_retained_nodes, parse_bbox, scan_walkable_ways};
use walkgraph::reachability::run_reachability;
use walkgraph::serialize::{
    now_utc_rfc3339, prepare_output_dir, write_meta_json, write_node_sidecars, GraphMeta,
};

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
    }
    Ok(())
}
