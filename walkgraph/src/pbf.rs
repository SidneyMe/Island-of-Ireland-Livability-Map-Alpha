use osmpbf::{Element, ElementReader, IndexedReader};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use tempfile::NamedTempFile;

pub const PRIVATE_VALUES: [&str; 2] = ["private", "no"];
pub const WALK_EXCLUDED: [&str; 9] = [
    "construction",
    "motor",
    "motorway",
    "motorway_link",
    "planned",
    "proposed",
    "raceway",
    "trunk",
    "trunk_link",
];

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Bbox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl Bbox {
    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        lat >= self.min_lat && lat <= self.max_lat && lon >= self.min_lon && lon <= self.max_lon
    }

    pub fn expand(&self, padding_m: f64) -> Self {
        if padding_m <= 0.0 {
            return *self;
        }
        let lat_padding_deg = padding_m / 111_320.0;
        let mid_lat = ((self.min_lat + self.max_lat) * 0.5).to_radians();
        let lon_scale = mid_lat.cos().abs().max(0.01);
        let lon_padding_deg = padding_m / (111_320.0 * lon_scale);
        Self {
            min_lat: self.min_lat - lat_padding_deg,
            min_lon: self.min_lon - lon_padding_deg,
            max_lat: self.max_lat + lat_padding_deg,
            max_lon: self.max_lon + lon_padding_deg,
        }
    }
}

#[derive(Debug)]
pub struct Pass1Artifacts {
    pub referenced_node_ids: Vec<i64>,
    pub edge_spool: NamedTempFile,
    pub walkable_way_count: u64,
    pub raw_directed_edge_pairs: u64,
}

#[derive(Debug)]
pub struct RetainedNodes {
    pub osm_ids: Vec<i64>,
    pub coords: Vec<(f32, f32)>,
}

pub fn parse_bbox(text: &str) -> Result<Bbox, String> {
    let mut parts = text.split(',');
    let values = [
        parts
            .next()
            .ok_or_else(|| "bbox must contain four comma-separated numbers".to_string())?
            .trim()
            .parse::<f64>()
            .map_err(|_| "bbox latitude values must be numeric".to_string())?,
        parts
            .next()
            .ok_or_else(|| "bbox must contain four comma-separated numbers".to_string())?
            .trim()
            .parse::<f64>()
            .map_err(|_| "bbox longitude values must be numeric".to_string())?,
        parts
            .next()
            .ok_or_else(|| "bbox must contain four comma-separated numbers".to_string())?
            .trim()
            .parse::<f64>()
            .map_err(|_| "bbox latitude values must be numeric".to_string())?,
        parts
            .next()
            .ok_or_else(|| "bbox must contain four comma-separated numbers".to_string())?
            .trim()
            .parse::<f64>()
            .map_err(|_| "bbox longitude values must be numeric".to_string())?,
    ];
    if parts.next().is_some() {
        return Err("bbox must contain exactly four comma-separated numbers".to_string());
    }
    if values[0] > values[2] || values[1] > values[3] {
        return Err("bbox minimums must be <= maximums".to_string());
    }
    Ok(Bbox {
        min_lat: values[0],
        min_lon: values[1],
        max_lat: values[2],
        max_lon: values[3],
    })
}

pub fn is_walkable_tags<I, K, V>(tags: I) -> bool
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut highway: Option<String> = None;
    let mut access: Option<String> = None;
    let mut foot: Option<String> = None;

    for (key, value) in tags {
        match key.as_ref() {
            "highway" => highway = Some(value.as_ref().to_string()),
            "access" => access = Some(value.as_ref().to_ascii_lowercase()),
            "foot" => foot = Some(value.as_ref().to_ascii_lowercase()),
            _ => {}
        }
    }

    let Some(highway) = highway else {
        return false;
    };
    if WALK_EXCLUDED.contains(&highway.as_str()) {
        return false;
    }

    for value in [access.as_deref(), foot.as_deref()].into_iter().flatten() {
        if PRIVATE_VALUES.contains(&value) {
            return false;
        }
    }
    true
}

pub fn scan_walkable_ways(pbf_path: &Path) -> Result<Pass1Artifacts, Box<dyn Error>> {
    let reader = ElementReader::from_path(pbf_path)?;
    let mut edge_spool = NamedTempFile::new()?;
    let mut referenced_node_ids = Vec::<i64>::new();
    let mut walkable_way_count = 0_u64;
    let mut raw_directed_edge_pairs = 0_u64;
    let mut write_error: Option<std::io::Error> = None;

    {
        let mut spool_writer = BufWriter::new(edge_spool.as_file_mut());

        reader.for_each(|element| {
            if write_error.is_some() {
                return;
            }
            let Element::Way(way) = element else {
                return;
            };
            if !is_walkable_tags(way.tags()) {
                return;
            }
            let refs: Vec<i64> = way.refs().collect();
            if refs.len() < 2 {
                return;
            }

            walkable_way_count += 1;
            referenced_node_ids.extend(refs.iter().copied());

            for pair in refs.windows(2) {
                let src = pair[0];
                let dst = pair[1];
                for (from, to) in [(src, dst), (dst, src)] {
                    if let Err(err) = spool_writer.write_all(&from.to_le_bytes()) {
                        write_error = Some(err);
                        return;
                    }
                    if let Err(err) = spool_writer.write_all(&to.to_le_bytes()) {
                        write_error = Some(err);
                        return;
                    }
                    raw_directed_edge_pairs += 1;
                }
            }
        })?;

        spool_writer.flush()?;
    }

    if let Some(err) = write_error {
        return Err(err.into());
    }

    referenced_node_ids.sort_unstable();
    referenced_node_ids.dedup();

    Ok(Pass1Artifacts {
        referenced_node_ids,
        edge_spool,
        walkable_way_count,
        raw_directed_edge_pairs,
    })
}

fn record_if_referenced(
    node_id: i64,
    lat: f64,
    lon: f64,
    referenced_node_ids: &[i64],
    ordered: bool,
    next_index: &mut usize,
    bbox: Option<Bbox>,
    osm_ids: &mut Vec<i64>,
    coords: &mut Vec<(f32, f32)>,
) {
    let referenced = if ordered {
        while *next_index < referenced_node_ids.len() && referenced_node_ids[*next_index] < node_id
        {
            *next_index += 1;
        }
        *next_index < referenced_node_ids.len() && referenced_node_ids[*next_index] == node_id
    } else {
        referenced_node_ids.binary_search(&node_id).is_ok()
    };

    if !referenced {
        return;
    }
    if let Some(bounds) = bbox {
        if !bounds.contains(lat, lon) {
            return;
        }
    }

    osm_ids.push(node_id);
    coords.push((lat as f32, lon as f32));
}

pub fn collect_retained_nodes(
    pbf_path: &Path,
    referenced_node_ids: &[i64],
    bbox: Option<Bbox>,
) -> Result<RetainedNodes, Box<dyn Error>> {
    let mut reader = IndexedReader::from_path(pbf_path)?;
    let mut osm_ids = Vec::with_capacity(referenced_node_ids.len());
    let mut coords = Vec::with_capacity(referenced_node_ids.len());
    let mut next_index = 0_usize;
    let mut ordered = true;
    let mut last_seen = i64::MIN;

    reader.for_each_node(|element| match element {
        Element::Node(node) => {
            let node_id = node.id();
            if node_id < last_seen {
                ordered = false;
            }
            last_seen = node_id;
            record_if_referenced(
                node_id,
                node.lat(),
                node.lon(),
                referenced_node_ids,
                ordered,
                &mut next_index,
                bbox,
                &mut osm_ids,
                &mut coords,
            );
        }
        Element::DenseNode(node) => {
            let node_id = node.id();
            if node_id < last_seen {
                ordered = false;
            }
            last_seen = node_id;
            record_if_referenced(
                node_id,
                node.lat(),
                node.lon(),
                referenced_node_ids,
                ordered,
                &mut next_index,
                bbox,
                &mut osm_ids,
                &mut coords,
            );
        }
        _ => {}
    })?;

    Ok(RetainedNodes { osm_ids, coords })
}

pub fn read_spooled_edge_pair(reader: &mut dyn Read) -> Result<Option<(i64, i64)>, Box<dyn Error>> {
    let mut buffer = [0_u8; 16];
    match reader.read_exact(&mut buffer) {
        Ok(()) => {
            let mut from_bytes = [0_u8; 8];
            let mut to_bytes = [0_u8; 8];
            from_bytes.copy_from_slice(&buffer[..8]);
            to_bytes.copy_from_slice(&buffer[8..]);
            Ok(Some((
                i64::from_le_bytes(from_bytes),
                i64::from_le_bytes(to_bytes),
            )))
        }
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn open_spool_reader(file: &File) -> Result<BufReader<File>, Box<dyn Error>> {
    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(0))?;
    Ok(BufReader::new(cloned))
}

#[cfg(test)]
mod tests;
