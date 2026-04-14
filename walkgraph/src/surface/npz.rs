//! Minimal .npz writer compatible with numpy's `np.savez_compressed`.
//!
//! NPZ is a ZIP archive where each member is a .npy file.
//! NPY 1.0 format: magic + version + u16 header_len + ASCII dict + raw C-order data.
//! We use Deflated compression at level 6 (numpy default).

use std::error::Error;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

// ── NPY helpers ──────────────────────────────────────────────────────────────

const NPY_MAGIC: &[u8] = b"\x93NUMPY";
const NPY_MAJOR: u8 = 1;
const NPY_MINOR: u8 = 0;

fn npy_bytes(descr: &str, shape: &[usize], data_bytes: &[u8]) -> Vec<u8> {
    // Build header dict string: must end with \n and total prefix must be
    // a multiple of 64 bytes (magic[6] + header_len[2] + header).
    let shape_str = if shape.len() == 1 {
        format!("({},)", shape[0])
    } else {
        let parts: Vec<String> = shape.iter().map(|s| s.to_string()).collect();
        format!("({})", parts.join(", "))
    };
    let base_header = format!(
        "{{'descr': '{}', 'fortran_order': False, 'shape': {}, }}",
        descr, shape_str
    );
    // Prefix length: 6 (magic) + 1 (major) + 1 (minor) + 2 (header_len) = 10 bytes.
    // Total must be multiple of 64.
    let prefix_len = 10usize;
    let raw_len = base_header.len() + 1; // +1 for the terminating \n
    let total_header_block = prefix_len + raw_len;
    let padded_total = (total_header_block + 63) / 64 * 64;
    let pad_spaces = padded_total - total_header_block;
    let header: String = format!("{}{}\n", base_header, " ".repeat(pad_spaces));
    let header_len = header.len() as u16;

    let mut out = Vec::with_capacity(prefix_len + header.len() + data_bytes.len());
    out.extend_from_slice(NPY_MAGIC);
    out.push(NPY_MAJOR);
    out.push(NPY_MINOR);
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(data_bytes);
    out
}

fn dtype_item_size(descr: &str) -> Option<usize> {
    match descr {
        "|b1" => Some(1),
        "<i4" | "<f4" => Some(4),
        _ => None,
    }
}

fn extract_quoted_value(header: &str, key: &str) -> Option<String> {
    let marker = format!("'{}': '", key);
    let start = header.find(&marker)? + marker.len();
    let remainder = &header[start..];
    let end = remainder.find('\'')?;
    Some(remainder[..end].to_string())
}

fn parse_shape(header: &str) -> Option<Vec<usize>> {
    let marker = "'shape': (";
    let start = header.find(marker)? + marker.len();
    let remainder = &header[start..];
    let end = remainder.find(')')?;
    let raw = remainder[..end].trim();
    if raw.is_empty() {
        return Some(vec![]);
    }

    let mut values = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        values.push(trimmed.parse::<usize>().ok()?);
    }
    Some(values)
}

fn parse_npy(bytes: &[u8]) -> Option<(String, Vec<usize>, &[u8])> {
    if bytes.len() < 10 || &bytes[..6] != NPY_MAGIC {
        return None;
    }
    let header_len = u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
    let data_start = 10usize.checked_add(header_len)?;
    if bytes.len() < data_start {
        return None;
    }
    let header = std::str::from_utf8(&bytes[10..data_start]).ok()?;
    let descr = extract_quoted_value(header, "descr")?;
    let shape = parse_shape(header)?;
    let item_size = dtype_item_size(&descr)?;
    let expected_bytes = shape
        .iter()
        .try_fold(1usize, |acc, dim| acc.checked_mul(*dim))?
        .checked_mul(item_size)?;
    if bytes.len() - data_start != expected_bytes {
        return None;
    }
    Some((descr, shape, &bytes[data_start..]))
}

fn validate_member_shape(
    archive: &mut ZipArchive<fs::File>,
    member_name: &str,
    expected_descr: &str,
    expected_shape: &[usize],
) -> bool {
    let mut member = match archive.by_name(member_name) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut bytes = Vec::new();
    if member.read_to_end(&mut bytes).is_err() {
        return false;
    }
    match parse_npy(&bytes) {
        Some((descr, shape, _data)) => descr == expected_descr && shape == expected_shape,
        None => false,
    }
}

fn validate_scalar_i32(
    archive: &mut ZipArchive<fs::File>,
    member_name: &str,
    expected_value: i32,
) -> bool {
    let mut member = match archive.by_name(member_name) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut bytes = Vec::new();
    if member.read_to_end(&mut bytes).is_err() {
        return false;
    }
    match parse_npy(&bytes) {
        Some((descr, shape, data)) => {
            descr == "<i4"
                && shape == [1usize]
                && data.len() == 4
                && i32::from_le_bytes([data[0], data[1], data[2], data[3]]) == expected_value
        }
        None => false,
    }
}

pub fn validate_shell_npz(
    path: &Path,
    rows: usize,
    cols: usize,
    x_min_m: i32,
    y_min_m: i32,
) -> bool {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut archive = match ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(_) => return false,
    };

    validate_member_shape(&mut archive, "origin_node_idx.npy", "<i4", &[rows, cols])
        && validate_member_shape(
            &mut archive,
            "effective_area_ratio.npy",
            "<f4",
            &[rows, cols],
        )
        && validate_member_shape(&mut archive, "valid_land_mask.npy", "|b1", &[rows, cols])
        && validate_scalar_i32(&mut archive, "x_min_m.npy", x_min_m)
        && validate_scalar_i32(&mut archive, "y_min_m.npy", y_min_m)
}

// ── Public typed helpers ─────────────────────────────────────────────────────

/// Build a .npy byte blob for a flat i32 array with the given shape.
pub fn npy_i32(data: &[i32], shape: &[usize]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(data.len() * 4);
    for &v in data {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    npy_bytes("<i4", shape, &raw)
}

/// Build a .npy byte blob for a flat f32 array with the given shape.
pub fn npy_f32(data: &[f32], shape: &[usize]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(data.len() * 4);
    for &v in data {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    npy_bytes("<f4", shape, &raw)
}

/// Build a .npy byte blob for a flat bool array (stored as u8 0/1) with the given shape.
pub fn npy_bool(data: &[bool], shape: &[usize]) -> Vec<u8> {
    let raw: Vec<u8> = data.iter().map(|&b| if b { 1u8 } else { 0u8 }).collect();
    npy_bytes("|b1", shape, &raw)
}

// ── NPZ writer ───────────────────────────────────────────────────────────────

/// Write a .npz file (compressed ZIP of .npy members).
///
/// `entries` is a slice of `(member_name, npy_bytes)` — member names should
/// NOT include the `.npy` extension (it is appended automatically).
///
/// Writes to a `.tmp` sibling path then renames atomically.
pub fn write_npz(
    path: &Path,
    entries: &[(&str, Vec<u8>)],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let tmp_path = path.with_extension("npz.tmp");
    {
        let file = fs::File::create(&tmp_path)?;
        let mut zip = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .compression_level(Some(6));
        for (name, npy_data) in entries {
            // ZIP entry names must use forward slashes; no OS path joining.
            let member = format!("{}.npy", name);
            zip.start_file(member, options)?;
            zip.write_all(npy_data)?;
        }
        zip.finish()?;
    }
    fs::rename(&tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_npy_header_alignment() {
        // Header block must be a multiple of 64 bytes.
        let blob = npy_i32(&[1i32, 2, 3, 4], &[2, 2]);
        // 6 (magic) + 1 + 1 + 2 = 10 prefix bytes, then header up to first \n
        let header_len = u16::from_le_bytes([blob[8], blob[9]]) as usize;
        let total = 10 + header_len;
        assert_eq!(
            total % 64,
            0,
            "NPY header block not 64-byte aligned: {total}"
        );
    }

    #[test]
    fn npy_shape_scalar() {
        // shape=(1,) for scalar arrays used by x_min_m / y_min_m
        let blob = npy_i32(&[42i32], &[1]);
        assert!(blob.len() > 10);
        // last 4 bytes should be 42 in LE
        let tail = &blob[blob.len() - 4..];
        assert_eq!(i32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]), 42);
    }
}
