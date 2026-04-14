//! Load walk_graph.nodes.bin, project all nodes to ITM, and build a
//! Euclidean KD-tree for fast nearest-node queries in metric space.

use std::error::Error;
use std::fs;
use std::path::Path;

use kiddo::{KdTree, SquaredEuclidean};

use super::itm::wgs84_to_itm;

/// Type alias matching kiddo 4.x generic parameters:
/// f64 coords, 2 dimensions, bucket size 32.
type Tree = KdTree<f64, 2>;

pub struct NodeKdTree {
    tree: Tree,
}

impl NodeKdTree {
    /// Build from `walk_graph.nodes.bin` (packed little-endian f32 lat/lon pairs).
    pub fn build(nodes_bin: &Path) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let bytes = fs::read(nodes_bin)?;
        if bytes.len() % 8 != 0 {
            return Err(format!("nodes.bin size {} is not a multiple of 8", bytes.len()).into());
        }
        let node_count = bytes.len() / 8;

        let mut tree: Tree = KdTree::with_capacity(node_count);

        for (idx, chunk) in bytes.chunks_exact(8).enumerate() {
            let lat = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as f64;
            let lon = f32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]) as f64;
            let (easting, northing) = wgs84_to_itm(lat, lon);
            tree.add(&[easting, northing], idx as u64);
        }

        Ok(Self { tree })
    }

    /// Return the 0-based node index closest to the given ITM position.
    #[inline]
    pub fn nearest(&self, easting: f64, northing: f64) -> u32 {
        self.tree
            .nearest_one::<SquaredEuclidean>(&[easting, northing])
            .item
            .try_into()
            .unwrap()
    }

    /// Number of nodes in the tree.
    pub fn len(&self) -> usize {
        self.tree.size().try_into().unwrap()
    }
}
