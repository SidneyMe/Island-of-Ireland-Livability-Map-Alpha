//! Daily access v4 cell scoring; mirrors `precompute/daily_access_score.py`
//! and `daily_access_reference.score_from_distances` operation for operation.

use serde::Deserialize;

pub const NEEDS: [&str; 6] = ["food", "other_retail", "primary_care", "pharmacy", "green", "transport"];
pub const CATEGORIES: [&str; 4] = ["shops", "transport", "healthcare", "parks"];
pub const UINT16_MISSING: u16 = u16::MAX;
pub const NETWORK_OUTSIDE_MASK: u8 = 0;
pub const NETWORK_OK: u8 = 1;
pub const NETWORK_NO_WALKING_NETWORK: u8 = 2;

#[derive(Clone, Debug, Deserialize)]
pub struct ScoreParams {
    /// need -> [full_m, zero_m]
    pub thresholds_m: std::collections::HashMap<String, [f64; 2]>,
    pub top3_weights: [f64; 3],
    pub category_max: std::collections::HashMap<String, f64>,
    pub shop_mix: [f64; 2],
    pub healthcare_mix: [f64; 2],
}

/// One service kind's best distances for one cell, as seen by the scorer.
pub struct KindDistances<'a> {
    pub need_index: usize,
    pub quality: f64,
    pub green: bool,
    pub kind_index: u32,
    /// `(logical id, distance_m)`, one entry per id.
    pub distances: &'a [(u32, f64)],
}

#[derive(Clone, Debug, PartialEq)]
pub struct CellScore {
    pub total: f64,
    /// shops, transport, healthcare, parks
    pub components: [f64; 4],
    /// railway, road (positive)
    pub penalties: [f64; 2],
    /// food, other_retail, primary_care, pharmacy, green, transport
    pub access: [f64; 6],
}

impl CellScore {
    pub fn zero() -> Self {
        Self {
            total: 0.0,
            components: [0.0; 4],
            penalties: [0.0; 2],
            access: [0.0; 6],
        }
    }
}

pub struct Scorer {
    thresholds: [(f64, f64); 6],
    top3: [f64; 3],
    category_max: [f64; 4],
    shop_mix: [f64; 2],
    healthcare_mix: [f64; 2],
}

impl Scorer {
    pub fn new(params: &ScoreParams) -> Result<Self, String> {
        let mut thresholds = [(0.0, 0.0); 6];
        for (index, need) in NEEDS.iter().enumerate() {
            let value = params
                .thresholds_m
                .get(*need)
                .ok_or_else(|| format!("missing threshold for {need}"))?;
            thresholds[index] = (value[0], value[1]);
        }
        let mut category_max = [0.0; 4];
        for (index, category) in CATEGORIES.iter().enumerate() {
            category_max[index] = *params
                .category_max
                .get(*category)
                .ok_or_else(|| format!("missing category max for {category}"))?;
        }
        Ok(Self {
            thresholds,
            top3: params.top3_weights,
            category_max,
            shop_mix: params.shop_mix,
            healthcare_mix: params.healthcare_mix,
        })
    }

    pub fn decay(&self, distance_m: f64, need_index: usize) -> f64 {
        let (full_m, zero_m) = self.thresholds[need_index];
        if distance_m <= full_m {
            return 1.0;
        }
        if distance_m >= zero_m {
            return 0.0;
        }
        let x = (distance_m - full_m) / (zero_m - full_m);
        1.0 - 3.0 * x * x + 2.0 * x * x * x
    }

    fn top3_access(&self, contributions: &mut [f64]) -> f64 {
        contributions.sort_by(|a, b| b.total_cmp(a));
        let mut sum = 0.0;
        for (weight, value) in self.top3.iter().zip(contributions.iter()) {
            sum += weight * value;
        }
        sum
    }

    /// Access per need from per-kind best distances (one entry per logical id).
    ///
    /// Mirrors `daily_access_reference.score_from_distances` + `need_access`:
    /// ids sharing a key (a green root across green kinds) keep their best
    /// contribution; transport takes the single best stop, every other need
    /// the top three.
    pub fn access_from_distances(&self, kinds: &[KindDistances<'_>]) -> [f64; 6] {
        let mut best: [Vec<(u64, f64)>; 6] = Default::default();
        for kind in kinds {
            for &(label, distance) in kind.distances {
                let key = if kind.green {
                    u64::from(label)
                } else {
                    (u64::from(kind.kind_index + 1) << 32) | u64::from(label)
                };
                let contribution = kind.quality * self.decay(distance, kind.need_index);
                let entries = &mut best[kind.need_index];
                match entries.iter_mut().find(|(existing, _)| *existing == key) {
                    Some(entry) => {
                        if contribution > entry.1 {
                            entry.1 = contribution;
                        }
                    }
                    None => entries.push((key, contribution)),
                }
            }
        }
        let mut access = [0.0; 6];
        for (need_index, entries) in best.iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let mut values: Vec<f64> = entries.iter().map(|entry| entry.1).collect();
            access[need_index] = if NEEDS[need_index] == "transport" {
                values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
            } else {
                self.top3_access(&mut values)
            };
        }
        access
    }

    pub fn score(&self, access: [f64; 6], railway_penalty: f64, road_penalty: f64) -> CellScore {
        let [food, other_retail, primary_care, pharmacy, green, transport] = access;
        let components = [
            self.category_max[0] * (self.shop_mix[0] * food + self.shop_mix[1] * other_retail),
            self.category_max[1] * transport,
            self.category_max[2] * (self.healthcare_mix[0] * primary_care + self.healthcare_mix[1] * pharmacy),
            self.category_max[3] * green,
        ];
        let railway = railway_penalty.max(0.0);
        let road = road_penalty.max(0.0);
        let mut raw = 0.0 + components[0] + components[1] + components[2] + components[3];
        raw -= railway + road;
        CellScore {
            total: raw.max(0.0).min(100.0),
            components,
            penalties: [railway, road],
            access,
        }
    }
}

fn round_half_up(value: f64, scale: f64) -> u16 {
    let code = (value * scale + 0.5).floor();
    assert!(
        (0.0..f64::from(UINT16_MISSING)).contains(&code),
        "value {value} does not fit the uint16 code"
    );
    code as u16
}

pub fn quantize_score(value: f64) -> u16 {
    round_half_up(value, 100.0)
}

pub fn quantize_access(value: f64) -> u16 {
    round_half_up(value, 10_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> ScoreParams {
        let thresholds = NEEDS
            .iter()
            .map(|need| {
                let value = match *need {
                    "green" => [400.0, 2000.0],
                    "transport" => [240.0, 800.0],
                    _ => [400.0, 1600.0],
                };
                (need.to_string(), value)
            })
            .collect();
        ScoreParams {
            thresholds_m: thresholds,
            top3_weights: [0.8, 0.15, 0.05],
            category_max: [("shops", 35.0), ("transport", 30.0), ("healthcare", 20.0), ("parks", 15.0)]
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
            shop_mix: [0.9, 0.1],
            healthcare_mix: [0.6, 0.4],
        }
    }

    #[test]
    fn curve_and_totals() {
        let scorer = Scorer::new(&params()).unwrap();
        assert_eq!(scorer.decay(400.0, 0), 1.0);
        assert_eq!(scorer.decay(1600.0, 0), 0.0);
        assert!((scorer.decay(1000.0, 0) - 0.5).abs() < 1e-12);
        let score = scorer.score([1.0; 6], 4.0, 3.0);
        assert_eq!(score.components, [35.0, 30.0, 20.0, 15.0]);
        assert_eq!(score.total, 93.0);
        assert_eq!(scorer.score([0.01, 0.0, 0.0, 0.0, 0.0, 0.0], 0.0, 4.0).total, 0.0);
    }

    #[test]
    fn same_label_collapses_and_quantization() {
        let scorer = Scorer::new(&params()).unwrap();
        let food = [(5_u32, 0.0), (6_u32, 2000.0)];
        let green_a = [(1_u32, 0.0)];
        let green_b = [(1_u32, 0.0), (2_u32, 0.0)];
        let kinds = [
            KindDistances { need_index: 0, quality: 1.0, green: false, kind_index: 0, distances: &food },
            KindDistances { need_index: 4, quality: 0.4, green: true, kind_index: 1, distances: &green_a },
            KindDistances { need_index: 4, quality: 1.0, green: true, kind_index: 2, distances: &green_b },
        ];
        let access = scorer.access_from_distances(&kinds);
        assert!((access[0] - 0.8).abs() < 1e-12);
        // Root 1 appears in two green kinds: counted once, at its best quality.
        assert!((access[4] - 0.95).abs() < 1e-12);
        assert_eq!(quantize_score(12.345), 1235);
        assert_eq!(quantize_access(0.12345), 1235);
    }
}
