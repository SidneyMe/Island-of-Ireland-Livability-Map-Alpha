//! Exact network entries on walk-graph edges (daily access v4).
//!
//! Mirrors `precompute/daily_access_geometry.py` operation for operation:
//! an edge is the straight segment between its two nodes (ITM metres), a
//! position `x` in `[0, G]` costs `c x` to node `a` and `c (G - x)` to node
//! `b` with `c = L / G`. Admissible connector positions are the radius
//! interval minus the exact "shadows" of nearby land-mask boundary segments;
//! costs over each interval are minimised in closed form (stationary point
//! clamped to the interval), and the same-edge case is a convex golden-section
//! search. Nothing is sampled.

pub const EPS_LENGTH_M: f64 = 1e-6;
pub const PIECE_POINT: u8 = 0;
pub const PIECE_LINE: u8 = 1;
pub const PIECE_ZERO: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EdgeFrame {
    pub edge: u32,
    pub node_a: u32,
    pub node_b: u32,
    pub ax: f64,
    pub ay: f64,
    pub ux: f64,
    pub uy: f64,
    pub length_geom_m: f64,
    pub length_graph_m: f64,
}

impl EdgeFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn new(edge: u32, node_a: u32, node_b: u32, ax: f64, ay: f64, bx: f64, by: f64, length_graph_m: f64) -> Self {
        let dx = bx - ax;
        let dy = by - ay;
        let length = dx.hypot(dy);
        if length <= EPS_LENGTH_M {
            return Self { edge, node_a, node_b, ax, ay, ux: 1.0, uy: 0.0, length_geom_m: 0.0, length_graph_m };
        }
        Self { edge, node_a, node_b, ax, ay, ux: dx / length, uy: dy / length, length_geom_m: length, length_graph_m }
    }

    pub fn c(&self) -> f64 {
        if self.length_geom_m <= EPS_LENGTH_M {
            1.0
        } else {
            self.length_graph_m / self.length_geom_m
        }
    }

    /// `(p, h)`: projection position along the edge line, perpendicular distance.
    pub fn project(&self, x: f64, y: f64) -> (f64, f64) {
        let dx = x - self.ax;
        let dy = y - self.ay;
        (dx * self.ux + dy * self.uy, (dx * self.uy - dy * self.ux).abs())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Piece {
    pub kind: u8,
    pub lo: f64,
    pub hi: f64,
    /// POINT: q ; LINE: alpha
    pub a: f64,
    /// POINT: k ; LINE: beta
    pub b: f64,
}

impl Piece {
    pub fn value(&self, position: f64) -> f64 {
        match self.kind {
            PIECE_POINT => (position - self.a).hypot(self.b),
            PIECE_LINE => (self.a * position + self.b).abs(),
            _ => 0.0,
        }
    }
}

pub type Interval = Option<(f64, f64)>;

/// Keep the positions `x` of `interval` where `c0 + c1 x >= 0`.
pub fn clip_linear(interval: Interval, c0: f64, c1: f64) -> Interval {
    let (mut lo, mut hi) = interval?;
    if c1 > 0.0 {
        lo = lo.max(-c0 / c1);
    } else if c1 < 0.0 {
        hi = hi.min(-c0 / c1);
    } else if c0 < 0.0 {
        return None;
    }
    if lo <= hi {
        Some((lo, hi))
    } else {
        None
    }
}

pub fn radius_interval(p: f64, h: f64, length_geom_m: f64, radius_m: f64) -> Interval {
    if h > radius_m {
        return None;
    }
    let half = (radius_m * radius_m - h * h).max(0.0).sqrt();
    let lo = 0.0_f64.max(p - half);
    let hi = length_geom_m.min(p + half);
    if lo <= hi {
        Some((lo, hi))
    } else {
        None
    }
}

/// Closure of `[lo, hi]` minus the closed `blocks` (see the Python docstring).
pub fn admissible_intervals(lo: f64, hi: f64, blocks: &[(f64, f64)], merge: f64) -> Vec<(f64, f64)> {
    if lo > hi {
        return Vec::new();
    }
    if hi - lo <= 0.0 {
        if blocks.iter().any(|&(b0, b1)| b0 - merge <= lo && lo <= b1 + merge) {
            return Vec::new();
        }
        return vec![(lo, hi)];
    }
    let mut clipped: Vec<(f64, f64)> = blocks
        .iter()
        .filter(|&&(b0, b1)| b1.min(hi) - b0.max(lo) > 0.0)
        .map(|&(b0, b1)| (b0.max(lo), b1.min(hi)))
        .collect();
    if clipped.is_empty() {
        return vec![(lo, hi)];
    }
    clipped.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.total_cmp(&b.1)));
    let mut out = Vec::new();
    let mut cursor = lo;
    for (start, end) in clipped {
        if start - cursor > merge {
            out.push((cursor, start));
        }
        cursor = cursor.max(end);
    }
    if hi - cursor > merge {
        out.push((cursor, hi));
    }
    out
}

fn cross(ax: f64, ay: f64, bx: f64, by: f64) -> f64 {
    ax * by - ay * bx
}

/// Positions whose connector from `(px, py)` meets the boundary `segment`.
pub fn point_shadow(frame: &EdgeFrame, px: f64, py: f64, segment: &[f64; 4], interval: (f64, f64)) -> Interval {
    let (d1x, d1y) = (segment[0] - px, segment[1] - py);
    let (d2x, d2y) = (segment[2] - px, segment[3] - py);
    let cr = cross(d1x, d1y, d2x, d2y);
    if cr == 0.0 {
        return None;
    }
    let sign = if cr > 0.0 { 1.0 } else { -1.0 };
    let (wx, wy) = (frame.ax - px, frame.ay - py);
    let (ux, uy) = (frame.ux, frame.uy);
    let out = clip_linear(Some(interval), sign * cross(wx, wy, d2x, d2y), sign * cross(ux, uy, d2x, d2y));
    let out = clip_linear(out, sign * cross(d1x, d1y, wx, wy), sign * cross(d1x, d1y, ux, uy));
    let (ex, ey) = (d2x - d1x, d2y - d1y);
    clip_linear(out, sign * cross(wx, wy, ex, ey) - cr.abs(), sign * cross(ux, uy, ex, ey))
}

/// Positions whose perpendicular connector from a footprint side meets `segment`.
#[allow(clippy::too_many_arguments)]
pub fn parallel_shadow(
    frame: &EdgeFrame,
    v1x: f64,
    v1y: f64,
    dx: f64,
    dy: f64,
    side: f64,
    segment: &[f64; 4],
    interval: (f64, f64),
) -> Interval {
    let (nx, ny) = (-dy, dx);
    let local = |x: f64, y: f64| -> (f64, f64) {
        let (rx, ry) = (x - v1x, y - v1y);
        (rx * dx + ry * dy, rx * nx + ry * ny)
    };
    let (mut tau1, mut t1) = local(segment[0], segment[1]);
    let (mut tau2, mut t2) = local(segment[2], segment[3]);
    if tau1 == tau2 {
        return None;
    }
    if tau1 > tau2 {
        std::mem::swap(&mut tau1, &mut tau2);
        std::mem::swap(&mut t1, &mut t2);
    }
    let span = tau2 - tau1;
    let (tau0, t0) = local(frame.ax, frame.ay);
    let u_tau = frame.ux * dx + frame.uy * dy;
    let u_t = frame.ux * nx + frame.uy * ny;
    let out = clip_linear(Some(interval), tau0 - tau1, u_tau);
    let out = clip_linear(out, tau2 - tau0, -u_tau);
    let dt = t2 - t1;
    let out = clip_linear(out, side * (t1 * span + dt * (tau0 - tau1)), side * dt * u_tau);
    clip_linear(out, side * (t0 * span - t1 * span - dt * (tau0 - tau1)), side * (u_t * span - dt * u_tau))
}

/// Admissible POINT pieces of a connector from `(px, py)`; `boundary=None`
/// means the point lies deeper than the land band.
pub fn point_pieces(
    frame: &EdgeFrame,
    px: f64,
    py: f64,
    radius_m: f64,
    boundary: Option<&[[f64; 4]]>,
    domain: Option<(f64, f64)>,
    merge: f64,
    out: &mut Vec<Piece>,
) {
    let (p, h) = frame.project(px, py);
    let mut interval = radius_interval(p, h, frame.length_geom_m, radius_m);
    if let (Some((lo, hi)), Some((d0, d1))) = (interval, domain) {
        let (lo, hi) = (lo.max(d0), hi.min(d1));
        interval = if lo <= hi { Some((lo, hi)) } else { None };
    }
    let Some(interval) = interval else {
        return;
    };
    let mut blocks = Vec::new();
    if let Some(segments) = boundary {
        for segment in segments {
            if let Some(blocked) = point_shadow(frame, px, py, segment, interval) {
                blocks.push(blocked);
            }
        }
    }
    for (lo, hi) in admissible_intervals(interval.0, interval.1, &blocks, merge) {
        out.push(Piece { kind: PIECE_POINT, lo, hi, a: p, b: h });
    }
}

/// Positions on `[0, G]` inside the footprint (even-odd over closed rings).
fn inside_intervals(frame: &EdgeFrame, rings: &[&[[f64; 2]]]) -> Vec<(f64, f64)> {
    if frame.length_geom_m <= 0.0 {
        return Vec::new();
    }
    let (ux, uy) = (frame.ux, frame.uy);
    let mut crossings: Vec<f64> = Vec::new();
    for ring in rings {
        for index in 0..ring.len().saturating_sub(1) {
            let (x0, y0) = (ring[index][0] - frame.ax, ring[index][1] - frame.ay);
            let (x1, y1) = (ring[index + 1][0] - frame.ax, ring[index + 1][1] - frame.ay);
            let s0 = cross(ux, uy, x0, y0);
            let s1 = cross(ux, uy, x1, y1);
            if (s0 > 0.0) == (s1 > 0.0) {
                continue;
            }
            let fraction = s0 / (s0 - s1);
            crossings.push((x0 + fraction * (x1 - x0)) * ux + (y0 + fraction * (y1 - y0)) * uy);
        }
    }
    crossings.sort_by(|a, b| a.total_cmp(b));
    let mut out = Vec::new();
    for pair in crossings.chunks_exact(2) {
        let lo = pair[0].max(0.0);
        let hi = pair[1].min(frame.length_geom_m);
        if lo < hi {
            out.push((lo, hi));
        }
    }
    out
}

/// Admissible pieces of connectors from a polygon footprint (0 inside).
pub fn footprint_pieces(
    frame: &EdgeFrame,
    rings: &[&[[f64; 2]]],
    radius_m: f64,
    boundary: Option<&[[f64; 4]]>,
    merge: f64,
    out: &mut Vec<Piece>,
) {
    let length = frame.length_geom_m;
    for (lo, hi) in inside_intervals(frame, rings) {
        out.push(Piece { kind: PIECE_ZERO, lo, hi, a: 0.0, b: 0.0 });
    }
    for ring in rings {
        for index in 0..ring.len().saturating_sub(1) {
            let (v1x, v1y) = (ring[index][0], ring[index][1]);
            let (v2x, v2y) = (ring[index + 1][0], ring[index + 1][1]);
            let side_length = (v2x - v1x).hypot(v2y - v1y);
            if side_length <= EPS_LENGTH_M {
                continue;
            }
            let (dx, dy) = ((v2x - v1x) / side_length, (v2y - v1y) / side_length);
            let tau0 = (frame.ax - v1x) * dx + (frame.ay - v1y) * dy;
            let u_tau = frame.ux * dx + frame.uy * dy;
            let full = Some((0.0, length));
            let before = clip_linear(full, -tau0, -u_tau);
            let after = clip_linear(full, tau0 - side_length, u_tau);
            let slab = clip_linear(clip_linear(full, tau0, u_tau), side_length - tau0, -u_tau);
            if before.is_some() {
                point_pieces(frame, v1x, v1y, radius_m, boundary, before, merge, out);
            }
            if after.is_some() {
                point_pieces(frame, v2x, v2y, radius_m, boundary, after, merge, out);
            }
            if slab.is_none() {
                continue;
            }
            let (nx, ny) = (-dy, dx);
            let t0 = (frame.ax - v1x) * nx + (frame.ay - v1y) * ny;
            let u_t = frame.ux * nx + frame.uy * ny;
            for side in [1.0_f64, -1.0] {
                let interval = clip_linear(slab, side * t0, side * u_t);
                let interval = clip_linear(interval, radius_m - side * t0, -side * u_t);
                let Some(interval) = interval else {
                    continue;
                };
                let mut blocks = Vec::new();
                if let Some(segments) = boundary {
                    for segment in segments {
                        if let Some(blocked) = parallel_shadow(frame, v1x, v1y, dx, dy, side, segment, interval) {
                            blocks.push(blocked);
                        }
                    }
                }
                for (lo, hi) in admissible_intervals(interval.0, interval.1, &blocks, merge) {
                    out.push(Piece { kind: PIECE_LINE, lo, hi, a: side * u_t, b: side * t0 });
                }
            }
        }
    }
}

/// Exact `min over [lo, hi]` of `piece(y) + slope * y` (inf if empty).
pub fn min_with_slope(piece: &Piece, lo: f64, hi: f64, slope: f64) -> f64 {
    if lo > hi {
        return f64::INFINITY;
    }
    match piece.kind {
        PIECE_POINT => {
            let position = if slope.abs() < 1.0 {
                let stationary = piece.a - slope * piece.b / (1.0 - slope * slope).sqrt();
                stationary.max(lo).min(hi)
            } else if slope > 0.0 {
                lo
            } else {
                hi
            };
            piece.value(position) + slope * position
        }
        PIECE_LINE => {
            let mut best = (piece.value(lo) + slope * lo).min(piece.value(hi) + slope * hi);
            if piece.a != 0.0 {
                let kink = -piece.b / piece.a;
                if lo < kink && kink < hi {
                    best = best.min(piece.value(kink) + slope * kink);
                }
            }
            best
        }
        _ => slope * if slope > 0.0 { lo } else { hi },
    }
}

/// `(to a, to b)`: best connector + along-edge cost over all pieces.
pub fn endpoint_costs(frame: &EdgeFrame, pieces: &[Piece], offset_m: f64) -> (f64, f64) {
    let c = frame.c();
    let mut to_a = f64::INFINITY;
    let mut to_b = f64::INFINITY;
    for piece in pieces {
        to_a = to_a.min(min_with_slope(piece, piece.lo, piece.hi, c));
        to_b = to_b.min(c * frame.length_geom_m + min_with_slope(piece, piece.lo, piece.hi, -c));
    }
    (to_a + offset_m, to_b + offset_m)
}

fn potential(piece: &Piece, c: f64, position: f64) -> f64 {
    let mut best = f64::INFINITY;
    if piece.lo <= position {
        best = c * position + min_with_slope(piece, piece.lo, position.min(piece.hi), -c);
    }
    if position <= piece.hi {
        best = best.min(-c * position + min_with_slope(piece, position.max(piece.lo), piece.hi, c));
    }
    best
}

/// Exact direct distance when the cell and the object enter the same edge.
pub fn same_edge_distance(frame: &EdgeFrame, cell: &Piece, target: &Piece, offset_m: f64, iterations: usize) -> f64 {
    let c = frame.c();
    let golden = (5.0_f64.sqrt() - 1.0) / 2.0;
    let total = |position: f64| cell.value(position) + potential(target, c, position);
    let (lo, hi) = (cell.lo, cell.hi);
    let mut best = total(lo).min(total(hi));
    let (mut left, mut right) = (lo, hi);
    let mut inner_left = right - golden * (right - left);
    let mut inner_right = left + golden * (right - left);
    let mut value_left = total(inner_left);
    let mut value_right = total(inner_right);
    for _ in 0..iterations {
        if value_left <= value_right {
            right = inner_right;
            inner_right = inner_left;
            value_right = value_left;
            inner_left = right - golden * (right - left);
            value_left = total(inner_left);
        } else {
            left = inner_left;
            inner_left = inner_right;
            value_left = value_right;
            inner_right = left + golden * (right - left);
            value_right = total(inner_right);
        }
    }
    best = best.min(value_left).min(value_right);
    best + offset_m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interior_stationary_point_counterexample() {
        // G = 500, L = 498.7, p = 249, h = 2, radius 250, through a.
        let frame = EdgeFrame::new(0, 0, 1, 0.0, 0.0, 500.0, 0.0, 498.7);
        let mut pieces = Vec::new();
        point_pieces(&frame, 249.0, 2.0, 250.0, None, None, 1e-7, &mut pieces);
        assert_eq!(pieces.len(), 1);
        let (to_a, _) = endpoint_costs(&frame, &pieces, 0.0);
        assert!((to_a - 248.496728).abs() < 1e-5, "{to_a}");
        let naive = [pieces[0].lo, pieces[0].hi, 249.0]
            .iter()
            .map(|&x| (x - 249.0_f64).hypot(2.0) + frame.c() * x)
            .fold(f64::INFINITY, f64::min);
        assert!((naive - 249.008032).abs() < 1e-5, "{naive}");
    }

    #[test]
    fn bay_splits_the_edge_into_two_intervals() {
        // U-shaped land around a 40 m channel (x 480..520, y >= 200).
        let ring = [
            (0.0, 0.0), (1000.0, 0.0), (1000.0, 600.0), (520.0, 600.0), (520.0, 200.0),
            (480.0, 200.0), (480.0, 600.0), (0.0, 600.0), (0.0, 0.0),
        ];
        let segments: Vec<[f64; 4]> = ring.windows(2).map(|w| [w[0].0, w[0].1, w[1].0, w[1].1]).collect();
        let frame = EdgeFrame::new(0, 0, 1, 200.0, 300.0, 800.0, 300.0, 600.0);
        let mut pieces = Vec::new();
        point_pieces(&frame, 500.0, 100.0, 250.0, Some(&segments), None, 1e-7, &mut pieces);
        assert_eq!(pieces.len(), 2);
        assert!((pieces[0].lo - 150.0).abs() < 1e-6 && (pieces[0].hi - 260.0).abs() < 1e-6);
        assert!((pieces[1].lo - 340.0).abs() < 1e-6 && (pieces[1].hi - 450.0).abs() < 1e-6);
    }

    #[test]
    fn same_edge_is_the_straight_line_across() {
        let frame = EdgeFrame::new(0, 0, 1, 0.0, 0.0, 2000.0, 0.0, 2000.0);
        let mut cell = Vec::new();
        point_pieces(&frame, 1000.0, 20.0, 250.0, None, None, 1e-7, &mut cell);
        let mut shop = Vec::new();
        point_pieces(&frame, 1010.0, -10.0, 250.0, None, None, 1e-7, &mut shop);
        let distance = same_edge_distance(&frame, &cell[0], &shop[0], 0.0, 64);
        assert!((distance - 10.0_f64.hypot(30.0)).abs() < 1e-6, "{distance}");
    }

    #[test]
    fn footprint_through_edge_costs_nothing() {
        let ring: Vec<[f64; 2]> = vec![[90.0, -10.0], [110.0, -10.0], [110.0, 10.0], [90.0, 10.0], [90.0, -10.0]];
        let frame = EdgeFrame::new(0, 0, 1, 0.0, 0.0, 300.0, 0.0, 300.0);
        let mut pieces = Vec::new();
        footprint_pieces(&frame, &[&ring[..]], 250.0, None, 1e-7, &mut pieces);
        let (to_a, to_b) = endpoint_costs(&frame, &pieces, 0.0);
        assert!((to_a - 90.0).abs() < 1e-6 && (to_b - 190.0).abs() < 1e-6, "{to_a} {to_b}");
    }
}
