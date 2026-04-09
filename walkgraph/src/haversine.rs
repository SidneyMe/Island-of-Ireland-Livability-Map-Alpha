use std::f32::consts::PI;

const EARTH_RADIUS_M: f32 = 6_371_000.0;

#[inline]
fn radians(value: f32) -> f32 {
    value * (PI / 180.0)
}

pub fn haversine_m(lat1: f32, lon1: f32, lat2: f32, lon2: f32) -> f32 {
    let phi1 = radians(lat1);
    let phi2 = radians(lat2);
    let dphi = radians(lat2 - lat1);
    let dlambda = radians(lon2 - lon1);
    let a = (dphi * 0.5).sin().powi(2) + phi1.cos() * phi2.cos() * (dlambda * 0.5).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

#[cfg(test)]
mod tests;
