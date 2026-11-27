function geometryPoints(geometry, points) {
  if (!geometry || !Array.isArray(geometry.coordinates)) return;
  function visit(value) {
    if (!Array.isArray(value)) return;
    if (value.length >= 2 && Number.isFinite(Number(value[0])) && Number.isFinite(Number(value[1]))) {
      points.push([Number(value[0]), Number(value[1])]);
      return;
    }
    value.forEach(visit);
  }
  visit(geometry.coordinates);
}

function cross(origin, first, second) {
  return (first[0] - origin[0]) * (second[1] - origin[1]) -
    (first[1] - origin[1]) * (second[0] - origin[0]);
}

export function convexHull(points) {
  const unique = Array.from(new Map(points.map(function (point) {
    return [point[0].toFixed(12) + "," + point[1].toFixed(12), point];
  })).values()).sort(function (a, b) {
    return a[0] - b[0] || a[1] - b[1];
  });
  if (unique.length <= 2) return unique;
  const lower = [];
  unique.forEach(function (point) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], point) <= 0) lower.pop();
    lower.push(point);
  });
  const upper = [];
  unique.slice().reverse().forEach(function (point) {
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], point) <= 0) upper.pop();
    upper.push(point);
  });
  lower.pop();
  upper.pop();
  return lower.concat(upper);
}

export function cellGeometryCenter(geometry) {
  const points = [];
  geometryPoints(geometry, points);
  if (!points.length) return null;
  let minLon = points[0][0];
  let maxLon = points[0][0];
  let minLat = points[0][1];
  let maxLat = points[0][1];
  points.forEach(function (point) {
    minLon = Math.min(minLon, point[0]);
    maxLon = Math.max(maxLon, point[0]);
    minLat = Math.min(minLat, point[1]);
    maxLat = Math.max(maxLat, point[1]);
  });
  return [(minLon + maxLon) / 2, (minLat + maxLat) / 2];
}

export function completeCellGeometry(renderedFeature, sourceFeatures = []) {
  if (!renderedFeature || !renderedFeature.geometry) return null;
  const cellId = String(renderedFeature.properties && renderedFeature.properties.cell_id || "");
  const candidates = [renderedFeature].concat(sourceFeatures || []).filter(function (feature) {
    return feature && feature.geometry && (!cellId || String(feature.properties && feature.properties.cell_id || "") === cellId);
  });
  const points = [];
  candidates.forEach(function (feature) { geometryPoints(feature.geometry, points); });
  const hull = convexHull(points);
  if (hull.length < 3) return renderedFeature.geometry;
  return { type: "Polygon", coordinates: [[...hull, hull[0]]] };
}
