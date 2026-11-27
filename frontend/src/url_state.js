const SCORE_LAYERS = new Set(["combined", "shops", "transport", "healthcare", "parks"]);
const TRAVEL_MODES = new Set(["walk", "bike", "mode_aware"]);

function finiteNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function coordinates(value) {
  const parts = String(value || "").split(",");
  if (parts.length !== 2) return null;
  const lat = finiteNumber(parts[0]);
  const lon = finiteNumber(parts[1]);
  if (lat === null || lon === null || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return null;
  }
  return { lat, lon };
}

function view(value, runtime = {}) {
  const parts = String(value || "").split(",");
  if (parts.length !== 3) return null;
  const point = coordinates(parts.slice(0, 2).join(","));
  const zoom = finiteNumber(parts[2]);
  const minZoom = Number(runtime.min_zoom ?? 5);
  const maxZoom = Number(runtime.max_zoom ?? 19);
  if (!point || zoom === null || zoom < minZoom || zoom > maxZoom) return null;
  return { ...point, zoom };
}

function availableModes(runtime = {}) {
  const declared = runtime.daily_access && Array.isArray(runtime.daily_access.available_modes)
    ? runtime.daily_access.available_modes.map(String)
    : ["walk"];
  return new Set(declared.filter(function (mode) { return TRAVEL_MODES.has(mode); }));
}

function compareCoordinates(value) {
  const points = String(value || "").split(";").map(coordinates).filter(Boolean);
  return points.length === 2 ? points : null;
}

export function parseMapUrlState(href, runtime = {}) {
  const url = new URL(String(href), "http://localhost/");
  const params = new URLSearchParams(url.hash.replace(/^#/, ""));
  const mode = String(params.get("m") || "");
  const layer = String(params.get("c") || "");
  const buildKey = String(params.get("b") || "").trim();
  return {
    view: view(params.get("v"), runtime),
    mode: availableModes(runtime).has(mode) ? mode : null,
    layer: SCORE_LAYERS.has(layer) ? layer : null,
    buildKey: buildKey && buildKey.length <= 256 ? buildKey : null,
    inspect: coordinates(params.get("cell")),
    compare: compareCoordinates(params.get("cmp"))
  };
}

function rounded(value, digits) {
  return Number(value).toFixed(digits).replace(/\.?0+$/, "");
}

export function mapPermalink(href, values) {
  const url = new URL(String(href), "http://localhost/");
  const params = new URLSearchParams();
  const lat = finiteNumber(values && values.lat);
  const lon = finiteNumber(values && values.lon);
  const zoom = finiteNumber(values && values.zoom);
  if (lat !== null && lon !== null && zoom !== null) {
    params.set("v", [rounded(lat, 5), rounded(lon, 5), rounded(zoom, 2)].join(","));
  }
  const mode = String(values && values.mode || "walk");
  const layer = String(values && values.layer || "combined");
  params.set("m", TRAVEL_MODES.has(mode) ? mode : "walk");
  params.set("c", SCORE_LAYERS.has(layer) ? layer : "combined");
  const buildKey = String(values && values.buildKey || "").trim();
  if (buildKey) params.set("b", buildKey);
  if (values && values.inspect) {
    const inspect = coordinates(values.inspect.lat + "," + values.inspect.lon);
    if (inspect) params.set("cell", rounded(inspect.lat, 5) + "," + rounded(inspect.lon, 5));
  }
  if (values && Array.isArray(values.compare) && values.compare.length === 2) {
    const points = values.compare.map(function (point) {
      const parsed = coordinates(point.lat + "," + point.lon);
      return parsed ? rounded(parsed.lat, 5) + "," + rounded(parsed.lon, 5) : null;
    });
    if (points.every(Boolean)) {
      params.delete("cell");
      params.set("cmp", points.join(";"));
    }
  }
  url.hash = params.toString();
  return url.toString();
}

export function linkedBuildUnavailable(urlState, runtime) {
  return Boolean(
    urlState && urlState.buildKey && runtime && runtime.build_key
    && String(urlState.buildKey) !== String(runtime.build_key)
  );
}

