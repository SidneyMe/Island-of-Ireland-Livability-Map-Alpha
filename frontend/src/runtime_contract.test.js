import assert from "node:assert/strict";

import {
  buildStyle,
  resolutionForZoom,
  zoomBoundsForResolution
} from "./runtime_contract.js";

const fullRuntime = {
  build_profile: "full",
  pmtiles_url: "/tiles/livability.pmtiles",
  coarse_vector_resolutions_m: [20000, 10000, 5000],
  fine_resolutions_m: [2500, 1000, 500, 250, 100, 50],
  fine_surface_enabled: true,
  surface_tile_url_template: "/tiles/surface/{resolution_m}/{z}/{x}/{y}.png",
  surface_zoom_breaks: [
    { min_zoom: 18, resolution_m: 50 },
    { min_zoom: 16, resolution_m: 100 },
    { min_zoom: 15, resolution_m: 250 },
    { min_zoom: 14, resolution_m: 500 },
    { min_zoom: 13, resolution_m: 1000 },
    { min_zoom: 12, resolution_m: 2500 },
    { min_zoom: 10, resolution_m: 5000 },
    { min_zoom: 8, resolution_m: 10000 },
    { min_zoom: 0, resolution_m: 20000 }
  ],
  category_colors: {
    shops: "#2166ac",
    transport: "#762a83",
    healthcare: "#d6604d",
    parks: "#1a9850"
  },
  max_zoom: 19
};

const devRuntime = {
  build_profile: "dev",
  pmtiles_url: "/tiles/livability-dev.pmtiles",
  coarse_vector_resolutions_m: [20000, 10000, 5000],
  fine_resolutions_m: [],
  fine_surface_enabled: false,
  surface_zoom_breaks: [
    { min_zoom: 10, resolution_m: 5000 },
    { min_zoom: 8, resolution_m: 10000 },
    { min_zoom: 0, resolution_m: 20000 }
  ],
  category_colors: {
    shops: "#2166ac",
    transport: "#762a83",
    healthcare: "#d6604d",
    parks: "#1a9850"
  },
  max_zoom: 19
};

{
  const style = buildStyle(devRuntime, { windowOrigin: "http://127.0.0.1:8000" });
  const sourceKeys = Object.keys(style.sources).filter(function (key) {
    return key.startsWith("surface-");
  });
  const surfaceLayers = style.layers.filter(function (layer) {
    return String(layer.id || "").startsWith("surface-");
  });

  assert.deepEqual(sourceKeys, []);
  assert.deepEqual(surfaceLayers, []);
  assert.equal(
    style.sources.livability.url,
    "pmtiles://http://127.0.0.1:8000/tiles/livability-dev.pmtiles",
  );
}

{
  assert.equal(resolutionForZoom(devRuntime, 7), 20000);
  assert.equal(resolutionForZoom(devRuntime, 8), 10000);
  assert.equal(resolutionForZoom(devRuntime, 10), 5000);
  assert.equal(resolutionForZoom(devRuntime, 18), 5000);
  assert.deepEqual(zoomBoundsForResolution(devRuntime, 5000), { minZoom: 10, maxZoom: 20 });
}

{
  const style = buildStyle(fullRuntime, { windowOrigin: "http://127.0.0.1:8000" });
  const surfaceLayers = style.layers.filter(function (layer) {
    return String(layer.id || "").startsWith("surface-");
  });

  assert.equal(surfaceLayers.length, fullRuntime.fine_resolutions_m.length);
  assert.equal(resolutionForZoom(fullRuntime, 12), 2500);
  assert.equal(resolutionForZoom(fullRuntime, 18), 50);
}

console.log("frontend runtime contract checks passed");
