import assert from "node:assert/strict";

import {
  GRID_INSERT_BEFORE_LAYER_ID,
  activeDebugGridFilter,
  activeDebugGridLayerId,
  activeGridFilter,
  activeGridLayerId,
  activeGridLifecycle,
  activeGridOutlineLayerId,
  buildActiveGridLayers,
  buildStyle,
  debugGridVisibilityPlan,
  debugGridStatusMessage,
  gridFillLayerId,
  gridFillLayerIds,
  gridVisibilityPlan,
  gridLayerIds,
  gridOutlineLayerId,
  gridOutlineLayerIds,
  resolutionForZoom,
  zoomBoundsForResolution
} from "./runtime_contract.js";

const fullRuntime = {
  build_profile: "full",
  pmtiles_url: "/tiles/livability.pmtiles",
  transport_reality_download_url: "/exports/transport-reality.zip",
  coarse_vector_resolutions_m: [20000, 10000, 5000],
  fine_resolutions_m: [2500, 1000, 500, 250, 100, 50],
  fine_surface_enabled: true,
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
  transport_reality_enabled: true,
  service_deserts_enabled: true,
  noise_enabled: true,
  max_zoom: 19
};

const devRuntime = {
  build_profile: "dev",
  pmtiles_url: "/tiles/livability-dev.pmtiles",
  transport_reality_download_url: "/exports/transport-reality.zip",
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
  transport_reality_enabled: true,
  service_deserts_enabled: true,
  noise_enabled: true,
  max_zoom: 19
};

{
  const style = buildStyle(devRuntime, { windowOrigin: "http://127.0.0.1:8000" });
  const gridFillLayers = style.layers.filter(function (layer) {
    return layer.id === "grid-fill-active";
  });
  const gridOutlineLayers = style.layers.filter(function (layer) {
    return layer.id === "grid-outline-active";
  });
  const debugGridLayers = style.layers.filter(function (layer) {
    return layer.id === "grid-fill-debug-active";
  });

  assert.equal(gridFillLayers.length, 1);
  assert.equal(gridOutlineLayers.length, 1);
  assert.equal(debugGridLayers.length, 1);
  assert.equal(gridFillLayerIds(devRuntime).length, 1);
  assert.equal(gridOutlineLayerIds(devRuntime).length, 1);
  assert.deepEqual(gridLayerIds(devRuntime), ["grid-fill-active"]);
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
  assert.equal(activeGridLayerId(fullRuntime, 12), "grid-fill-active");
  assert.equal(activeGridOutlineLayerId(fullRuntime, 12), "grid-outline-active");
  assert.equal(activeDebugGridLayerId(fullRuntime, 12), "grid-fill-debug-active");
  assert.equal(activeGridLayerId(fullRuntime, 18), "grid-fill-active");
  assert.equal(activeGridOutlineLayerId(fullRuntime, 18), "grid-outline-active");

  assert.deepEqual(activeGridFilter(fullRuntime, 12), ["==", ["get", "resolution_m"], 2500]);
  assert.deepEqual(activeGridFilter(fullRuntime, 13), ["==", ["get", "resolution_m"], 1000]);
  assert.deepEqual(activeGridFilter(fullRuntime, 14), ["==", ["get", "resolution_m"], 500]);
  assert.deepEqual(activeGridFilter(fullRuntime, 15), ["==", ["get", "resolution_m"], 250]);
  assert.deepEqual(activeGridFilter(fullRuntime, 16), ["==", ["get", "resolution_m"], 100]);
  assert.deepEqual(activeGridFilter(fullRuntime, 18), ["==", ["get", "resolution_m"], 50]);
  assert.deepEqual(activeDebugGridFilter(fullRuntime, 18), ["==", ["get", "resolution_m"], 50]);

  assert.deepEqual(gridVisibilityPlan(fullRuntime, 16, true), [
    { layerId: "grid-fill-active", visibility: "visible" },
    { layerId: "grid-outline-active", visibility: "visible" }
  ]);
  assert.deepEqual(gridVisibilityPlan(fullRuntime, 16, false), [
    { layerId: "grid-fill-active", visibility: "none" },
    { layerId: "grid-outline-active", visibility: "none" }
  ]);
  assert.deepEqual(debugGridVisibilityPlan(fullRuntime, 14, true), [
    { layerId: "grid-fill-debug-active", visibility: "visible" }
  ]);
  assert.deepEqual(debugGridVisibilityPlan(fullRuntime, 18, false), [
    { layerId: "grid-fill-debug-active", visibility: "none" }
  ]);
}

{
  const layers2500 = buildActiveGridLayers(fullRuntime, 2500);
  const layers500 = buildActiveGridLayers(fullRuntime, 500);
  const layers50 = buildActiveGridLayers(fullRuntime, 50);
  const lifecycle2500 = activeGridLifecycle(fullRuntime, null, 12);
  const lifecycle1000 = activeGridLifecycle(fullRuntime, 2500, 13);
  const lifecycle500 = activeGridLifecycle(fullRuntime, 1000, 14);
  const lifecycle1000Stable = activeGridLifecycle(fullRuntime, 1000, 13);

  assert.equal(GRID_INSERT_BEFORE_LAYER_ID, "noise-fill");
  assert.equal(layers2500.length, 3);
  assert.equal(layers2500[0].id, "grid-fill-active");
  assert.equal(layers2500[1].id, "grid-outline-active");
  assert.equal(layers2500[2].id, "grid-fill-debug-active");
  assert.deepEqual(layers2500[0].filter, ["==", ["get", "resolution_m"], 2500]);
  assert.deepEqual(layers2500[1].filter, ["==", ["get", "resolution_m"], 2500]);
  assert.deepEqual(layers2500[2].filter, ["==", ["get", "resolution_m"], 2500]);
  assert.deepEqual(layers500[0].filter, ["==", ["get", "resolution_m"], 500]);
  assert.deepEqual(layers500[1].filter, ["==", ["get", "resolution_m"], 500]);
  assert.deepEqual(layers500[2].filter, ["==", ["get", "resolution_m"], 500]);
  assert.equal(layers2500[0].paint["fill-antialias"], false);
  assert.equal(layers2500[0].paint["fill-opacity"], 0.52);
  assert.equal(layers50[1].paint["line-color"], "#334155");
  assert.deepEqual(layers50[0].filter, ["==", ["get", "resolution_m"], 50]);

  assert.equal(lifecycle2500.resolutionM, 2500);
  assert.equal(lifecycle2500.rebuild, true);
  assert.deepEqual(lifecycle2500.filter, ["==", ["get", "resolution_m"], 2500]);
  assert.equal(lifecycle1000.resolutionM, 1000);
  assert.equal(lifecycle1000.rebuild, true);
  assert.equal(lifecycle500.resolutionM, 500);
  assert.equal(lifecycle500.rebuild, true);
  assert.deepEqual(lifecycle500.filter, ["==", ["get", "resolution_m"], 500]);
  assert.deepEqual(
    lifecycle500.layerDefinitions.map(function (layer) {
      return layer.filter;
    }),
    [
      ["==", ["get", "resolution_m"], 500],
      ["==", ["get", "resolution_m"], 500],
      ["==", ["get", "resolution_m"], 500]
    ]
  );
  assert.equal(lifecycle1000Stable.resolutionM, 1000);
  assert.equal(lifecycle1000Stable.rebuild, false);
  assert.equal(
    debugGridStatusMessage(500, 14, 20, 0),
    "Grid debug: 500m at z14.00 -> source 20, rendered 0"
  );
}

{
  const style = buildStyle(fullRuntime, { windowOrigin: "http://127.0.0.1:8000" });
  const gridFillLayers = style.layers.filter(function (layer) {
    return layer.id === "grid-fill-active";
  });
  const gridOutlineLayers = style.layers.filter(function (layer) {
    return layer.id === "grid-outline-active";
  });
  const transportRealityLayer = style.layers.find(function (layer) {
    return layer.id === "transport-reality-circle";
  });
  const noiseLayer = style.layers.find(function (layer) {
    return layer.id === "noise-fill";
  });
  const serviceDesertsLayer = style.layers.find(function (layer) {
    return layer.id === "service-deserts-fill";
  });
  const activeFillLayer = style.layers.find(function (layer) {
    return layer.id === "grid-fill-active";
  });
  const activeOutlineLayer = style.layers.find(function (layer) {
    return layer.id === "grid-outline-active";
  });

  assert.equal(gridFillLayers.length, 1);
  assert.equal(gridOutlineLayers.length, 1);
  assert.equal(gridLayerIds(fullRuntime).length, gridFillLayerIds(fullRuntime).length);
  assert.deepEqual(gridLayerIds(fullRuntime), ["grid-fill-active"]);
  assert.equal(gridFillLayerId(2500), "grid-fill-active");
  assert.equal(gridOutlineLayerId(2500), "grid-outline-active");
  assert.equal(resolutionForZoom(fullRuntime, 12), 2500);
  assert.equal(resolutionForZoom(fullRuntime, 18), 50);
  assert.equal(activeFillLayer["source-layer"], "grid");
  assert.deepEqual(activeFillLayer.filter, ["==", ["get", "resolution_m"], 20000]);
  assert.equal(activeFillLayer.minzoom, 0);
  assert.equal(activeFillLayer.maxzoom, 20);
  assert.equal(activeFillLayer.layout.visibility, "none");
  assert.equal(activeFillLayer.paint["fill-antialias"], false);
  assert.equal(activeFillLayer.paint["fill-opacity"], 0.52);
  assert.equal(activeOutlineLayer["source-layer"], "grid");
  assert.deepEqual(activeOutlineLayer.filter, ["==", ["get", "resolution_m"], 20000]);
  assert.equal(activeOutlineLayer.layout.visibility, "none");
  assert.equal(activeOutlineLayer.paint["line-color"], "#334155");
  assert.deepEqual(activeOutlineLayer.paint["line-opacity"], [
    "interpolate", ["linear"], ["zoom"],
    5, 0.18,
    12, 0.24,
    15, 0.30,
    19, 0.38
  ]);
  assert.deepEqual(activeOutlineLayer.paint["line-width"], [
    "interpolate", ["linear"], ["zoom"],
    5, 0.35,
    12, 0.45,
    15, 0.55,
    19, 0.75
  ]);
  assert.equal(transportRealityLayer.source, "livability");
  assert.equal(noiseLayer.source, "livability");
  assert.equal(noiseLayer["source-layer"], "noise");
  assert.equal(noiseLayer.minzoom, 8);
  assert.deepEqual(noiseLayer.filter, ["all", ["==", ["get", "metric"], "Lden"]]);
  assert.deepEqual(noiseLayer.paint["fill-color"], [
    "interpolate", ["linear"], ["coalesce", ["get", "db_low"], 0],
    45, "#fee8c8",
    50, "#fdbb84",
    55, "#fc8d59",
    60, "#ef6548",
    65, "#d7301f",
    70, "#990000",
    75, "#67000d"
  ]);
  assert.equal(transportRealityLayer["source-layer"], "transport_reality");
  assert.equal(transportRealityLayer.minzoom, 9);
  assert.equal(transportRealityLayer.paint["circle-stroke-color"], "#ffffff");
  assert.equal(transportRealityLayer.paint["circle-stroke-width"], 1);
  assert.deepEqual(transportRealityLayer.paint["circle-color"], [
    "case",
    ["==", ["get", "is_unscheduled_stop"], 1], "#8c8778",
    ["in", ",tram,", ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]], "#8f4bb8",
    ["in", ",rail,", ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]], "#2f6fb6",
    [
      "match",
      ["coalesce", ["get", "bus_service_subtier"], ""],
      "mon_sun", "#1f7a4d",
      "mon_sat", "#1d7874",
      "tue_sun", "#3b6fb6",
      "weekdays_only", "#c07a1c",
      "weekends_only", "#b84b5e",
      "single_day_only", "#c05621",
      "partial_week", "#5f6b7a",
      "#9aa1a6"
    ]
  ]);
  assert.deepEqual(transportRealityLayer.paint["circle-opacity"], [
    "case",
    ["==", ["get", "is_unscheduled_stop"], 1], 0.8,
    ["in", ",tram,", ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]], 0.84,
    ["in", ",rail,", ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]], 0.84,
    ["==", ["coalesce", ["get", "bus_service_subtier"], ""], ""], 0.42,
    0.84
  ]);
  assert.equal(style.sources["transport-reality"], undefined);
  assert.equal(serviceDesertsLayer["source-layer"], "service_deserts");
}

console.log("frontend runtime contract checks passed");
