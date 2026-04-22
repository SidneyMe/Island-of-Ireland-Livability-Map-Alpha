const BASEMAP_RASTER = "https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png";
const ACTIVE_GRID_FILL_LAYER_ID = "grid-fill-active";
const ACTIVE_GRID_OUTLINE_LAYER_ID = "grid-outline-active";
const ACTIVE_DEBUG_GRID_LAYER_ID = "grid-fill-debug-active";
const GRID_SOURCE_ID = "livability";
const GRID_SOURCE_LAYER_ID = "grid";
const GRID_INSERT_BEFORE_LAYER_ID = "amenities-circle";

function runtimeZoomBreaks(runtime) {
  const breaks = runtime && Array.isArray(runtime.surface_zoom_breaks)
    ? runtime.surface_zoom_breaks
    : [];
  return breaks
    .map(function (entry) {
      return {
        minZoom: Number(entry.min_zoom),
        resolutionM: Number(entry.resolution_m)
      };
    })
    .sort(function (left, right) {
      return right.minZoom - left.minZoom;
    });
}

function runtimeFineResolutions(runtime) {
  const values = runtime && Array.isArray(runtime.fine_resolutions_m)
    ? runtime.fine_resolutions_m
    : [];
  return values.map(function (value) {
    return Number(value);
  });
}

function fineSurfaceEnabled(runtime) {
  return Boolean(runtime && runtime.fine_surface_enabled);
}

function gridResolutions(runtime) {
  const seen = new Set();
  const resolutions = [];
  runtimeZoomBreaks(runtime).forEach(function (entry) {
    if (seen.has(entry.resolutionM)) return;
    seen.add(entry.resolutionM);
    resolutions.push(entry.resolutionM);
  });
  return resolutions;
}

function gridFillLayerId(resolutionM) {
  return ACTIVE_GRID_FILL_LAYER_ID;
}

function gridOutlineLayerId(resolutionM) {
  return ACTIVE_GRID_OUTLINE_LAYER_ID;
}

function gridLayerId(resolutionM) {
  return gridFillLayerId(resolutionM);
}

function gridDebugLayerId(resolutionM) {
  return ACTIVE_DEBUG_GRID_LAYER_ID;
}

function gridFillLayerIds(runtime) {
  return gridResolutions(runtime).length > 0 ? [gridFillLayerId()] : [];
}

function gridOutlineLayerIds(runtime) {
  return gridResolutions(runtime).length > 0 ? [gridOutlineLayerId()] : [];
}

function gridLayerIds(runtime) {
  return gridFillLayerIds(runtime);
}

function gridLayerPairs(runtime) {
  return gridResolutions(runtime).length > 0
    ? [{
      fillLayerId: gridFillLayerId(),
      outlineLayerId: gridOutlineLayerId()
    }]
    : [];
}

function debugGridLayerIds(runtime) {
  return gridResolutions(runtime).length > 0 ? [gridDebugLayerId()] : [];
}

function activeGridResolution(runtime, zoom) {
  return resolutionForZoom(runtime, zoom);
}

function activeGridLayerId(runtime, zoom) {
  return gridFillLayerId();
}

function activeGridOutlineLayerId(runtime, zoom) {
  return gridOutlineLayerId();
}

function activeDebugGridLayerId(runtime, zoom) {
  return gridDebugLayerId();
}

function gridFilterForResolution(resolutionM) {
  return ["==", ["get", "resolution_m"], Number(resolutionM)];
}

function activeGridFilter(runtime, zoom) {
  return gridFilterForResolution(activeGridResolution(runtime, zoom));
}

function activeDebugGridFilter(runtime, zoom) {
  return activeGridFilter(runtime, zoom);
}

function gridZoomRange(runtime) {
  const breaks = runtimeZoomBreaks(runtime);
  return {
    minZoom: breaks.length > 0 ? breaks[breaks.length - 1].minZoom : 0,
    maxZoom: Number(runtime && runtime.max_zoom || 19) + 1
  };
}

function buildActiveGridLayers(runtime, resolutionM) {
  const range = gridZoomRange(runtime);
  const filter = gridFilterForResolution(resolutionM);
  return [
    {
      id: gridFillLayerId(),
      type: "fill",
      source: GRID_SOURCE_ID,
      "source-layer": GRID_SOURCE_LAYER_ID,
      minzoom: range.minZoom,
      maxzoom: range.maxZoom,
      filter: filter,
      layout: { visibility: "none" },
      paint: {
        "fill-color": gridFillColorExpression(),
        "fill-antialias": false,
        "fill-opacity": 0.52
      }
    },
    {
      id: gridOutlineLayerId(),
      type: "line",
      source: GRID_SOURCE_ID,
      "source-layer": GRID_SOURCE_LAYER_ID,
      minzoom: range.minZoom,
      maxzoom: range.maxZoom,
      filter: filter,
      layout: {
        visibility: "none",
        "line-cap": "butt",
        "line-join": "miter"
      },
      paint: {
        "line-color": "#334155",
        "line-opacity": gridOutlineOpacityExpression(),
        "line-width": gridOutlineWidthExpression()
      }
    },
    {
      id: gridDebugLayerId(),
      type: "fill",
      source: GRID_SOURCE_ID,
      "source-layer": GRID_SOURCE_LAYER_ID,
      minzoom: range.minZoom,
      maxzoom: range.maxZoom,
      filter: filter,
      layout: { visibility: "none" },
      paint: {
        "fill-color": "#222222",
        "fill-opacity": 0.1,
        "fill-outline-color": "#111111"
      }
    }
  ];
}

function activeGridLifecycle(runtime, previousResolutionM, zoom) {
  const resolutionM = activeGridResolution(runtime, zoom);
  return {
    resolutionM: resolutionM,
    rebuild: Number(previousResolutionM) !== Number(resolutionM),
    filter: gridFilterForResolution(resolutionM),
    layerDefinitions: buildActiveGridLayers(runtime, resolutionM)
  };
}

function formatResolutionLabel(resolutionM) {
  const resolution = Number(resolutionM);
  if (resolution >= 1000) {
    const km = resolution / 1000;
    return Number.isInteger(km) ? km + "km" : km.toFixed(1) + "km";
  }
  return resolution + "m";
}

function debugGridStatusMessage(resolutionM, zoom, sourceCount, renderedCount) {
  return (
    "Grid debug: " +
    formatResolutionLabel(resolutionM) +
    " at z" +
    Number(zoom).toFixed(2) +
    " -> source " +
    Number(sourceCount) +
    ", rendered " +
    Number(renderedCount)
  );
}

function gridVisibilityPlan(runtime, zoom, enabled) {
  return gridLayerPairs(runtime).flatMap(function (entry) {
    return [
      {
        layerId: entry.fillLayerId,
        visibility: enabled ? "visible" : "none"
      },
      {
        layerId: entry.outlineLayerId,
        visibility: enabled ? "visible" : "none"
      }
    ];
  });
}

function debugGridVisibilityPlan(runtime, zoom, enabled) {
  const activeLayer = enabled ? activeDebugGridLayerId(runtime, zoom) : null;
  return debugGridLayerIds(runtime).map(function (layerId) {
    return {
      layerId: layerId,
      visibility: layerId === activeLayer ? "visible" : "none"
    };
  });
}

function resolutionForZoom(runtime, zoom) {
  const zoomValue = Math.floor(Number(zoom || 0));
  const breaks = runtimeZoomBreaks(runtime);
  for (const entry of breaks) {
    if (zoomValue >= entry.minZoom) {
      return entry.resolutionM;
    }
  }
  const coarse = runtime && Array.isArray(runtime.coarse_vector_resolutions_m)
    ? runtime.coarse_vector_resolutions_m
    : [];
  return coarse.length > 0 ? Number(coarse[0]) : 20000;
}

function zoomBoundsForResolution(runtime, resolutionM) {
  const breaks = runtimeZoomBreaks(runtime);
  for (let index = 0; index < breaks.length; index += 1) {
    if (breaks[index].resolutionM !== resolutionM) continue;
    if (index === 0) {
      return { minZoom: breaks[index].minZoom, maxZoom: Number(runtime.max_zoom || 19) + 1 };
    }
    return {
      minZoom: breaks[index].minZoom,
      maxZoom: breaks[index - 1].minZoom
    };
  }
  return { minZoom: 0, maxZoom: Number(runtime.max_zoom || 19) + 1 };
}

function gridFillColorExpression() {
  return [
    "interpolate",
    ["linear"],
    ["coalesce", ["get", "total_score"], 0],
    0, "#440154",
    25, "#3b528b",
    50, "#21908c",
    75, "#5dc863",
    100, "#fde725"
  ];
}

function gridOutlineOpacityExpression() {
  return [
    "interpolate",
    ["linear"],
    ["zoom"],
    5, 0.18,
    12, 0.24,
    15, 0.30,
    19, 0.38
  ];
}

function gridOutlineWidthExpression() {
  return [
    "interpolate",
    ["linear"],
    ["zoom"],
    5, 0.35,
    12, 0.45,
    15, 0.55,
    19, 0.75
  ];
}

function amenityCircleColorExpression(colors) {
  const expr = ["match", ["get", "category"]];
  Object.keys(colors).forEach(function (category) {
    expr.push(category, colors[category]);
  });
  expr.push("#888888");
  return expr;
}

function transportRealityCircleColorExpression() {
  return [
    "match",
    ["get", "reality_status"],
    "active_confirmed", "#1a9850",
    "inactive_confirmed", "#d73027",
    "school_only_confirmed", "#fdae61",
    "#666666"
  ];
}

function transportRealityCircleStrokeColorExpression() {
  return "#ffffff";
}

function transportRealityCircleStrokeWidthExpression() {
  return 1;
}

function serviceDesertFillColorExpression() {
  return [
    "interpolate",
    ["linear"],
    ["coalesce", ["get", "baseline_reachable_stop_count"], 0],
    1, "#fee08b",
    3, "#f46d43",
    6, "#a50026"
  ];
}

function buildStyle(runtime, options = {}) {
  const colors = runtime.category_colors || {};
  const origin = options.windowOrigin || "http://127.0.0.1:8000";
  const pmtilesUrl = "pmtiles://" + origin + (runtime.pmtiles_url || "/tiles/livability.pmtiles");
  const layers = [{ id: "basemap", type: "raster", source: "basemap" }];
  buildActiveGridLayers(
    runtime,
    activeGridResolution(runtime, Number(runtime.default_zoom || 0))
  ).forEach(function (layer) {
    layers.push(layer);
  });

  layers.push({
    id: "amenities-circle",
    type: "circle",
    source: "livability",
    "source-layer": "amenities",
    layout: { visibility: "none" },
    filter: ["in", ["get", "category"], ["literal", []]],
    paint: {
      "circle-radius": [
        "interpolate", ["linear"], ["zoom"],
        9, 2,
        19, 7
      ],
      "circle-color": amenityCircleColorExpression(colors),
      "circle-stroke-color": "#ffffff",
      "circle-stroke-width": 1
    }
  });

  layers.push({
    id: "service-deserts-fill",
    type: "fill",
    source: "livability",
    "source-layer": "service_deserts",
    layout: { visibility: "none" },
    paint: {
      "fill-color": serviceDesertFillColorExpression(),
      "fill-opacity": 0.24,
      "fill-outline-color": "#a63603"
    }
  });

  layers.push({
    id: "transport-reality-circle",
    type: "circle",
    source: "livability",
    "source-layer": "transport_reality",
    minzoom: 9,
    layout: { visibility: "none" },
    paint: {
      "circle-radius": [
        "interpolate", ["linear"], ["zoom"],
        9, 2.5,
        19, 8
      ],
      "circle-color": transportRealityCircleColorExpression(),
      "circle-opacity": 0.82,
      "circle-stroke-color": transportRealityCircleStrokeColorExpression(),
      "circle-stroke-width": transportRealityCircleStrokeWidthExpression()
    }
  });

  return {
    version: 8,
    glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
    sources: {
      basemap: {
        type: "raster",
        tiles: [BASEMAP_RASTER],
        tileSize: 256,
        attribution: "&copy; OpenStreetMap contributors &copy; CARTO"
      },
      livability: {
        type: "vector",
        url: pmtilesUrl
      },
    },
    layers: layers
  };
}

export {
  GRID_INSERT_BEFORE_LAYER_ID,
  GRID_SOURCE_ID,
  GRID_SOURCE_LAYER_ID,
  activeDebugGridFilter,
  activeDebugGridLayerId,
  activeGridFilter,
  activeGridLayerId,
  activeGridLifecycle,
  activeGridOutlineLayerId,
  activeGridResolution,
  buildStyle,
  buildActiveGridLayers,
  debugGridVisibilityPlan,
  debugGridStatusMessage,
  debugGridLayerIds,
  fineSurfaceEnabled,
  gridFilterForResolution,
  gridFillLayerId,
  gridFillLayerIds,
  gridOutlineLayerId,
  gridOutlineLayerIds,
  gridVisibilityPlan,
  gridLayerIds,
  resolutionForZoom,
  runtimeFineResolutions,
  runtimeZoomBreaks,
  zoomBoundsForResolution
};
