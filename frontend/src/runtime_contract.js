const BASEMAP_RASTER = "https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png";

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

function fineSurfaceSource(runtime, resolutionM) {
  const template = String(runtime.surface_tile_url_template || "");
  return {
    type: "raster",
    tiles: [template.replace("{resolution_m}", String(resolutionM))],
    tileSize: 256
  };
}


function buildStyle(runtime, options = {}) {
  const colors = runtime.category_colors || {};
  const origin = options.windowOrigin || "http://127.0.0.1:8000";
  const pmtilesUrl = "pmtiles://" + origin + (runtime.pmtiles_url || "/tiles/livability.pmtiles");
  const fineSources = {};
  runtimeFineResolutions(runtime).forEach(function (resolutionM) {
    fineSources["surface-" + resolutionM] = fineSurfaceSource(runtime, resolutionM);
  });

  const layers = [
    { id: "basemap", type: "raster", source: "basemap" },
    {
      id: "grid-fill-coarse",
      type: "fill",
      source: "livability",
      "source-layer": "grid",
      maxzoom: 12,
      paint: {
        "fill-color": gridFillColorExpression(),
        "fill-opacity": 0.58
      }
    }
  ];

  if (fineSurfaceEnabled(runtime)) {
    runtimeFineResolutions(runtime).forEach(function (resolutionM) {
      const zoomBounds = zoomBoundsForResolution(runtime, resolutionM);
      layers.push({
        id: "surface-" + resolutionM,
        type: "raster",
        source: "surface-" + resolutionM,
        minzoom: zoomBounds.minZoom,
        maxzoom: zoomBounds.maxZoom,
        paint: {
          "raster-opacity": 0.82,
          "raster-resampling": "nearest",
          "raster-fade-duration": 0
        }
      });
    });
  }

  layers.push({
    id: "grid-fill-debug",
    type: "fill",
    source: "livability",
    "source-layer": "grid",
    layout: { visibility: "none" },
    paint: {
      "fill-color": "#222222",
      "fill-opacity": 0.1,
      "fill-outline-color": "#111111"
    }
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
...fineSources
    },
    layers: layers
  };
}

export {
  buildStyle,
  fineSurfaceEnabled,
  resolutionForZoom,
  runtimeFineResolutions,
  runtimeZoomBreaks,
  zoomBoundsForResolution
};
