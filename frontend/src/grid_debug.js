function createGridDebugState(enabled) {
  return {
    enabled: Boolean(enabled),
    zoom: null,
    resolutionM: null,
    gridVisible: true,
    activeFillLayerId: null,
    activeOutlineLayerId: null,
    activeFilter: "none",
    sourceCount: 0,
    renderedCount: 0,
    sourceResolutions: {},
    fillLayerPresent: false,
    outlineLayerPresent: false,
    fillVisibility: "none",
    outlineVisibility: "none",
    sourceLoaded: false,
    lastSourceEvent: "none",
    lastMapError: "none"
  };
}

function debugGridEnabledFromUrl(urlLike, options = {}) {
  const paramName = String(options.paramName || "debug-grid");
  let parsedUrl;
  try {
    parsedUrl = new URL(
      String(urlLike || "/"),
      String(options.baseUrl || "http://localhost")
    );
  } catch (error) {
    return false;
  }
  if (parsedUrl.pathname !== "/") {
    return false;
  }
  return parsedUrl.searchParams.get(paramName) === "1";
}

function formatSourceResolutions(counts) {
  if (!counts || typeof counts !== "object") {
    return "none";
  }
  const entries = Object.keys(counts)
    .map(function (key) {
      return [Number(key), Number(counts[key] || 0)];
    })
    .filter(function (entry) {
      return Number.isFinite(entry[0]) && entry[0] > 0 && entry[1] > 0;
    })
    .sort(function (left, right) {
      return left[0] - right[0];
    });
  if (entries.length === 0) {
    return "none";
  }
  return entries
    .map(function (entry) {
      return String(entry[0]) + ":" + String(entry[1]);
    })
    .join(", ");
}

function formatResolutionLabel(resolutionM) {
  const resolution = Number(resolutionM);
  if (!Number.isFinite(resolution) || resolution <= 0) {
    return "--";
  }
  if (resolution >= 1000) {
    const km = resolution / 1000;
    return Number.isInteger(km) ? km + "km" : km.toFixed(1) + "km";
  }
  return resolution + "m";
}

function formatZoomLabel(zoom) {
  return Number.isFinite(Number(zoom))
    ? "z" + Number(zoom).toFixed(2)
    : "--";
}

function formatGridDebugFilter(filter) {
  if (
    Array.isArray(filter) &&
    filter[0] === "==" &&
    Array.isArray(filter[1]) &&
    filter[1][0] === "get" &&
    filter.length >= 3
  ) {
    return String(filter[1][1]) + " == " + String(filter[2]);
  }
  if (typeof filter === "string" && filter.trim()) {
    return filter;
  }
  if (filter == null) {
    return "none";
  }
  try {
    return JSON.stringify(filter);
  } catch (error) {
    return String(filter);
  }
}

function formatLayerStatus(present, visibility) {
  const presence = present ? "present" : "missing";
  const visible = visibility === "visible" ? "visible" : "none";
  return presence + " + " + visible;
}

function formatLayerId(layerId) {
  return layerId ? String(layerId) : "none";
}

function formatCount(value) {
  return String(Number(value || 0));
}

function gridDebugDiagnosis(debugState) {
  if (!debugState || !debugState.enabled) {
    return "Grid diagnostics are disabled";
  }
  if (!debugState.sourceLoaded) {
    return "Livability tiles still loading";
  }
  if (
    !debugState.activeFillLayerId ||
    !debugState.activeOutlineLayerId ||
    !debugState.fillLayerPresent ||
    !debugState.outlineLayerPresent
  ) {
    return "Active grid layer was not rebuilt correctly";
  }
  if (!debugState.gridVisible) {
    return "Grid toggle is off, so active grid layers are hidden";
  }
  if (debugState.fillVisibility === "none" || debugState.outlineVisibility === "none") {
    return "Active grid layers are present but hidden";
  }
  if (Number(debugState.sourceCount || 0) === 0) {
    return "No matching grid features in the current viewport";
  }
  if (Number(debugState.sourceCount || 0) > 0 && Number(debugState.renderedCount || 0) === 0) {
    return "Tile data loaded, but grid is not rendering";
  }
  return "Grid layers are present and rendering for the current viewport";
}

function gridDebugRows(debugState) {
  return {
    enabled: debugState.enabled ? "on" : "off",
    zoom: formatZoomLabel(debugState.zoom),
    resolutionM: formatResolutionLabel(debugState.resolutionM),
    gridVisible: debugState.gridVisible ? "on" : "off",
    activeFilter: formatGridDebugFilter(debugState.activeFilter),
    sourceCount: formatCount(debugState.sourceCount),
    renderedCount: formatCount(debugState.renderedCount),
    sourceResolutions: formatSourceResolutions(debugState.sourceResolutions),
    fillLayer: formatLayerStatus(debugState.fillLayerPresent, debugState.fillVisibility),
    fillLayerId: formatLayerId(debugState.activeFillLayerId),
    outlineLayer: formatLayerStatus(debugState.outlineLayerPresent, debugState.outlineVisibility),
    outlineLayerId: formatLayerId(debugState.activeOutlineLayerId),
    sourceLoaded: debugState.sourceLoaded ? "loaded" : "loading",
    lastSourceEvent: debugState.lastSourceEvent || "none",
    lastMapError: debugState.lastMapError || "none"
  };
}

function buildGridDebugSnapshot(runtime, debugState) {
  const rows = gridDebugRows(debugState);
  return [
    "build_profile=" + String(runtime && runtime.build_profile || "unknown"),
    "zoom=" + rows.zoom,
    "active_resolution=" + rows.resolutionM,
    "source_count=" + rows.sourceCount,
    "rendered_count=" + rows.renderedCount,
    "source_resolutions=" + rows.sourceResolutions,
    "fill_layer=" + rows.fillLayer + " (" + rows.fillLayerId + ")",
    "outline_layer=" + rows.outlineLayer + " (" + rows.outlineLayerId + ")",
    "filter=" + rows.activeFilter,
    "last_source_event=" + rows.lastSourceEvent,
    "last_map_error=" + rows.lastMapError
  ].join("\n");
}

function renderGridDebugCard(elements, runtime, debugState, options = {}) {
  if (!elements || !elements.section) return;
  elements.section.hidden = !debugState.enabled;
  if (!debugState.enabled) {
    return;
  }

  const rows = gridDebugRows(debugState);
  if (elements.diagnosis) {
    elements.diagnosis.textContent = gridDebugDiagnosis(debugState);
  }
  Object.entries(rows).forEach(function (entry) {
    const key = entry[0];
    const value = entry[1];
    if (!elements.values || !elements.values[key]) return;
    elements.values[key].textContent = value;
  });

  if (elements.snapshotFallback) {
    const fallbackText = String(options.snapshotFallbackText || "");
    elements.snapshotFallback.hidden = fallbackText.length === 0;
    elements.snapshotFallback.textContent = fallbackText;
  }
}

function formatSourceEventLabel(event) {
  if (!event || !event.type) {
    return "none";
  }
  const parts = [String(event.type)];
  const detail = event.sourceDataType || event.dataType || "";
  if (detail) {
    parts.push(String(detail));
  }
  if (event.isSourceLoaded) {
    parts.push("idle");
  }
  return parts.join("/");
}

async function copyGridDebugSnapshot(clipboard, snapshotText, onFallback) {
  try {
    if (!clipboard || typeof clipboard.writeText !== "function") {
      throw new Error("Clipboard write unavailable");
    }
    await clipboard.writeText(snapshotText);
    if (typeof onFallback === "function") {
      onFallback("");
    }
    return true;
  } catch (error) {
    if (typeof onFallback === "function") {
      onFallback(snapshotText, error);
    }
    return false;
  }
}

export {
  buildGridDebugSnapshot,
  copyGridDebugSnapshot,
  createGridDebugState,
  debugGridEnabledFromUrl,
  formatGridDebugFilter,
  formatResolutionLabel,
  formatSourceEventLabel,
  formatSourceResolutions,
  gridDebugDiagnosis,
  renderGridDebugCard
};
