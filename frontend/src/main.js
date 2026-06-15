import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import "./main.css";
import {
  buildAmenityLayerFilter,
  defaultAmenityTierSelections,
  formatAmenityLabel,
  tierOptionsForCategory
} from "./amenity_filters.js";
import {
  buildTransportLayerFilter,
  transportFlagCounts,
  transportBusFrequencyOptions,
  transportModeOptions,
  transportTierOptions
} from "./transport_filters.js";
import {
  buildNoiseLayerFilter,
  defaultNoiseSelections,
  noiseBandOptions,
  noiseMetricOptions,
  noiseSourceOptions
} from "./noise_filters.js";
import {
  CLICK_ACTIONS,
  resolveMapClickAction
} from "./click_priority.js";
import {
  DEFAULT_NOISE_OPACITY as runtimeDefaultNoiseOpacity,
  GRID_INSERT_BEFORE_LAYER_ID as runtimeGridInsertBeforeLayerId,
  noiseOutlineOpacity as runtimeNoiseOutlineOpacity,
  activeGridLifecycle as runtimeActiveGridLifecycle,
  activeDebugGridLayerId as runtimeActiveDebugGridLayerId,
  activeGridLayerId as runtimeActiveGridLayerId,
  activeGridOutlineLayerId as runtimeActiveGridOutlineLayerId,
  gridFillColorExpression as runtimeGridFillColorExpression,
  buildStyle as buildRuntimeStyle,
  buildActiveGridLayers as runtimeBuildActiveGridLayers,
  debugGridVisibilityPlan as runtimeDebugGridVisibilityPlan,
  fineSurfaceEnabled as runtimeFineSurfaceEnabled,
  gridScoreLayerLabel as runtimeGridScoreLayerLabel,
  gridScoreLayerOptions as runtimeGridScoreLayerOptions,
  normalizeGridScoreLayer as runtimeNormalizeGridScoreLayer,
  gridVisibilityPlan as runtimeGridVisibilityPlan,
  resolutionForZoom as runtimeResolutionForZoom
} from "./runtime_contract.js";
import {
  buildGridDebugSnapshot,
  copyGridDebugSnapshot,
  createGridDebugState,
  debugGridEnabledFromUrl,
  formatGridDebugFilter,
  formatResolutionLabel,
  formatSourceEventLabel,
  renderGridDebugCard
} from "./grid_debug.js";
import { transportRealityPopupHtml } from "./transport_reality_popup.js";
import { fetchWithTimeout } from "./api_client.js";
import { amenityPopupHtml } from "./popups/amenity_popup.js";
import { noisePopupHtml } from "./popups/noise_popup.js";
import { inspectPopupHtml, coarseGridPopupHtml } from "./popups/grid_popup.js";
import { serviceDesertPopupHtml } from "./popups/service_desert_popup.js";

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const MIN_ZOOM = 5;
const DEBUG_GRID_QUERY_PARAM = "debug-grid";
const GRID_LAYER_STORAGE_KEY = "livability-grid-layer";
const RUNTIME_FETCH_TIMEOUT_MS = 45000;
const RUNTIME_FETCH_RETRY_DELAY_MS = 1200;
const RUNTIME_FETCH_MAX_ATTEMPTS = 2;
const INSPECT_FETCH_TIMEOUT_MS = 15000;
const DEBUG_GRID_ENABLED = debugGridEnabledFromUrl(window.location.href, {
  paramName: DEBUG_GRID_QUERY_PARAM
});

const elements = {
  statusPill: document.getElementById("status-pill"),
  controlPanel: document.getElementById("control-panel"),
  panelToggle: document.getElementById("panel-toggle"),
  zoomIn: document.getElementById("zoom-in"),
  zoomOut: document.getElementById("zoom-out"),
  resolutionChip: document.getElementById("resolution-chip"),
  amenityControls: document.getElementById("amenity-controls"),
  amenityNote: document.getElementById("amenity-note"),
  transitControls: document.getElementById("transit-controls"),
  noiseControls: document.getElementById("noise-controls"),
  noiseNote: document.getElementById("noise-note"),
  gridToggle: document.getElementById("grid-toggle"),
  gridLayerControls: document.getElementById("grid-layer-controls"),
  gridLayerNote: document.getElementById("grid-layer-note"),
  map: document.getElementById("map"),
  mapStage: document.getElementById("map-stage"),
  gridDebug: {
    section: document.getElementById("grid-debug-section"),
    controls: document.getElementById("grid-debug-controls"),
    diagnosis: document.getElementById("grid-debug-diagnosis"),
    copyButton: document.getElementById("grid-debug-copy-button"),
    snapshotFallback: document.getElementById("grid-debug-snapshot-fallback"),
    values: {
      enabled: document.getElementById("grid-debug-enabled"),
      zoom: document.getElementById("grid-debug-zoom"),
      resolutionM: document.getElementById("grid-debug-resolution"),
      gridVisible: document.getElementById("grid-debug-grid-visible"),
      activeFilter: document.getElementById("grid-debug-filter"),
      sourceCount: document.getElementById("grid-debug-source-count"),
      renderedCount: document.getElementById("grid-debug-rendered-count"),
      sourceResolutions: document.getElementById("grid-debug-source-resolutions"),
      fillLayer: document.getElementById("grid-debug-fill-layer"),
      fillLayerId: document.getElementById("grid-debug-fill-layer-id"),
      outlineLayer: document.getElementById("grid-debug-outline-layer"),
      outlineLayerId: document.getElementById("grid-debug-outline-layer-id"),
      sourceLoaded: document.getElementById("grid-debug-source-loaded"),
      lastSourceEvent: document.getElementById("grid-debug-last-source-event"),
      lastMapError: document.getElementById("grid-debug-last-map-error")
    }
  }
};

const state = {
  runtime: null,
  map: null,
  popup: null,
  panelHidden: false,
  selectedGridLayer: "combined",
  enabledAmenityCategories: new Set(),
  enabledAmenityTiers: new Map(),
  gridVisible: true,
  activeGridResolutionM: null,
  debugGridVisible: false,
  gridDebug: createGridDebugState(DEBUG_GRID_ENABLED),
  gridDebugSnapshotFallbackText: "",
  transportRealityVisible: false,
  selectedTransportSubtiers: new Set(),
  selectedTransportBusFrequencies: new Set(),
  selectedTransportModes: new Set(["bus", "rail", "tram"]),
  transportIncludeUnscheduled: false,
  transportRequireExceptionOnly: false,
  serviceDesertsVisible: false,
  noiseVisible: false,
  noiseOpacity: runtimeDefaultNoiseOpacity,
  selectedNoiseMetric: "Lden",
  selectedNoiseSources: new Set(),
  selectedNoiseBands: new Set(),
  pendingInspect: null
};

function updateStatus(message) {
  elements.statusPill.textContent = message;
}

function fineSurfaceEnabled() {
  return runtimeFineSurfaceEnabled(state.runtime);
}

function resolutionForZoom(zoom) {
  return runtimeResolutionForZoom(state.runtime, zoom);
}

function activeGridLayerId() {
  const zoom = state.map ? state.map.getZoom() : 0;
  return runtimeActiveGridLayerId(state.runtime || {}, zoom);
}

function activeGridOutlineLayerId() {
  const zoom = state.map ? state.map.getZoom() : 0;
  return runtimeActiveGridOutlineLayerId(state.runtime || {}, zoom);
}

function activeDebugGridLayerId() {
  const zoom = state.map ? state.map.getZoom() : 0;
  return runtimeActiveDebugGridLayerId(state.runtime || {}, zoom);
}

function activeGridLifecycle() {
  const zoom = state.map ? state.map.getZoom() : 0;
  return runtimeActiveGridLifecycle(
    state.runtime || {},
    state.activeGridResolutionM,
    zoom,
    state.selectedGridLayer
  );
}

function setResolutionChip() {
  if (!state.map) {
    elements.resolutionChip.textContent = "Grid --";
    return;
  }
  const resolution = resolutionForZoom(state.map.getZoom());
  elements.resolutionChip.textContent = "Grid " + formatResolutionLabel(resolution);
}

function updateAmenityNote() {
  const count = state.enabledAmenityCategories.size;
  elements.amenityNote.textContent = count
    ? count + " layer" + (count === 1 ? "" : "s") + " on"
    : "Off until enabled";
}

function readSavedGridLayer() {
  try {
    const value = window.sessionStorage.getItem(GRID_LAYER_STORAGE_KEY);
    return runtimeNormalizeGridScoreLayer(value);
  } catch (error) {
    return "combined";
  }
}

function saveGridLayerSelection(value) {
  try {
    window.sessionStorage.setItem(GRID_LAYER_STORAGE_KEY, runtimeNormalizeGridScoreLayer(value));
  } catch (error) {
    // Session storage is best-effort only.
  }
}

function updateGridLayerNote() {
  if (!elements.gridLayerNote) return;
  elements.gridLayerNote.textContent = "Active layer: " + runtimeGridScoreLayerLabel(state.selectedGridLayer);
}

function applyGridLayerSelection() {
  if (!state.map) {
    updateGridLayerNote();
    return;
  }
  const layerId = activeGridLayerId();
  if (state.map.getLayer(layerId)) {
    state.map.setPaintProperty(
      layerId,
      "fill-color",
      runtimeGridFillColorExpression(state.selectedGridLayer)
    );
  }
  updateGridLayerNote();
}

function buildGridLayerControls() {
  if (!elements.gridLayerControls) return;
  elements.gridLayerControls.replaceChildren();
  const options = runtimeGridScoreLayerOptions();
  options.forEach(function (option) {
    const label = document.createElement("label");
    label.className = "toggle-row grid-layer-row";
    label.htmlFor = "grid-layer-" + option.value;

    const textWrap = document.createElement("span");
    textWrap.className = "toggle-label";

    const title = document.createElement("strong");
    title.textContent = option.label;

    const subtitle = document.createElement("span");
    subtitle.textContent = option.value === "combined"
      ? "Total score"
      : "Single-category view";

    const input = document.createElement("input");
    input.type = "radio";
    input.name = "grid-layer";
    input.id = "grid-layer-" + option.value;
    input.checked = state.selectedGridLayer === option.value;
    input.addEventListener("change", function () {
      if (!input.checked) return;
      state.selectedGridLayer = option.value;
      saveGridLayerSelection(option.value);
      applyGridLayerSelection();
    });

    textWrap.appendChild(title);
    textWrap.appendChild(subtitle);
    label.appendChild(textWrap);
    label.appendChild(input);
    elements.gridLayerControls.appendChild(label);
  });
  updateGridLayerNote();
}

function transportMappedCount(amenityCounts) {
  const legacyAmenityCount = Number(amenityCounts && amenityCounts.transport);
  if (Number.isFinite(legacyAmenityCount) && legacyAmenityCount > 0) {
    return legacyAmenityCount;
  }
  const subtierCounts = state.runtime && state.runtime.transport_subtier_counts;
  if (!subtierCounts || typeof subtierCounts !== "object") {
    return 0;
  }
  return Object.values(subtierCounts).reduce(function (sum, value) {
    const count = Number(value);
    if (!Number.isFinite(count) || count < 0) {
      return sum;
    }
    return sum + count;
  }, 0);
}

function selectedAmenityTiers(category) {
  return Array.from(state.enabledAmenityTiers.get(category) || []);
}

function hasVisibleAmenitySelection() {
  return Array.from(state.enabledAmenityCategories).some(function (category) {
    const tierOptions = tierOptionsForCategory(state.runtime, category);
    return !tierOptions.length || selectedAmenityTiers(category).length > 0;
  });
}

function ensureAmenityTierSelection(category) {
  const tierOptions = tierOptionsForCategory(state.runtime, category);
  if (!tierOptions.length) return;
  const existing = state.enabledAmenityTiers.get(category);
  if (existing && existing.size > 0) return;
  state.enabledAmenityTiers.set(category, new Set(tierOptions));
}

function amenityTierSummary(category) {
  const tierOptions = tierOptionsForCategory(state.runtime, category);
  if (!tierOptions.length) return "";
  const selectedCount = selectedAmenityTiers(category).length;
  if (!selectedCount) return "No sub-tiers selected";
  if (selectedCount === tierOptions.length) return "All sub-tiers";
  return selectedCount + " of " + tierOptions.length + " selected";
}

function updatePanelVisibility() {
  document.body.classList.toggle("panel-hidden", state.panelHidden);
  elements.panelToggle.textContent = state.panelHidden ? "Show panel" : "Hide panel";
  elements.panelToggle.setAttribute("aria-expanded", state.panelHidden ? "false" : "true");
}

function resizeMapAfterPanelTransition() {
  const panel = elements.controlPanel;
  if (!panel) {
    state.map.resize();
    return;
  }
  let done = false;
  const finish = function () {
    if (done) return;
    done = true;
    panel.removeEventListener("transitionend", finish);
    if (state.map) state.map.resize();
  };
  panel.addEventListener("transitionend", finish);
  // Fallback in case transitionend never fires (transitions disabled, reduced-motion, etc.).
  window.setTimeout(finish, 400);
}

function setLayerVisibility(layerId, visibility) {
  if (!state.map || !state.map.getLayer(layerId)) return;
  state.map.setLayoutProperty(layerId, "visibility", visibility);
}

function removeLayerIfPresent(layerId) {
  if (!state.map || !state.map.getLayer(layerId)) return;
  state.map.removeLayer(layerId);
}

function livabilitySourceLoaded() {
  if (!state.map || typeof state.map.isSourceLoaded !== "function") {
    return Boolean(state.gridDebug.sourceLoaded);
  }
  try {
    return Boolean(state.map.isSourceLoaded("livability"));
  } catch (error) {
    return Boolean(state.gridDebug.sourceLoaded);
  }
}

function queryGridSourceCount(filter) {
  if (!state.map) return 0;
  try {
    return state.map.querySourceFeatures("livability", {
      sourceLayer: "grid",
      filter: filter
    }).length;
  } catch (error) {
    return 0;
  }
}

function queryGridSourceResolutionCounts() {
  if (!state.map) return {};
  try {
    const features = state.map.querySourceFeatures("livability", {
      sourceLayer: "grid"
    });
    const counts = {};
    for (let i = 0; i < features.length; i += 1) {
      const props = features[i] && features[i].properties;
      if (!props) continue;
      const key = String(Number(props.resolution_m));
      counts[key] = (counts[key] || 0) + 1;
    }
    return counts;
  } catch (error) {
    return {};
  }
}

function queryGridRenderedCount(activeFillLayerId) {
  if (!state.map || !activeFillLayerId || !state.map.getLayer(activeFillLayerId)) {
    return 0;
  }
  try {
    return state.map.queryRenderedFeatures(undefined, {
      layers: [activeFillLayerId]
    }).length;
  } catch (error) {
    return 0;
  }
}

function refreshGridDebugCard(overrides = {}) {
  const zoom = state.map
    ? state.map.getZoom()
    : Number(state.runtime && state.runtime.default_zoom || 0);
  const lifecycle = runtimeActiveGridLifecycle(
    state.runtime || {},
    state.activeGridResolutionM,
    zoom
  );
  const activeFillLayer = runtimeActiveGridLayerId(state.runtime || {}, zoom);
  const activeOutlineLayer = runtimeActiveGridOutlineLayerId(state.runtime || {}, zoom);
  const fillLayerPresent = Boolean(state.map && activeFillLayer && state.map.getLayer(activeFillLayer));
  const outlineLayerPresent = Boolean(state.map && activeOutlineLayer && state.map.getLayer(activeOutlineLayer));
  const nextGridDebug = Object.assign({}, state.gridDebug, {
    enabled: DEBUG_GRID_ENABLED,
    zoom: zoom,
    resolutionM: lifecycle.resolutionM,
    gridVisible: state.gridVisible,
    activeFillLayerId: activeFillLayer,
    activeOutlineLayerId: activeOutlineLayer,
    activeFilter: formatGridDebugFilter(lifecycle.filter),
    sourceCount: queryGridSourceCount(lifecycle.filter),
    renderedCount: queryGridRenderedCount(activeFillLayer),
    sourceResolutions: queryGridSourceResolutionCounts(),
    fillLayerPresent: fillLayerPresent,
    outlineLayerPresent: outlineLayerPresent,
    fillVisibility: fillLayerPresent
      ? String(state.map.getLayoutProperty(activeFillLayer, "visibility") || "visible")
      : "none",
    outlineVisibility: outlineLayerPresent
      ? String(state.map.getLayoutProperty(activeOutlineLayer, "visibility") || "visible")
      : "none",
    sourceLoaded: livabilitySourceLoaded()
  }, overrides);
  state.gridDebug = nextGridDebug;
  renderGridDebugCard(elements.gridDebug, state.runtime, state.gridDebug, {
    snapshotFallbackText: state.gridDebugSnapshotFallbackText
  });
}

function ensureActiveGridLayers() {
  if (!state.map) return null;
  const lifecycle = activeGridLifecycle();
  const requiredLayerIds = [
    activeGridLayerId(),
    activeGridOutlineLayerId(),
    activeDebugGridLayerId()
  ];
  const layersPresent = requiredLayerIds.every(function (layerId) {
    return Boolean(state.map && state.map.getLayer(layerId));
  });
  if (!lifecycle.rebuild && layersPresent) {
    return lifecycle;
  }

  removeLayerIfPresent(activeDebugGridLayerId());
  removeLayerIfPresent(activeGridOutlineLayerId());
  removeLayerIfPresent(activeGridLayerId());

  const beforeLayerId = state.map.getLayer(runtimeGridInsertBeforeLayerId)
    ? runtimeGridInsertBeforeLayerId
    : undefined;
  runtimeBuildActiveGridLayers(
    state.runtime || {},
    lifecycle.resolutionM,
    state.selectedGridLayer
  ).forEach(function (layer) {
    state.map.addLayer(layer, beforeLayerId);
  });
  state.activeGridResolutionM = lifecycle.resolutionM;
  return lifecycle;
}

function applyGridVisibility() {
  const lifecycle = ensureActiveGridLayers();
  if (!state.map || !lifecycle) return;
  applyGridLayerSelection();
  runtimeGridVisibilityPlan(state.runtime || {}, state.map.getZoom(), state.gridVisible).forEach(function (entry) {
    setLayerVisibility(entry.layerId, entry.visibility);
  });
  refreshGridDebugCard();
}

function applyDebugGridVisibility() {
  const lifecycle = ensureActiveGridLayers();
  if (!state.map || !lifecycle) return;
  runtimeDebugGridVisibilityPlan(state.runtime || {}, state.map.getZoom(), state.debugGridVisible).forEach(function (entry) {
    setLayerVisibility(entry.layerId, entry.visibility);
  });
  refreshGridDebugCard();
}

function applyAmenityFilter() {
  if (!state.map) return;
  const visibility = hasVisibleAmenitySelection() ? "visible" : "none";
  state.map.setLayoutProperty("amenities-circle", "visibility", visibility);
  state.map.setFilter(
    "amenities-circle",
    buildAmenityLayerFilter(
      state.runtime,
      state.enabledAmenityCategories,
      state.enabledAmenityTiers
    )
  );
}

function transportFilterSummary() {
  const selectedCount = (
    state.selectedTransportSubtiers.size +
    state.selectedTransportBusFrequencies.size +
    state.selectedTransportModes.size +
    (state.transportIncludeUnscheduled ? 1 : 0)
  );
  if (!selectedCount) {
    return state.transportRequireExceptionOnly ? "All tiers + holiday filter" : "All tiers";
  }
  const totalCount = (
    transportTierOptions(state.runtime).length +
    transportBusFrequencyOptions(state.runtime).length +
    transportModeOptions(state.runtime).length +
    1
  );
  const suffix = state.transportRequireExceptionOnly ? " + holiday filter" : "";
  return selectedCount + " of " + totalCount + " selected" + suffix;
}

function applyTransportRealityFilter() {
  if (!state.map) return;
  state.map.setFilter(
    "transport-reality-circle",
    buildTransportLayerFilter({
      selectedSubtiers: state.selectedTransportSubtiers,
      selectedBusFrequencies: state.selectedTransportBusFrequencies,
      selectedModes: state.selectedTransportModes,
      includeUnscheduled: state.transportIncludeUnscheduled,
      requireExceptionOnly: state.transportRequireExceptionOnly
    })
  );
}

function applyTransportRealityVisibility() {
  if (!state.map) return;
  state.map.setLayoutProperty(
    "transport-reality-circle",
    "visibility",
    state.transportRealityVisible ? "visible" : "none"
  );
  applyTransportRealityFilter();
}

function applyServiceDesertVisibility() {
  if (!state.map) return;
  state.map.setLayoutProperty(
    "service-deserts-fill",
    "visibility",
    state.serviceDesertsVisible ? "visible" : "none"
  );
}

function noiseFilterSummary() {
  const sourceOptions = noiseSourceOptions(state.runtime);
  const sourceCount = state.selectedNoiseSources.size;
  const sourceText = sourceCount === sourceOptions.length ? "all kinds selected" : sourceCount + " kind selected";
  return state.selectedNoiseMetric + ", " + sourceText + ", opacity " + state.noiseOpacity.toFixed(2);
}

function updateNoiseNote() {
  if (!elements.noiseNote) return;
  if (!state.runtime.noise_enabled) {
    elements.noiseNote.textContent = "No transport noise overlay in this build";
    return;
  }
  const caveat = "Official-derived noise overlay. Road/rail use grid proxy; airport/industry use resolved official noise polygons. Not measured point noise.";
  elements.noiseNote.textContent = state.noiseVisible
    ? caveat
    : caveat + " Off until enabled.";
}

function applyNoiseFilter() {
  if (!state.map) return;
  if (!state.map.getLayer("noise-proxy-fill")) return;
  const filter = buildNoiseLayerFilter({
    metric: state.selectedNoiseMetric,
    selectedSources: state.selectedNoiseSources,
    selectedBands: state.selectedNoiseBands
  });
  state.map.setFilter(
    "noise-proxy-fill",
    filter
  );
  if (state.map.getLayer("noise-proxy-outline")) {
    state.map.setFilter("noise-proxy-outline", filter);
  }
}

function applyNoiseOpacity() {
  if (!state.map) return;
  if (!state.map.getLayer("noise-proxy-fill")) return;
  const fillOpacity = Number(state.noiseOpacity ?? runtimeDefaultNoiseOpacity);
  state.map.setPaintProperty("noise-proxy-fill", "fill-opacity", fillOpacity);
  if (state.map.getLayer("noise-proxy-outline")) {
    state.map.setPaintProperty(
      "noise-proxy-outline",
      "line-opacity",
      runtimeNoiseOutlineOpacity(fillOpacity)
    );
  }
}

function applyNoiseVisibility() {
  if (!state.map) return;
  if (!state.map.getLayer("noise-proxy-fill")) return;
  state.map.setLayoutProperty(
    "noise-proxy-fill",
    "visibility",
    state.noiseVisible ? "visible" : "none"
  );
  if (state.map.getLayer("noise-proxy-outline")) {
    state.map.setLayoutProperty(
      "noise-proxy-outline",
      "visibility",
      state.noiseVisible ? "visible" : "none"
    );
  }
  applyNoiseFilter();
  applyNoiseOpacity();
}

function buildAmenityControls() {
  elements.amenityControls.replaceChildren();
  const colors = state.runtime.category_colors || {};
  const counts = state.runtime.amenity_counts || {};
  let transportCardAdded = false;

  function appendTransportControlCard(color) {
    const card = document.createElement("div");
    card.className = "amenity-control-card";

    const overlayLabel = document.createElement("label");
    overlayLabel.className = "toggle-row";
    overlayLabel.htmlFor = "transport-reality-toggle";

    const overlayTextWrap = document.createElement("span");
    overlayTextWrap.className = "toggle-label";

    const overlayTitle = document.createElement("strong");
    overlayTitle.textContent = "Transport";
    if (color) {
      overlayTitle.style.color = color;
    }

    const overlaySubtitle = document.createElement("span");
    overlaySubtitle.textContent = String(transportMappedCount(counts)) + " mapped";

    const overlayInput = document.createElement("input");
    overlayInput.type = "checkbox";
    overlayInput.id = "transport-reality-toggle";
    overlayInput.checked = state.transportRealityVisible;
    overlayInput.disabled = !state.runtime.transport_reality_enabled;

    overlayTextWrap.appendChild(overlayTitle);
    overlayTextWrap.appendChild(overlaySubtitle);
    overlayLabel.appendChild(overlayTextWrap);
    overlayLabel.appendChild(overlayInput);
    card.appendChild(overlayLabel);

    if (!state.runtime.transport_reality_enabled) {
      elements.amenityControls.appendChild(card);
      return;
    }

    const tierDetails = document.createElement("details");
    tierDetails.className = "amenity-tier-details";

    const tierSummary = document.createElement("summary");
    tierSummary.className = "amenity-tier-summary";

    const tierSummaryLabel = document.createElement("span");
    tierSummaryLabel.textContent = "Sub-tiers";

    const tierMeta = document.createElement("span");
    tierMeta.className = "amenity-tier-meta";
    tierMeta.textContent = transportFilterSummary();

    tierSummary.appendChild(tierSummaryLabel);
    tierSummary.appendChild(tierMeta);
    tierDetails.appendChild(tierSummary);

    const tierList = document.createElement("div");
    tierList.className = "amenity-tier-list";

    const tierRows = [];
    const groupContainers = [];
    const flagCounts = transportFlagCounts(state.runtime);

    function appendTransportRow(targetContainer, entry) {
      const row = document.createElement("label");
      row.className = "amenity-tier-row";
      row.htmlFor = entry.id;

      const textWrap = document.createElement("span");
      textWrap.className = "toggle-label";

      const title = document.createElement("strong");
      title.textContent = entry.label;

      const subtitle = document.createElement("span");
      subtitle.textContent = String(entry.count || 0) + " mapped";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.id = entry.id;
      input.addEventListener("change", function () {
        if (entry.type === "subtier") {
          if (input.checked) {
            state.selectedTransportSubtiers.add(entry.value);
          } else {
            state.selectedTransportSubtiers.delete(entry.value);
          }
        } else if (entry.type === "bus_frequency") {
          if (input.checked) {
            state.selectedTransportBusFrequencies.add(entry.value);
          } else {
            state.selectedTransportBusFrequencies.delete(entry.value);
          }
        } else if (entry.type === "mode") {
          if (input.checked) {
            state.selectedTransportModes.add(entry.value);
          } else {
            state.selectedTransportModes.delete(entry.value);
          }
        } else if (entry.type === "flag") {
          if (entry.value === "unscheduled") {
            state.transportIncludeUnscheduled = input.checked;
          } else if (entry.value === "exception_only") {
            state.transportRequireExceptionOnly = input.checked;
          }
        }
        syncTransportInputs();
        applyTransportRealityFilter();
      });

      tierRows.push({ type: entry.type, value: entry.value, input: input });
      textWrap.appendChild(title);
      textWrap.appendChild(subtitle);
      row.appendChild(textWrap);
      row.appendChild(input);
      targetContainer.appendChild(row);
    }

    function syncTransportInputs() {
      const busModeEnabled = state.selectedTransportModes.has("bus");
      tierRows.forEach(function (entry) {
        let isChecked = false;
        if (entry.type === "subtier") {
          isChecked = state.selectedTransportSubtiers.has(entry.value);
        } else if (entry.type === "bus_frequency") {
          isChecked = state.selectedTransportBusFrequencies.has(entry.value);
        } else if (entry.type === "mode") {
          isChecked = state.selectedTransportModes.has(entry.value);
        } else if (entry.value === "unscheduled") {
          isChecked = state.transportIncludeUnscheduled;
        } else {
          isChecked = state.transportRequireExceptionOnly;
        }
        entry.input.checked = isChecked;
        const isBusSubFilter = (
          entry.type === "subtier" ||
          entry.type === "bus_frequency" ||
          entry.type === "flag"
        );
        entry.input.disabled = !state.transportRealityVisible || (isBusSubFilter && !busModeEnabled);
      });
      tierMeta.textContent = transportFilterSummary();
      tierDetails.classList.toggle("is-disabled", !state.transportRealityVisible);
      groupContainers.forEach(function (container) {
        container.classList.toggle("is-disabled", !state.transportRealityVisible);
      });
      busDetails.classList.toggle("is-disabled", !state.transportRealityVisible || !busModeEnabled);
      if (!state.transportRealityVisible) {
        tierDetails.open = false;
      }
    }

    const transportDetails = document.createElement("details");
    transportDetails.className = "amenity-tier-details transport-subgroup";
    const transportSummary = document.createElement("summary");
    transportSummary.className = "amenity-tier-summary";
    transportSummary.appendChild(document.createTextNode("Transport"));
    transportDetails.appendChild(transportSummary);
    const transportList = document.createElement("div");
    transportList.className = "amenity-tier-list";
    transportDetails.appendChild(transportList);
    tierList.appendChild(transportDetails);
    groupContainers.push(transportDetails);

    const busDetails = document.createElement("details");
    busDetails.className = "amenity-tier-details transport-subgroup";
    const busSummary = document.createElement("summary");
    busSummary.className = "amenity-tier-summary";
    busSummary.appendChild(document.createTextNode("Bus"));
    busDetails.appendChild(busSummary);
    const busList = document.createElement("div");
    busList.className = "amenity-tier-list";
    busDetails.appendChild(busList);
    tierList.appendChild(busDetails);
    groupContainers.push(busDetails);

    const scheduleDetails = document.createElement("details");
    scheduleDetails.className = "amenity-tier-details transport-subgroup";
    const scheduleSummary = document.createElement("summary");
    scheduleSummary.className = "amenity-tier-summary";
    scheduleSummary.appendChild(document.createTextNode("Schedule"));
    scheduleDetails.appendChild(scheduleSummary);
    const scheduleList = document.createElement("div");
    scheduleList.className = "amenity-tier-list";
    scheduleDetails.appendChild(scheduleList);
    busList.appendChild(scheduleDetails);
    groupContainers.push(scheduleDetails);

    const frequencyDetails = document.createElement("details");
    frequencyDetails.className = "amenity-tier-details transport-subgroup";
    const frequencySummary = document.createElement("summary");
    frequencySummary.className = "amenity-tier-summary";
    frequencySummary.appendChild(document.createTextNode("Frequency"));
    frequencyDetails.appendChild(frequencySummary);
    const frequencyList = document.createElement("div");
    frequencyList.className = "amenity-tier-list";
    frequencyDetails.appendChild(frequencyList);
    busList.appendChild(frequencyDetails);
    groupContainers.push(frequencyDetails);

    transportTierOptions(state.runtime).forEach(function (option) {
      appendTransportRow(scheduleList, {
        type: "subtier",
        value: option.value,
        label: option.label,
        count: option.count,
        id: "transport-tier-" + option.value
      });
    });

    appendTransportRow(scheduleList, {
      type: "flag",
      value: "unscheduled",
      label: "Unscheduled",
      count: Number(flagCounts.is_unscheduled_stop || 0),
      id: "transport-flag-unscheduled"
    });
    appendTransportRow(scheduleList, {
      type: "flag",
      value: "exception_only",
      label: "Has calendar_dates-only bus service",
      count: Number(flagCounts.has_exception_only_service || 0),
      id: "transport-flag-exception_only"
    });

    transportBusFrequencyOptions(state.runtime).forEach(function (option) {
      appendTransportRow(frequencyList, {
        type: "bus_frequency",
        value: option.value,
        label: option.label,
        count: option.count,
        id: "transport-bus-frequency-" + option.value
      });
    });

    transportModeOptions(state.runtime).forEach(function (option) {
      appendTransportRow(transportList, {
        type: "mode",
        value: option.value,
        label: option.label,
        count: option.count,
        id: "transport-mode-" + option.value
      });
    });

    tierDetails.appendChild(tierList);
    card.appendChild(tierDetails);

    overlayInput.addEventListener("change", function () {
      state.transportRealityVisible = overlayInput.checked;
      syncTransportInputs();
      applyTransportRealityVisibility();
    });

    syncTransportInputs();
    elements.amenityControls.appendChild(card);
  }

  Object.keys(colors)
    .sort()
    .forEach(function (category) {
      if (category === "transport") {
        appendTransportControlCard(colors[category]);
        transportCardAdded = true;
        return;
      }
      const tierOptions = tierOptionsForCategory(state.runtime, category);
      if (tierOptions.length && !state.enabledAmenityTiers.has(category)) {
        state.enabledAmenityTiers.set(category, new Set(tierOptions));
      }

      const card = document.createElement("div");
      card.className = "amenity-control-card";

      const label = document.createElement("label");
      label.className = "toggle-row";
      label.htmlFor = "amenity-" + category;

      const textWrap = document.createElement("span");
      textWrap.className = "toggle-label";

      const title = document.createElement("strong");
      title.textContent = formatAmenityLabel(category);
      title.style.color = colors[category];

      const subtitle = document.createElement("span");
      subtitle.textContent = String(counts[category] || 0) + " mapped";

      const categoryInput = document.createElement("input");
      categoryInput.type = "checkbox";
      categoryInput.id = "amenity-" + category;
      categoryInput.checked = false;

      const tierInputs = [];
      let tierDetails = null;
      let tierMeta = null;

      function syncTierInputs() {
        const selected = state.enabledAmenityTiers.get(category) || new Set();
        tierInputs.forEach(function (tierInput) {
          tierInput.checked = selected.has(tierInput.dataset.tier || "");
          tierInput.disabled = !categoryInput.checked;
        });
        if (tierMeta) {
          tierMeta.textContent = amenityTierSummary(category);
        }
        if (tierDetails) {
          tierDetails.classList.toggle("is-disabled", !categoryInput.checked);
          if (!categoryInput.checked) {
            tierDetails.open = false;
          }
        }
      }

      categoryInput.addEventListener("change", function () {
        if (categoryInput.checked) {
          state.enabledAmenityCategories.add(category);
          ensureAmenityTierSelection(category);
        } else {
          state.enabledAmenityCategories.delete(category);
        }
        syncTierInputs();
        updateAmenityNote();
        applyAmenityFilter();
      });

      textWrap.appendChild(title);
      textWrap.appendChild(subtitle);
      label.appendChild(textWrap);
      label.appendChild(categoryInput);
      card.appendChild(label);

      if (tierOptions.length) {
        const tierCounts = (state.runtime.amenity_tier_counts || {})[category] || {};
        tierDetails = document.createElement("details");
        tierDetails.className = "amenity-tier-details";

        const tierSummary = document.createElement("summary");
        tierSummary.className = "amenity-tier-summary";

        const tierSummaryLabel = document.createElement("span");
        tierSummaryLabel.textContent = "Sub-tiers";

        tierMeta = document.createElement("span");
        tierMeta.className = "amenity-tier-meta";
        tierMeta.textContent = amenityTierSummary(category);

        tierSummary.appendChild(tierSummaryLabel);
        tierSummary.appendChild(tierMeta);
        tierDetails.appendChild(tierSummary);

        const tierList = document.createElement("div");
        tierList.className = "amenity-tier-list";

        tierOptions.forEach(function (tier) {
          const tierRow = document.createElement("label");
          tierRow.className = "amenity-tier-row";
          tierRow.htmlFor = "amenity-" + category + "-" + tier;

          const tierTextWrap = document.createElement("span");
          tierTextWrap.className = "toggle-label";

          const tierTitle = document.createElement("strong");
          tierTitle.textContent = formatAmenityLabel(tier);

          const tierSubtitle = document.createElement("span");
          tierSubtitle.textContent = String(tierCounts[tier] || 0) + " mapped";

          const tierInput = document.createElement("input");
          tierInput.type = "checkbox";
          tierInput.id = "amenity-" + category + "-" + tier;
          tierInput.dataset.tier = tier;
          tierInput.checked = selectedAmenityTiers(category).includes(tier);
          tierInput.addEventListener("change", function () {
            const selected = new Set(state.enabledAmenityTiers.get(category) || []);
            if (tierInput.checked) {
              selected.add(tier);
            } else {
              selected.delete(tier);
            }
            state.enabledAmenityTiers.set(category, selected);
            syncTierInputs();
            applyAmenityFilter();
          });

          tierInputs.push(tierInput);
          tierTextWrap.appendChild(tierTitle);
          tierTextWrap.appendChild(tierSubtitle);
          tierRow.appendChild(tierTextWrap);
          tierRow.appendChild(tierInput);
          tierList.appendChild(tierRow);
        });

        tierDetails.appendChild(tierList);
        card.appendChild(tierDetails);
        syncTierInputs();
      }

      elements.amenityControls.appendChild(card);
    });
  if (!transportCardAdded && state.runtime.transport_reality_enabled) {
    appendTransportControlCard("");
  }
  updateAmenityNote();
}

function buildNoiseControls() {
  if (!elements.noiseControls) return;
  elements.noiseControls.replaceChildren();
  if (!state.runtime.noise_enabled) {
    updateNoiseNote();
    return;
  }

  const overlayLabel = document.createElement("label");
  overlayLabel.className = "toggle-row";
  overlayLabel.htmlFor = "noise-toggle";

  const overlayTextWrap = document.createElement("span");
  overlayTextWrap.className = "toggle-label";

  const overlayTitle = document.createElement("strong");
  overlayTitle.textContent = "Show noise overlay";

  const overlaySubtitle = document.createElement("span");
  overlaySubtitle.textContent = "Official-derived transport/industry noise overlay";

  const overlayInput = document.createElement("input");
  overlayInput.type = "checkbox";
  overlayInput.id = "noise-toggle";
  overlayInput.checked = state.noiseVisible;

  overlayTextWrap.appendChild(overlayTitle);
  overlayTextWrap.appendChild(overlaySubtitle);
  overlayLabel.appendChild(overlayTextWrap);
  overlayLabel.appendChild(overlayInput);
  elements.noiseControls.appendChild(overlayLabel);

  const filterDetails = document.createElement("details");
  filterDetails.className = "amenity-tier-details";

  const filterSummary = document.createElement("summary");
  filterSummary.className = "amenity-tier-summary";

  const filterSummaryLabel = document.createElement("span");
  filterSummaryLabel.textContent = "Proxy filters";

  const filterMeta = document.createElement("span");
  filterMeta.className = "amenity-tier-meta";
  filterMeta.textContent = noiseFilterSummary();

  filterSummary.appendChild(filterSummaryLabel);
  filterSummary.appendChild(filterMeta);
  filterDetails.appendChild(filterSummary);

  const filterList = document.createElement("div");
  filterList.className = "amenity-tier-list";

  const filterRows = [];

  function syncNoiseInputs() {
    filterRows.forEach(function (entry) {
      if (entry.type === "metric") {
        entry.input.checked = state.selectedNoiseMetric === entry.value;
      } else if (entry.type === "source") {
        entry.input.checked = state.selectedNoiseSources.has(entry.value);
      } else if (entry.type === "band") {
        entry.input.checked = state.selectedNoiseBands.has(entry.value);
      } else if (entry.type === "opacity") {
        entry.input.value = String(state.noiseOpacity.toFixed(2));
      }
      if (entry.type !== "opacity") {
        entry.input.disabled = !state.noiseVisible;
      }
    });
    filterMeta.textContent = noiseFilterSummary();
    filterDetails.classList.toggle("is-disabled", !state.noiseVisible);
    if (!state.noiseVisible) {
      filterDetails.open = false;
    }
    updateNoiseNote();
  }

  function appendFilterRow({ type, value, label, count, inputType }) {
    const row = document.createElement("label");
    row.className = "amenity-tier-row";
    row.htmlFor = "noise-" + type + "-" + value;

    const textWrap = document.createElement("span");
    textWrap.className = "toggle-label";

    const title = document.createElement("strong");
    title.textContent = label;

    const subtitle = document.createElement("span");
    subtitle.textContent = String(count || 0) + " mapped";

    const input = document.createElement("input");
    input.type = inputType;
    if (type === "metric") {
      input.name = "noise-metric";
    }
    input.id = "noise-" + type + "-" + value;
    input.addEventListener("change", function () {
      if (type === "metric" && input.checked) {
        state.selectedNoiseMetric = value;
        state.selectedNoiseBands = new Set(
          noiseBandOptions(state.runtime, state.selectedNoiseMetric).map(function (option) {
            return option.value;
          })
        );
        buildNoiseControls();
        applyNoiseFilter();
        return;
      } else if (type === "source") {
        if (input.checked) {
          state.selectedNoiseSources.add(value);
        } else {
          state.selectedNoiseSources.delete(value);
        }
      } else if (type === "band") {
        if (input.checked) {
          state.selectedNoiseBands.add(value);
        } else {
          state.selectedNoiseBands.delete(value);
        }
      }
      syncNoiseInputs();
      applyNoiseFilter();
    });

    filterRows.push({ type: type, value: value, input: input });
    textWrap.appendChild(title);
    textWrap.appendChild(subtitle);
    row.appendChild(textWrap);
    row.appendChild(input);
    filterList.appendChild(row);
  }

  const metricOptions = noiseMetricOptions(state.runtime);
  if (metricOptions.length > 1) {
    metricOptions.forEach(function (option) {
      appendFilterRow({
        type: "metric",
        value: option.value,
        label: option.label,
        count: option.count,
        inputType: "radio"
      });
    });
  }

  noiseSourceOptions(state.runtime).forEach(function (option) {
    appendFilterRow({
      type: "source",
      value: option.value,
      label: option.label,
      count: option.count,
      inputType: "checkbox"
    });
  });

  const opacityRow = document.createElement("label");
  opacityRow.className = "amenity-tier-row";
  opacityRow.htmlFor = "noise-opacity";

  const opacityTextWrap = document.createElement("span");
  opacityTextWrap.className = "toggle-label";

  const opacityTitle = document.createElement("strong");
  opacityTitle.textContent = "Noise opacity";

  const opacitySubtitle = document.createElement("span");
  opacitySubtitle.textContent = "0 to 1 (default 0.45)";

  const opacityInput = document.createElement("input");
  opacityInput.type = "range";
  opacityInput.min = "0";
  opacityInput.max = "1";
  opacityInput.step = "0.05";
  opacityInput.value = String(state.noiseOpacity.toFixed(2));
  opacityInput.id = "noise-opacity";
  opacityInput.addEventListener("input", function () {
    const nextOpacity = Number(opacityInput.value);
    if (!Number.isFinite(nextOpacity)) return;
    state.noiseOpacity = Math.max(0, Math.min(1, nextOpacity));
    applyNoiseOpacity();
    filterMeta.textContent = noiseFilterSummary();
  });
  filterRows.push({ type: "opacity", value: "opacity", input: opacityInput });

  opacityTextWrap.appendChild(opacityTitle);
  opacityTextWrap.appendChild(opacitySubtitle);
  opacityRow.appendChild(opacityTextWrap);
  opacityRow.appendChild(opacityInput);
  filterList.appendChild(opacityRow);

  filterDetails.appendChild(filterList);
  elements.noiseControls.appendChild(filterDetails);

  overlayInput.addEventListener("change", function () {
    state.noiseVisible = overlayInput.checked;
    syncNoiseInputs();
    applyNoiseVisibility();
  });
  syncNoiseInputs();
}

function buildTransitControls() {
  if (!elements.transitControls) return;
  elements.transitControls.replaceChildren();
  if (state.runtime.service_deserts_enabled) {
    const label = document.createElement("label");
    label.className = "toggle-row";
    label.htmlFor = "service-deserts-toggle";

    const text = document.createElement("span");
    text.textContent = "Show service desert overlay";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.id = "service-deserts-toggle";
    input.checked = state.serviceDesertsVisible;
    input.addEventListener("change", function () {
      state.serviceDesertsVisible = input.checked;
      applyServiceDesertVisibility();
    });

    label.appendChild(text);
    label.appendChild(input);
    elements.transitControls.appendChild(label);
  }

}

function maybeBuildDebugGridControl() {
  renderGridDebugCard(elements.gridDebug, state.runtime, state.gridDebug, {
    snapshotFallbackText: state.gridDebugSnapshotFallbackText
  });
  if (!DEBUG_GRID_ENABLED || !elements.gridDebug.controls) return;

  const label = document.createElement("label");
  label.className = "toggle-row";
  label.htmlFor = "grid-debug-toggle";

  const text = document.createElement("span");
  text.textContent = "Show vector debug grid";

  const input = document.createElement("input");
  input.type = "checkbox";
  input.id = "grid-debug-toggle";
  input.checked = false;
  input.addEventListener("change", function () {
    state.debugGridVisible = input.checked;
    applyDebugGridVisibility();
  });

  label.appendChild(text);
  label.appendChild(input);
  elements.gridDebug.controls.appendChild(label);
}

async function fetchInspect(lngLat) {
  if (state.pendingInspect) {
    state.pendingInspect.abort();
  }
  const controller = new AbortController();
  state.pendingInspect = controller;
  const url = new URL(String(state.runtime.inspect_url), window.location.origin);
  url.searchParams.set("lat", String(lngLat.lat));
  url.searchParams.set("lon", String(lngLat.lng));
  url.searchParams.set("zoom", String(state.map.getZoom()));
  try {
    const response = await fetchWithTimeout(
      url,
      { signal: controller.signal },
      INSPECT_FETCH_TIMEOUT_MS
    );
    if (!response.ok) {
      const payload = await response.json().catch(function () {
        return {};
      });
      throw new Error(payload.error || response.statusText);
    }
    return response.json();
  } finally {
    if (state.pendingInspect === controller) {
      state.pendingInspect = null;
    }
  }
}

function isLivabilitySourceEvent(event) {
  if (!event) return false;
  if (event.sourceId === "livability") return true;
  if (event.source && event.source.id === "livability") return true;
  if (!state.map || typeof state.map.getSource !== "function") return false;
  try {
    return event.source === state.map.getSource("livability");
  } catch (error) {
    return false;
  }
}

function wireUi() {
  elements.panelToggle.addEventListener("click", function () {
    state.panelHidden = !state.panelHidden;
    updatePanelVisibility();
    if (state.map) {
      resizeMapAfterPanelTransition();
    }
  });

  elements.zoomIn.addEventListener("click", function () {
    if (state.map) state.map.zoomIn();
  });

  elements.zoomOut.addEventListener("click", function () {
    if (state.map) state.map.zoomOut();
  });

  elements.gridToggle.addEventListener("change", function () {
    state.gridVisible = elements.gridToggle.checked;
    applyGridVisibility();
  });

  if (elements.gridDebug.copyButton) {
    elements.gridDebug.copyButton.addEventListener("click", async function () {
      const snapshot = buildGridDebugSnapshot(state.runtime, state.gridDebug);
      await copyGridDebugSnapshot(
        window.navigator && window.navigator.clipboard,
        snapshot,
        function (fallbackText) {
          state.gridDebugSnapshotFallbackText = fallbackText || "";
        }
      );
      if (state.gridDebugSnapshotFallbackText.length === 0) {
        renderGridDebugCard(elements.gridDebug, state.runtime, state.gridDebug, {
          snapshotFallbackText: ""
        });
        return;
      }
      renderGridDebugCard(elements.gridDebug, state.runtime, state.gridDebug, {
        snapshotFallbackText: state.gridDebugSnapshotFallbackText
      });
    });
  }

  window.addEventListener("resize", function () {
    if (state.map) state.map.resize();
  });
}

function initializeMap() {
  state.map = new maplibregl.Map({
    container: elements.map,
    style: buildRuntimeStyle(state.runtime, {
      windowOrigin: window.location.origin,
      selectedGridLayer: state.selectedGridLayer
    }),
    center: [state.runtime.map_center.lon, state.runtime.map_center.lat],
    zoom: state.runtime.default_zoom || 6,
    minZoom: MIN_ZOOM,
    maxZoom: Number(state.runtime.max_zoom || 19),
    attributionControl: false,
    hash: false
  });

  state.map.addControl(
    new maplibregl.AttributionControl({ compact: true }),
    "bottom-left"
  );

  state.popup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: true,
    className: "livability-popup",
    maxWidth: "320px"
  });

  state.map.on("load", function () {
    setResolutionChip();
    applyGridVisibility();
    applyDebugGridVisibility();
    applyAmenityFilter();
    applyTransportRealityVisibility();
    applyServiceDesertVisibility();
    applyNoiseVisibility();
    updateStatus("");
    elements.statusPill.style.display = "none";
  });

  state.map.on("zoom", function () {
    setResolutionChip();
    applyGridVisibility();
    applyDebugGridVisibility();
  });
  state.map.on("zoomend", function () {
    setResolutionChip();
    applyGridVisibility();
    applyDebugGridVisibility();
  });
  state.map.on("moveend", function () {
    refreshGridDebugCard();
  });
  state.map.on("sourcedata", function (event) {
    if (!isLivabilitySourceEvent(event)) return;
    refreshGridDebugCard({
      lastSourceEvent: formatSourceEventLabel(event),
      sourceLoaded: event.isSourceLoaded === undefined
        ? livabilitySourceLoaded()
        : Boolean(event.isSourceLoaded)
    });
  });
  state.map.on("data", function (event) {
    if (!isLivabilitySourceEvent(event)) return;
    refreshGridDebugCard({
      lastSourceEvent: formatSourceEventLabel(event)
    });
  });

  state.map.on("click", async function (event) {
    const clickAction = resolveMapClickAction({
      map: state.map,
      point: event.point,
      fineSurfaceEnabled: fineSurfaceEnabled(),
      gridVisible: state.gridVisible,
      activeGridLayerId: activeGridLayerId()
    });

    if (clickAction.type === CLICK_ACTIONS.TRANSPORT) {
      const popupRows = Array.from(
        clickAction.features.reduce(function (rowsBySourceRef, feature) {
          const properties = feature && feature.properties ? feature.properties : {};
          const sourceRef = String(properties.source_ref || feature.id || rowsBySourceRef.size);
          if (!rowsBySourceRef.has(sourceRef)) {
            rowsBySourceRef.set(sourceRef, properties);
          }
          return rowsBySourceRef;
        }, new Map()).values()
      );
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(transportRealityPopupHtml(popupRows))
        .addTo(state.map);
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.AMENITY) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(amenityPopupHtml(clickAction.features[0].properties || {}))
        .addTo(state.map);
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.SERVICE_DESERT) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(serviceDesertPopupHtml(clickAction.features[0].properties || {}, resolutionForZoom(state.map.getZoom())))
        .addTo(state.map);
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.NOISE) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(noisePopupHtml(clickAction.features[0].properties || {}))
        .addTo(state.map);
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.FINE_INSPECT) {
      try {
        const payload = await fetchInspect(event.lngLat);
        state.popup
          .setLngLat(event.lngLat)
          .setHTML(inspectPopupHtml(payload))
          .addTo(state.map);
      } catch (error) {
        if (error && error.name === "AbortError") {
          return;
        }
        updateStatus(error.message || "Inspect failed");
        elements.statusPill.style.display = "";
      }
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.COARSE_GRID) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(coarseGridPopupHtml(clickAction.features[0].properties || {}, resolutionForZoom(state.map.getZoom())))
        .addTo(state.map);
    }
  });

  state.map.on("mouseenter", "amenities-circle", function () {
    state.map.getCanvas().style.cursor = "pointer";
  });
  state.map.on("mouseleave", "amenities-circle", function () {
    state.map.getCanvas().style.cursor = "";
  });
  state.map.on("mouseenter", "transport-reality-circle", function () {
    state.map.getCanvas().style.cursor = "pointer";
  });
  state.map.on("mouseleave", "transport-reality-circle", function () {
    state.map.getCanvas().style.cursor = "";
  });
  state.map.on("mouseenter", "service-deserts-fill", function () {
    state.map.getCanvas().style.cursor = "pointer";
  });
  state.map.on("mouseleave", "service-deserts-fill", function () {
    state.map.getCanvas().style.cursor = "";
  });
  if (state.map.getLayer("noise-proxy-fill")) {
    state.map.on("mouseenter", "noise-proxy-fill", function () {
      state.map.getCanvas().style.cursor = "pointer";
    });
    state.map.on("mouseleave", "noise-proxy-fill", function () {
      state.map.getCanvas().style.cursor = "";
    });
  }

  state.map.on("error", function (event) {
    const message = (event && event.error && event.error.message) || "Map error";
    updateStatus(message);
    elements.statusPill.style.display = "";
    refreshGridDebugCard({
      lastMapError: message
    });
  });
}

function initializeApp(runtime) {
  state.runtime = runtime;
  state.gridVisible = true;
  state.activeGridResolutionM = null;
  state.debugGridVisible = false;
  state.gridDebug = createGridDebugState(DEBUG_GRID_ENABLED);
  state.gridDebugSnapshotFallbackText = "";
  state.selectedGridLayer = readSavedGridLayer();
  state.enabledAmenityCategories = new Set();
  state.enabledAmenityTiers = new Map(
    Object.entries(defaultAmenityTierSelections(runtime)).map(function (entry) {
      return [entry[0], new Set(entry[1])];
    })
  );
  state.selectedTransportSubtiers = new Set();
  state.selectedTransportBusFrequencies = new Set();
  state.selectedTransportModes = new Set(["bus", "rail", "tram"]);
  state.transportIncludeUnscheduled = false;
  state.transportRequireExceptionOnly = false;
  state.noiseVisible = false;
  state.noiseOpacity = runtimeDefaultNoiseOpacity;
  const noiseDefaults = defaultNoiseSelections(runtime);
  state.selectedNoiseMetric = noiseDefaults.metric;
  state.selectedNoiseSources = new Set(noiseDefaults.sources);
  if (state.selectedNoiseSources.has("industry") && state.selectedNoiseSources.size > 1) {
    state.selectedNoiseSources.delete("industry");
  }
  state.selectedNoiseBands = new Set(noiseDefaults.bands || []);
  buildGridLayerControls();
  buildAmenityControls();
  buildNoiseControls();
  buildTransitControls();
  maybeBuildDebugGridControl();
  wireUi();
  updatePanelVisibility();
  initializeMap();
}

function delay(ms) {
  return new Promise(function (resolve) {
    window.setTimeout(resolve, ms);
  });
}

async function runtimeErrorFromResponse(response) {
  const payload = await response.json().catch(function () {
    return {};
  });
  return new Error(payload.error || response.statusText);
}

async function fetchRuntimePayload() {
  let lastError = null;
  for (let attempt = 1; attempt <= RUNTIME_FETCH_MAX_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetchWithTimeout(
        "/api/runtime",
        undefined,
        RUNTIME_FETCH_TIMEOUT_MS
      );
      if (!response.ok) {
        throw await runtimeErrorFromResponse(response);
      }
      return response.json();
    } catch (error) {
      lastError = error;
      if (!error || error.name !== "AbortError" || attempt >= RUNTIME_FETCH_MAX_ATTEMPTS) {
        throw error;
      }
      updateStatus("Runtime is warming up; retrying...");
      await delay(RUNTIME_FETCH_RETRY_DELAY_MS);
    }
  }
  throw lastError || new Error("Failed to load runtime");
}

fetchRuntimePayload()
  .then(initializeApp)
  .catch(function (error) {
    const message = error && error.name === "AbortError"
      ? "Timed out loading runtime"
      : (error && error.message) || "Failed to load runtime";
    updateStatus(message);
    elements.gridToggle.disabled = true;
  });
