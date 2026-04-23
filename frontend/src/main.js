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
  transportModeOptions,
  transportTierOptions
} from "./transport_filters.js";
import {
  GRID_INSERT_BEFORE_LAYER_ID as runtimeGridInsertBeforeLayerId,
  activeGridLifecycle as runtimeActiveGridLifecycle,
  activeDebugGridLayerId as runtimeActiveDebugGridLayerId,
  activeGridLayerId as runtimeActiveGridLayerId,
  activeGridOutlineLayerId as runtimeActiveGridOutlineLayerId,
  buildStyle as buildRuntimeStyle,
  buildActiveGridLayers as runtimeBuildActiveGridLayers,
  debugGridVisibilityPlan as runtimeDebugGridVisibilityPlan,
  fineSurfaceEnabled as runtimeFineSurfaceEnabled,
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

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const MIN_ZOOM = 5;
const DEBUG_GRID_QUERY_PARAM = "debug-grid";
const SCORE_SECTIONS = ["shops", "transport", "healthcare", "parks"];
const RUNTIME_FETCH_TIMEOUT_MS = 10000;
const INSPECT_FETCH_TIMEOUT_MS = 15000;
const DEBUG_GRID_ENABLED = debugGridEnabledFromUrl(window.location.href, {
  paramName: DEBUG_GRID_QUERY_PARAM
});

function fetchWithTimeout(input, init, timeoutMs) {
  const controller = new AbortController();
  const externalSignal = init && init.signal;
  if (externalSignal) {
    if (externalSignal.aborted) {
      controller.abort();
    } else {
      externalSignal.addEventListener("abort", function () {
        controller.abort();
      }, { once: true });
    }
  }
  const timer = window.setTimeout(function () {
    controller.abort();
  }, timeoutMs);
  const opts = Object.assign({}, init || {}, { signal: controller.signal });
  return fetch(input, opts).finally(function () {
    window.clearTimeout(timer);
  });
}

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
  transitNote: document.getElementById("transit-note"),
  transportRealityDownload: document.getElementById("transport-reality-download"),
  gridToggle: document.getElementById("grid-toggle"),
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
  enabledAmenityCategories: new Set(),
  enabledAmenityTiers: new Map(),
  gridVisible: true,
  activeGridResolutionM: null,
  debugGridVisible: false,
  gridDebug: createGridDebugState(DEBUG_GRID_ENABLED),
  gridDebugSnapshotFallbackText: "",
  transportRealityVisible: false,
  selectedTransportSubtiers: new Set(),
  selectedTransportModes: new Set(),
  transportIncludeUnscheduled: false,
  transportRequireExceptionOnly: false,
  serviceDesertsVisible: false,
  pendingInspect: null
};

function updateStatus(message) {
  elements.statusPill.textContent = message;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
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
    zoom
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
  runtimeBuildActiveGridLayers(state.runtime || {}, lifecycle.resolutionM).forEach(function (layer) {
    state.map.addLayer(layer, beforeLayerId);
  });
  state.activeGridResolutionM = lifecycle.resolutionM;
  return lifecycle;
}

function applyGridVisibility() {
  const lifecycle = ensureActiveGridLayers();
  if (!state.map || !lifecycle) return;
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
    state.selectedTransportModes.size +
    (state.transportIncludeUnscheduled ? 1 : 0)
  );
  if (!selectedCount) {
    return state.transportRequireExceptionOnly ? "All tiers + holiday filter" : "All tiers";
  }
  const totalCount = (
    transportTierOptions(state.runtime).length +
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

function buildAmenityControls() {
  elements.amenityControls.replaceChildren();
  const colors = state.runtime.category_colors || {};
  const counts = state.runtime.amenity_counts || {};
  Object.keys(colors)
    .sort()
    .forEach(function (category) {
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
  updateAmenityNote();
}

function buildTransitControls() {
  elements.transitControls.replaceChildren();
  if (state.runtime.transport_reality_enabled) {
    const overlayLabel = document.createElement("label");
    overlayLabel.className = "toggle-row";
    overlayLabel.htmlFor = "transport-reality-toggle";

    const overlayTextWrap = document.createElement("span");
    overlayTextWrap.className = "toggle-label";

    const overlayTitle = document.createElement("strong");
    overlayTitle.textContent = "Show public transport tiers";

    const overlaySubtitle = document.createElement("span");
    overlaySubtitle.textContent = "GTFS weekly service and mode view";

    const overlayInput = document.createElement("input");
    overlayInput.type = "checkbox";
    overlayInput.id = "transport-reality-toggle";
    overlayInput.checked = state.transportRealityVisible;

    overlayTextWrap.appendChild(overlayTitle);
    overlayTextWrap.appendChild(overlaySubtitle);
    overlayLabel.appendChild(overlayTextWrap);
    overlayLabel.appendChild(overlayInput);
    elements.transitControls.appendChild(overlayLabel);

    const tierDetails = document.createElement("details");
    tierDetails.className = "amenity-tier-details";

    const tierSummary = document.createElement("summary");
    tierSummary.className = "amenity-tier-summary";

    const tierSummaryLabel = document.createElement("span");
    tierSummaryLabel.textContent = "Public transport filters";

    const tierMeta = document.createElement("span");
    tierMeta.className = "amenity-tier-meta";
    tierMeta.textContent = transportFilterSummary();

    tierSummary.appendChild(tierSummaryLabel);
    tierSummary.appendChild(tierMeta);
    tierDetails.appendChild(tierSummary);

    const tierList = document.createElement("div");
    tierList.className = "amenity-tier-list";

    const tierRows = [];
    const flagCounts = transportFlagCounts(state.runtime);

    function syncTransportInputs() {
      tierRows.forEach(function (entry) {
        let isChecked = false;
        if (entry.type === "subtier") {
          isChecked = state.selectedTransportSubtiers.has(entry.value);
        } else if (entry.type === "mode") {
          isChecked = state.selectedTransportModes.has(entry.value);
        } else {
          isChecked = entry.value === "unscheduled"
            ? state.transportIncludeUnscheduled
            : state.transportRequireExceptionOnly;
        }
        entry.input.checked = isChecked;
        entry.input.disabled = !state.transportRealityVisible;
      });
      tierMeta.textContent = transportFilterSummary();
      tierDetails.classList.toggle("is-disabled", !state.transportRealityVisible);
      if (!state.transportRealityVisible) {
        tierDetails.open = false;
      }
    }

    transportTierOptions(state.runtime).forEach(function (option) {
      const row = document.createElement("label");
      row.className = "amenity-tier-row";
      row.htmlFor = "transport-tier-" + option.value;

      const textWrap = document.createElement("span");
      textWrap.className = "toggle-label";

      const title = document.createElement("strong");
      title.textContent = option.label;

      const subtitle = document.createElement("span");
      subtitle.textContent = String(option.count || 0) + " mapped";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.id = "transport-tier-" + option.value;
      input.checked = state.selectedTransportSubtiers.has(option.value);
      input.addEventListener("change", function () {
        if (input.checked) {
          state.selectedTransportSubtiers.add(option.value);
        } else {
          state.selectedTransportSubtiers.delete(option.value);
        }
        syncTransportInputs();
        applyTransportRealityFilter();
      });

      tierRows.push({ type: "subtier", value: option.value, input: input });
      textWrap.appendChild(title);
      textWrap.appendChild(subtitle);
      row.appendChild(textWrap);
      row.appendChild(input);
      tierList.appendChild(row);
    });

    transportModeOptions(state.runtime).forEach(function (option) {
      const row = document.createElement("label");
      row.className = "amenity-tier-row";
      row.htmlFor = "transport-mode-" + option.value;

      const textWrap = document.createElement("span");
      textWrap.className = "toggle-label";

      const title = document.createElement("strong");
      title.textContent = option.label;

      const subtitle = document.createElement("span");
      subtitle.textContent = String(option.count || 0) + " mapped";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.id = "transport-mode-" + option.value;
      input.checked = state.selectedTransportModes.has(option.value);
      input.addEventListener("change", function () {
        if (input.checked) {
          state.selectedTransportModes.add(option.value);
        } else {
          state.selectedTransportModes.delete(option.value);
        }
        syncTransportInputs();
        applyTransportRealityFilter();
      });

      tierRows.push({ type: "mode", value: option.value, input: input });
      textWrap.appendChild(title);
      textWrap.appendChild(subtitle);
      row.appendChild(textWrap);
      row.appendChild(input);
      tierList.appendChild(row);
    });

    [
      {
        type: "flag",
        value: "unscheduled",
        label: "Unscheduled",
        count: Number(flagCounts.is_unscheduled_stop || 0),
        onChange: function (checked) {
          state.transportIncludeUnscheduled = checked;
        }
      },
      {
        type: "flag",
        value: "exception_only",
        label: "Has calendar_dates-only bus service",
        count: Number(flagCounts.has_exception_only_service || 0),
        onChange: function (checked) {
          state.transportRequireExceptionOnly = checked;
        }
      }
    ].forEach(function (option) {
      const row = document.createElement("label");
      row.className = "amenity-tier-row";
      row.htmlFor = "transport-flag-" + option.value;

      const textWrap = document.createElement("span");
      textWrap.className = "toggle-label";

      const title = document.createElement("strong");
      title.textContent = option.label;

      const subtitle = document.createElement("span");
      subtitle.textContent = String(option.count || 0) + " mapped";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.id = "transport-flag-" + option.value;
      input.checked = option.value === "unscheduled"
        ? state.transportIncludeUnscheduled
        : state.transportRequireExceptionOnly;
      input.addEventListener("change", function () {
        option.onChange(input.checked);
        syncTransportInputs();
        applyTransportRealityFilter();
      });

      tierRows.push({ type: option.type, value: option.value, input: input });
      textWrap.appendChild(title);
      textWrap.appendChild(subtitle);
      row.appendChild(textWrap);
      row.appendChild(input);
      tierList.appendChild(row);
    });

    tierDetails.appendChild(tierList);
    elements.transitControls.appendChild(tierDetails);

    overlayInput.addEventListener("change", function () {
      state.transportRealityVisible = overlayInput.checked;
      syncTransportInputs();
      applyTransportRealityVisibility();
    });
    syncTransportInputs();
  }
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

  if (elements.transportRealityDownload) {
    const href = state.runtime.transport_reality_download_url || "/exports/transport-reality.zip";
    elements.transportRealityDownload.href = href;
    elements.transportRealityDownload.style.display = state.runtime.transport_reality_enabled ? "" : "none";
  }

  if (elements.transitNote) {
    const analysisDate = state.runtime.transit_analysis_date || "unknown date";
    elements.transitNote.textContent = "Snapshot as of " + analysisDate;
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

function amenityPopupHtml(properties) {
  const title = properties.name || formatAmenityLabel(properties.category || "Amenity");
  const details = [];
  const categoryLabel = formatAmenityLabel(properties.category || "");
  const tierLabel = formatAmenityLabel(properties.tier || "");
  if (properties.name && categoryLabel) {
    const categoryText = tierLabel
      ? categoryLabel + " • " + tierLabel
      : categoryLabel;
    details.push("<p>" + escapeHtml(categoryText) + "</p>");
  } else if (tierLabel) {
    details.push("<p>" + escapeHtml(tierLabel) + "</p>");
  }
  if (properties.source_ref) {
    details.push("<p>" + escapeHtml(properties.source_ref) + "</p>");
  }
  if (properties.source) {
    details.push("<p>Source: " + escapeHtml(properties.source) + "</p>");
  }
  if (properties.conflict_class && properties.conflict_class !== "osm_only") {
    details.push("<p>Merge: " + escapeHtml(properties.conflict_class) + "</p>");
  }
  return (
    '<div class="popup-content">' +
      "<h3>" + escapeHtml(title) + "</h3>" +
      details.join("") +
    "</div>"
  );
}

function serviceDesertPopupHtml(properties) {
  return (
    '<div class="popup-content">' +
      "<h3>Service desert candidate</h3>" +
      "<p>Resolution: " + escapeHtml(formatResolutionLabel(properties.resolution_m || resolutionForZoom(state.map.getZoom()))) + "</p>" +
      "<p>Baseline reachable GTFS stops: " + escapeHtml(String(properties.baseline_reachable_stop_count || 0)) + "</p>" +
      "<p>Reachable public departures (7d): " + escapeHtml(String(properties.reachable_public_departures_7d || 0)) + "</p>" +
    "</div>"
  );
}

function formatEffectiveUnits(value) {
  const numeric = Number(value || 0);
  return numeric.toFixed(2).replace(/\.?0+$/, "");
}

function inspectPopupHtml(payload) {
  if (!payload.valid_land) {
    return (
      '<div class="popup-content">' +
        "<h3>No land cell</h3>" +
        "<p>The canonical 50m surface is transparent at this location.</p>" +
        "<p>Visible grid: " + escapeHtml(formatResolutionLabel(payload.visible_resolution_m || 50)) + "</p>" +
      "</div>"
    );
  }

  const listHtml = SCORE_SECTIONS.map(function (category) {
    const title = category.charAt(0).toUpperCase() + category.slice(1);
    const count = Number((payload.counts || {})[category] || 0);
    const clusterCount = Number((payload.cluster_counts || {})[category] || 0);
    const effectiveUnits = formatEffectiveUnits((payload.effective_units || {})[category] || 0);
    const score = Number((payload.component_scores || {})[category] || 0).toFixed(1);
    return (
      "<li><strong>" + title + "</strong>: " +
      count + " raw, " +
      clusterCount + " clusters, " +
      effectiveUnits + " effective units, " +
      score + " points</li>"
    );
  }).join("");

  return (
    '<div class="popup-content">' +
      "<h3>Walk score " + Number(payload.total_score || 0).toFixed(1) + " / 100</h3>" +
      "<p>Exact surface: " + escapeHtml(formatResolutionLabel(payload.resolution_m || 50)) + "</p>" +
      "<p>Visible grid: " + escapeHtml(formatResolutionLabel(payload.visible_resolution_m || payload.resolution_m || 50)) + "</p>" +
      "<p>Land coverage: " + (Number(payload.effective_area_ratio || 0) * 100).toFixed(0) + "%</p>" +
      "<ul>" + listHtml + "</ul>" +
    "</div>"
  );
}

function coarseGridPopupHtml(properties) {
  const listHtml = SCORE_SECTIONS.map(function (category) {
    const title = category.charAt(0).toUpperCase() + category.slice(1);
    const count = Number(properties["count_" + category] || 0);
    const clusterCount = Number(properties["cluster_" + category] || 0);
    const effectiveUnits = formatEffectiveUnits(properties["effective_units_" + category] || 0);
    const score = Number(properties["score_" + category] || 0).toFixed(1);
    return (
      "<li><strong>" + title + "</strong>: " +
      count + " raw, " +
      clusterCount + " clusters, " +
      effectiveUnits + " effective units, " +
      score + " points</li>"
    );
  }).join("");

  return (
    '<div class="popup-content">' +
      "<h3>Walk score " + Number(properties.total_score || 0).toFixed(1) + " / 100</h3>" +
      "<p>Visible grid: " + escapeHtml(formatResolutionLabel(properties.resolution_m || resolutionForZoom(state.map.getZoom()))) + "</p>" +
      "<ul>" + listHtml + "</ul>" +
    "</div>"
  );
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
    style: buildRuntimeStyle(state.runtime, { windowOrigin: window.location.origin }),
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
    const transportRealityFeatures = state.map.queryRenderedFeatures(event.point, { layers: ["transport-reality-circle"] });
    if (transportRealityFeatures.length > 0) {
      const popupRows = Array.from(
        transportRealityFeatures.reduce(function (rowsBySourceRef, feature) {
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

    const amenityFeatures = state.map.queryRenderedFeatures(event.point, { layers: ["amenities-circle"] });
    if (amenityFeatures.length > 0) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(amenityPopupHtml(amenityFeatures[0].properties || {}))
        .addTo(state.map);
      return;
    }

    if (fineSurfaceEnabled()) {
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

    const desertFeatures = state.map.queryRenderedFeatures(event.point, { layers: ["service-deserts-fill"] });
    if (desertFeatures.length > 0) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(serviceDesertPopupHtml(desertFeatures[0].properties || {}))
        .addTo(state.map);
      return;
    }

    const activeLayerId = state.gridVisible ? activeGridLayerId() : null;
    const visibleGridLayers = activeLayerId ? [activeLayerId] : [];
    if (visibleGridLayers.length === 0) {
      return;
    }
    const gridFeatures = state.map.queryRenderedFeatures(event.point, { layers: visibleGridLayers });
    if (gridFeatures.length > 0) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(coarseGridPopupHtml(gridFeatures[0].properties || {}))
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
  state.enabledAmenityCategories = new Set();
  state.enabledAmenityTiers = new Map(
    Object.entries(defaultAmenityTierSelections(runtime)).map(function (entry) {
      return [entry[0], new Set(entry[1])];
    })
  );
  state.selectedTransportSubtiers = new Set();
  state.selectedTransportModes = new Set();
  state.transportIncludeUnscheduled = false;
  state.transportRequireExceptionOnly = false;
  buildAmenityControls();
  buildTransitControls();
  maybeBuildDebugGridControl();
  wireUi();
  updatePanelVisibility();
  initializeMap();
}

fetchWithTimeout("/api/runtime", undefined, RUNTIME_FETCH_TIMEOUT_MS)
  .then(function (response) {
    if (!response.ok) {
      return response.json().then(function (payload) {
        throw new Error(payload.error || response.statusText);
      });
    }
    return response.json();
  })
  .then(initializeApp)
  .catch(function (error) {
    const message = error && error.name === "AbortError"
      ? "Timed out loading runtime"
      : (error && error.message) || "Failed to load runtime";
    updateStatus(message);
    elements.gridToggle.disabled = true;
  });
