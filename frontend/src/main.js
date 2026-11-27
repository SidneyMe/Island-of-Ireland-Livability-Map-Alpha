import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import "./main.css";
import "./workspace.css";
import { initializeWorkspace } from "./workspace_ui.js";
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
  LANDUSE_CONTEXT_CLASS_COLORS,
  buildLanduseContextLayerFilter,
  defaultLanduseContextSelections,
  landuseContextOptions
} from "./landuse_filters.js";
import {
  CLICK_ACTIONS,
  resolveMapClickAction
} from "./click_priority.js";
import { cellGeometryCenter, completeCellGeometry } from "./cell_highlight.js";
import {
  DEFAULT_NOISE_OPACITY as runtimeDefaultNoiseOpacity,
  GRID_INSERT_BEFORE_LAYER_ID as runtimeGridInsertBeforeLayerId,
  LANDUSE_CONTEXT_DEFAULT_OPACITY as runtimeLanduseContextDefaultOpacity,
  LANDUSE_CONTEXT_FILL_LAYER_ID as runtimeLanduseContextFillLayerId,
  LANDUSE_CONTEXT_OUTLINE_LAYER_ID as runtimeLanduseContextOutlineLayerId,
  landuseContextLayerMinZoom as runtimeLanduseContextLayerMinZoom,
  noiseOutlineOpacity as runtimeNoiseOutlineOpacity,
  activeGridLifecycle as runtimeActiveGridLifecycle,
  availableGridTravelModes as runtimeAvailableGridTravelModes,
  activeDebugGridLayerId as runtimeActiveDebugGridLayerId,
  activeGridLayerId as runtimeActiveGridLayerId,
  activeGridOutlineLayerId as runtimeActiveGridOutlineLayerId,
  gridFillColorExpression as runtimeGridFillColorExpression,
  buildStyle as buildRuntimeStyle,
  buildActiveGridLayers as runtimeBuildActiveGridLayers,
  debugGridVisibilityPlan as runtimeDebugGridVisibilityPlan,
  fineSurfaceEnabled as runtimeFineSurfaceEnabled,
  inspectAvailable as runtimeInspectAvailable,
  isDailyAccessModel as runtimeIsDailyAccessModel,
  legacyFieldsUnavailable as runtimeLegacyFieldsUnavailable,
  gridScoreLayerLabel as runtimeGridScoreLayerLabel,
  gridScoreLayerOptions as runtimeGridScoreLayerOptions,
  gridTravelModeLabel as runtimeGridTravelModeLabel,
  normalizeGridTravelMode as runtimeNormalizeGridTravelMode,
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
import {
  emptyRouteFeatureCollection,
  focusFeatureFilter,
  mergeRouteFeatureCollections,
  normalizeRouteIndex,
  overviewModeFilter,
  parseRouteKeys,
  routeDirectionGroups,
  routeDisplayName,
  searchRoutes
} from "./route_network.js";
import { fetchWithTimeout } from "./api_client.js";
import { sourceLoaded as mapSourceLoaded } from "./map_source_guard.js";
import { amenityPopupHtml } from "./popups/amenity_popup.js";
import { noisePopupHtml } from "./popups/noise_popup.js";
import { inspectPopupHtml, coarseGridPopupHtml } from "./popups/grid_popup.js";
import { serviceDesertPopupHtml } from "./popups/service_desert_popup.js";
import { buildMetadataView } from "./build_metadata.js";
import { linkedBuildUnavailable, mapPermalink, parseMapUrlState } from "./url_state.js";
import {
  SHORTLIST_MAX_ITEMS,
  comparisonRows,
  createShortlistItem,
  loadShortlist,
  moveShortlistItem,
  refreshShortlistItems,
  refreshedShortlistItem,
  removeShortlistItem,
  renameShortlistItem,
  saveShortlist,
  shortlistExport,
  totalDeltaFromPrevious,
  toggleComparisonSelection,
  upsertShortlistItem
} from "./shortlist.js";

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const MIN_ZOOM = 5;
const DEBUG_GRID_QUERY_PARAM = "debug-grid";
const GRID_LAYER_STORAGE_KEY = "livability-grid-layer";
const GRID_TRAVEL_MODE_STORAGE_KEY = "livability-grid-travel-mode";
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
  transportNetworkControls: document.getElementById("transport-network-controls"),
  transitControls: document.getElementById("transit-controls"),
  noiseControls: document.getElementById("noise-controls"),
  noiseNote: document.getElementById("noise-note"),
  gridToggle: document.getElementById("grid-toggle"),
  gridLayerControls: document.getElementById("grid-layer-controls"),
  gridTravelModeControls: document.getElementById("grid-travel-mode-controls"),
  gridLayerNote: document.getElementById("grid-layer-note"),
  landuseControls: document.getElementById("landuse-controls"),
  landuseNote: document.getElementById("landuse-note"),
  map: document.getElementById("map"),
  mapStage: document.getElementById("map-stage"),
  dataProvenance: document.getElementById("data-provenance"),
  dataFreshness: document.getElementById("data-freshness"),
  dataOsm: document.getElementById("data-osm"),
  dataGtfs: document.getElementById("data-gtfs"),
  dataModel: document.getElementById("data-model"),
  dataUpdated: document.getElementById("data-updated"),
  dataBuild: document.getElementById("data-build"),
  linkedBuildWarning: document.getElementById("linked-build-warning"),
  useCurrentBuild: document.getElementById("use-current-build"),
  shortlistSection: document.getElementById("shortlist-section"),
  shortlistCount: document.getElementById("shortlist-count"),
  shortlistEmpty: document.getElementById("shortlist-empty"),
  shortlistItems: document.getElementById("shortlist-items"),
  shortlistCompare: document.getElementById("shortlist-compare"),
  shortlistExport: document.getElementById("shortlist-export"),
  shortlistClear: document.getElementById("shortlist-clear"),
  inspectDrawer: document.getElementById("inspect-drawer"),
  inspectContent: document.getElementById("inspect-content"),
  inspectClose: document.getElementById("inspect-close"),
  compareDrawer: document.getElementById("compare-drawer"),
  compareContent: document.getElementById("compare-content"),
  compareStatus: document.getElementById("compare-status"),
  compareClose: document.getElementById("compare-close"),
  compareShare: document.getElementById("compare-share"),
  compareSaveBoth: document.getElementById("compare-save-both"),
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
  selectedGridTravelMode: "walk",
  enabledAmenityCategories: new Set(),
  enabledAmenityTiers: new Map(),
  landuseVisible: false,
  selectedLanduseClasses: new Set(),
  landuseOpacity: runtimeLanduseContextDefaultOpacity,
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
  routeIndex: [],
  routeIndexLoaded: false,
  routeSearchQuery: "",
  focusedRouteKeys: [],
  selectedRouteKey: null,
  selectedRoutePatternId: null,
  routeFeatureCollection: emptyRouteFeatureCollection(),
  routeBrowserElement: null,
  pendingRouteAssets: null,
  serviceDesertsVisible: false,
  noiseVisible: false,
  noiseOpacity: runtimeDefaultNoiseOpacity,
  selectedNoiseMetric: "Lden",
  selectedNoiseSources: new Set(),
  selectedNoiseBands: new Set(),
  pendingInspect: null,
  initialUrlState: null,
  linkedBuildUnavailable: false,
  permalinkInspect: null,
  shortlist: [],
  compareSelectedIds: new Set(),
  compareItems: [],
  compareOpen: false,
  compareEphemeral: false,
  compareReturnFocus: null,
  compareLoadToken: 0,
  inspectOpen: false,
  inspectPayload: null,
  inspectReturnFocus: null,
  linkedCellMarker: null,
  linkedCellHighlightTimer: null,
  linkedCellGeometry: null
};

function updateStatus(message) {
  elements.statusPill.textContent = message;
}

function renderBuildMetadata() {
  if (!elements.dataProvenance) return;
  const view = buildMetadataView(state.runtime);
  elements.dataFreshness.textContent = view.statusLabel;
  elements.dataFreshness.className = "data-freshness data-freshness-" + view.status;
  elements.dataOsm.textContent = view.osmLabel;
  elements.dataGtfs.textContent = view.gtfsLabel;
  elements.dataModel.textContent = view.modelLabel;
  elements.dataUpdated.textContent = view.updatedLabel;
  elements.dataBuild.textContent = view.buildLabel;
  elements.dataProvenance.title = view.warnings.join("\n");
  if (elements.linkedBuildWarning) {
    elements.linkedBuildWarning.hidden = !state.linkedBuildUnavailable;
  }
}

function currentMapPermalink(inspect = state.permalinkInspect) {
  if (!state.map) return window.location.href;
  const center = state.map.getCenter();
  const compare = state.compareOpen && state.compareItems.length === 2
    ? state.compareItems.map(function (item) { return { lat: item.lat, lon: item.lon }; })
    : null;
  return mapPermalink(window.location.href, {
    lat: center.lat,
    lon: center.lng,
    zoom: state.map.getZoom(),
    mode: state.selectedGridTravelMode,
    layer: state.selectedGridLayer,
    buildKey: state.runtime && state.runtime.build_key,
    inspect: compare ? null : inspect,
    compare
  });
}

function syncPermalink() {
  if (!state.map || !window.history || typeof window.history.replaceState !== "function") return;
  window.history.replaceState(null, "", currentMapPermalink());
}

function popupBuildMetadata() {
  const view = buildMetadataView(state.runtime);
  return { model: view.modelLabel, buildKey: state.runtime && state.runtime.build_key };
}

function wirePopupPermalink(root = null) {
  const popupElement = root || (state.popup && state.popup.getElement && state.popup.getElement());
  const button = popupElement && popupElement.querySelector(".popup-permalink");
  if (!button) return;
  button.addEventListener("click", async function () {
    const link = currentMapPermalink();
    try {
      await window.navigator.clipboard.writeText(link);
      button.textContent = "Link copied";
    } catch (error) {
      button.textContent = "Copy failed";
      button.title = link;
    }
  });
}

function clearLinkedCellHighlight() {
  if (state.linkedCellHighlightTimer) {
    window.clearTimeout(state.linkedCellHighlightTimer);
    state.linkedCellHighlightTimer = null;
  }
  if (state.linkedCellMarker) {
    state.linkedCellMarker.remove();
    state.linkedCellMarker = null;
  }
  state.linkedCellGeometry = null;
  if (state.map) {
    const source = state.map.getSource("linked-cell-highlight");
    if (source && typeof source.setData === "function") {
      source.setData({ type: "FeatureCollection", features: [] });
    }
  }
}

function installLinkedCellHighlightLayers() {
  if (!state.map || state.map.getSource("linked-cell-highlight")) return;
  state.map.addSource("linked-cell-highlight", {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] }
  });
  state.map.addLayer({
    id: "linked-cell-highlight-fill",
    type: "fill",
    source: "linked-cell-highlight",
    paint: { "fill-color": "#d6006f", "fill-opacity": 0.2 }
  });
  state.map.addLayer({
    id: "linked-cell-highlight-halo",
    type: "line",
    source: "linked-cell-highlight",
    paint: { "line-color": "#ffffff", "line-width": 8, "line-opacity": 0.95 }
  });
  state.map.addLayer({
    id: "linked-cell-highlight-line",
    type: "line",
    source: "linked-cell-highlight",
    paint: { "line-color": "#d6006f", "line-width": 4, "line-opacity": 1 }
  });
}

function highlightRenderedLinkedCell(lngLat, renderedFeature = null) {
  installLinkedCellHighlightLayers();
  let feature = renderedFeature;
  if (!feature) {
    const layerId = activeGridLayerId();
    if (!layerId || !state.map.getLayer(layerId)) return false;
    const point = state.map.project([Number(lngLat.lng), Number(lngLat.lat)]);
    feature = state.map.queryRenderedFeatures(point, { layers: [layerId] })[0];
  }
  if (!feature || !feature.geometry) return false;
  let sourceFeatures = [];
  const cellId = String(feature.properties && feature.properties.cell_id || "");
  if (cellId && typeof state.map.querySourceFeatures === "function") {
    try {
      sourceFeatures = state.map.querySourceFeatures("livability", {
        sourceLayer: "grid",
        filter: ["==", ["get", "cell_id"], cellId]
      });
    } catch (error) {
      sourceFeatures = [];
    }
  }
  const geometry = completeCellGeometry(feature, sourceFeatures);
  if (!geometry) return false;
  state.linkedCellGeometry = {
    type: "Feature",
    properties: { linked_cell: 1 },
    geometry
  };
  const source = state.map.getSource("linked-cell-highlight");
  if (source && typeof source.setData === "function") source.setData(state.linkedCellGeometry);
  const center = cellGeometryCenter(geometry);
  if (center && state.linkedCellMarker) state.linkedCellMarker.setLngLat(center);
  return true;
}

function highlightLinkedCell(lngLat) {
  clearLinkedCellHighlight();
  const element = document.createElement("div");
  element.className = "permalink-cell-highlight";
  const core = document.createElement("span");
  core.className = "permalink-cell-highlight-core";
  core.setAttribute("aria-hidden", "true");
  element.appendChild(core);
  element.setAttribute("role", "img");
  element.setAttribute("aria-label", "Cell opened from shared link");
  element.title = "Cell opened from shared link";
  state.linkedCellMarker = new maplibregl.Marker({ element, anchor: "center" })
    .setLngLat([Number(lngLat.lng), Number(lngLat.lat)])
    .addTo(state.map);
  state.linkedCellHighlightTimer = window.setTimeout(function () {
    element.classList.add("permalink-cell-highlight-settled");
    state.linkedCellHighlightTimer = null;
  }, 3000);
  state.map.once("idle", function () {
    if (state.linkedCellMarker) highlightRenderedLinkedCell(lngLat);
  });
}

function savedCell(cellId) {
  return state.shortlist.some(function (item) { return item.cellId === String(cellId || ""); });
}

function newShortlistId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") return window.crypto.randomUUID();
  return "location-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 9);
}

function persistShortlist() {
  try {
    saveShortlist(window.localStorage, state.shortlist);
  } catch (error) {
    updateStatus("Shortlist could not be saved in this browser.");
    elements.statusPill.style.display = "";
  }
}

function modeTotal(item) {
  const mode = item && item.snapshot && item.snapshot.modes[state.selectedGridTravelMode];
  return mode && Number.isFinite(mode.total) ? mode.total : null;
}

function formatSigned(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "n/a";
  const number = Number(value);
  return (number > 0 ? "+" : "") + number.toFixed(1);
}

function updateShortlistMarkers() {
  if (!state.map) return;
  const source = state.map.getSource("shortlist-pins");
  if (!source || typeof source.setData !== "function") return;
  source.setData({
    type: "FeatureCollection",
    features: state.shortlist.map(function (item, index) {
      return {
        type: "Feature",
        id: item.id,
        properties: { id: item.id, position: String(index + 1), label: item.label },
        geometry: { type: "Point", coordinates: [item.lon, item.lat] }
      };
    })
  });
}

function installShortlistLayers() {
  if (!state.map || state.map.getSource("shortlist-pins")) return;
  state.map.addSource("shortlist-pins", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  state.map.addLayer({
    id: "shortlist-pins-circle",
    type: "circle",
    source: "shortlist-pins",
    paint: {
      "circle-radius": 13,
      "circle-color": "#1f5f45",
      "circle-stroke-width": 2,
      "circle-stroke-color": "#ffffff"
    }
  });
  state.map.addLayer({
    id: "shortlist-pins-label",
    type: "symbol",
    source: "shortlist-pins",
    layout: { "text-field": ["get", "position"], "text-size": 11, "text-allow-overlap": true },
    paint: { "text-color": "#ffffff" }
  });
  state.map.on("mouseenter", "shortlist-pins-circle", function () { state.map.getCanvas().style.cursor = "pointer"; });
  state.map.on("mouseleave", "shortlist-pins-circle", function () { state.map.getCanvas().style.cursor = ""; });
  updateShortlistMarkers();
}

function shortlistStatusText(item) {
  const total = modeTotal(item);
  const delta = totalDeltaFromPrevious(item, state.selectedGridTravelMode);
  const parts = [total === null ? "n/a" : total.toFixed(1) + " / 100", item.status];
  if (delta !== null) parts.push("Δ " + formatSigned(delta));
  return parts.join(" · ");
}

function renderShortlist() {
  if (!elements.shortlistSection) return;
  elements.shortlistSection.hidden = !runtimeIsDailyAccessModel(state.runtime);
  if (elements.shortlistSection.hidden) return;
  const selected = state.compareSelectedIds;
  Array.from(selected).forEach(function (id) {
    if (!state.shortlist.some(function (item) { return item.id === id; })) selected.delete(id);
  });
  elements.shortlistCount.textContent = state.shortlist.length + " / " + SHORTLIST_MAX_ITEMS;
  elements.shortlistEmpty.hidden = state.shortlist.length > 0;
  elements.shortlistItems.replaceChildren();
  state.shortlist.forEach(function (item, index) {
    const card = document.createElement("div");
    card.className = "shortlist-card";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selected.has(item.id);
    checkbox.disabled = !checkbox.checked && selected.size >= 2;
    checkbox.setAttribute("aria-label", "Select " + item.label + " for comparison");
    checkbox.addEventListener("change", function () {
      const result = toggleComparisonSelection(selected, item.id, checkbox.checked);
      state.compareSelectedIds = result.selected;
      renderShortlist();
    });

    const body = document.createElement("div");
    const title = document.createElement("button");
    title.type = "button";
    title.className = "shortlist-card-title";
    title.textContent = item.label;
    title.addEventListener("click", function () { openShortlistItem(item); });
    const meta = document.createElement("span");
    meta.className = "shortlist-card-meta shortlist-card-status-" + item.status;
    meta.textContent = shortlistStatusText(item);
    meta.title = item.lastError || "";
    body.append(title, meta);

    const buttons = document.createElement("div");
    buttons.className = "shortlist-card-buttons";
    [["↑", -1, index === 0], ["↓", 1, index === state.shortlist.length - 1]].forEach(function ([label, direction, disabled]) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = label;
      button.disabled = disabled;
      button.setAttribute("aria-label", (direction < 0 ? "Move up " : "Move down ") + item.label);
      button.addEventListener("click", function () {
        state.shortlist = moveShortlistItem(state.shortlist, item.id, direction);
        persistShortlist(); renderShortlist(); updateShortlistMarkers();
      });
      buttons.appendChild(button);
    });
    const rename = document.createElement("button");
    rename.type = "button"; rename.textContent = "✎"; rename.setAttribute("aria-label", "Rename " + item.label);
    rename.addEventListener("click", function () {
      const value = window.prompt("Location name", item.label);
      if (value === null) return;
      state.shortlist = renameShortlistItem(state.shortlist, item.id, value);
      persistShortlist(); renderShortlist(); updateShortlistMarkers();
    });
    const remove = document.createElement("button");
    remove.type = "button"; remove.textContent = "×"; remove.setAttribute("aria-label", "Remove " + item.label);
    remove.addEventListener("click", function () {
      state.shortlist = removeShortlistItem(state.shortlist, item.id);
      selected.delete(item.id);
      persistShortlist(); renderShortlist(); updateShortlistMarkers();
      if (state.compareOpen && !state.compareEphemeral) closeCompareDrawer();
    });
    buttons.append(rename, remove);
    if (item.status === "stale" || item.status === "error") {
      const retry = document.createElement("button");
      retry.type = "button"; retry.textContent = "↻"; retry.setAttribute("aria-label", "Retry " + item.label);
      retry.addEventListener("click", function () { refreshOneShortlistItem(item.id); });
      buttons.appendChild(retry);
    }
    card.append(checkbox, body, buttons);
    elements.shortlistItems.appendChild(card);
  });
  elements.shortlistCompare.textContent = "Compare (" + selected.size + ")";
  elements.shortlistCompare.disabled = selected.size !== 2;
  elements.shortlistExport.disabled = state.shortlist.length === 0;
  elements.shortlistClear.disabled = state.shortlist.length === 0;
}

function saveInspectToShortlist(payload, label = null) {
  const item = createShortlistItem(payload, {
    id: newShortlistId(),
    buildKey: state.runtime.build_key,
    label,
    position: state.shortlist.length + 1
  });
  const result = upsertShortlistItem(state.shortlist, item);
  if (result.full) {
    updateStatus("Shortlist is full (20 locations).");
    elements.statusPill.style.display = "";
    return false;
  }
  state.shortlist = result.items;
  persistShortlist(); renderShortlist(); updateShortlistMarkers();
  return true;
}

function wirePopupShortlist(payload, root = null) {
  const popupElement = root || (state.popup && state.popup.getElement && state.popup.getElement());
  const button = popupElement && popupElement.querySelector(".popup-shortlist");
  if (!button) return;
  button.addEventListener("click", function () {
    if (saveInspectToShortlist(payload)) {
      button.textContent = "Saved";
      button.disabled = true;
    }
  });
}

function ensureInspectDrawerElements() {
  let drawer = document.getElementById("inspect-drawer");
  let content = document.getElementById("inspect-content");
  let close = document.getElementById("inspect-close");
  if (!drawer || !content || !close) {
    if (drawer) drawer.remove();
    drawer = document.createElement("aside");
    drawer.id = "inspect-drawer";
    drawer.className = "inspect-drawer";
    drawer.setAttribute("role", "dialog");
    drawer.setAttribute("aria-modal", "false");
    drawer.setAttribute("aria-labelledby", "inspect-title");
    drawer.tabIndex = -1;
    drawer.hidden = true;

    const header = document.createElement("div");
    header.className = "inspect-header";
    const heading = document.createElement("div");
    const eyebrow = document.createElement("p");
    eyebrow.className = "eyebrow";
    eyebrow.textContent = "Map cell";
    const title = document.createElement("h2");
    title.id = "inspect-title";
    title.textContent = "Cell details";
    heading.append(eyebrow, title);
    close = document.createElement("button");
    close.id = "inspect-close";
    close.type = "button";
    close.setAttribute("aria-label", "Close cell details");
    close.textContent = "×";
    header.append(heading, close);
    content = document.createElement("div");
    content.id = "inspect-content";
    content.className = "inspect-content";
    drawer.append(header, content);
    (document.getElementById("app") || document.body).appendChild(drawer);
  }
  elements.inspectDrawer = drawer;
  elements.inspectContent = content;
  elements.inspectClose = close;
  if (!close.dataset.inspectCloseWired) {
    close.dataset.inspectCloseWired = "true";
    close.addEventListener("click", function () { closeInspectDrawer(); });
  }
}

function renderInspectDrawerHtml(html, payload = null) {
  ensureInspectDrawerElements();
  elements.inspectContent.innerHTML = html;
  wirePopupPermalink(elements.inspectContent);
  if (payload) wirePopupShortlist(payload, elements.inspectContent);
}

function closeInspectDrawer(options = {}) {
  if (!state.inspectOpen) return;
  state.inspectOpen = false;
  state.inspectPayload = null;
  clearLinkedCellHighlight();
  elements.inspectDrawer.hidden = true;
  document.body.classList.remove("inspect-open");
  if (options.clearPermalink !== false) {
    state.permalinkInspect = null;
    syncPermalink();
  }
  if (options.restoreFocus !== false && state.inspectReturnFocus && typeof state.inspectReturnFocus.focus === "function") {
    state.inspectReturnFocus.focus();
  }
  state.inspectReturnFocus = null;
}

function showGridPopup(lngLat, html, payload = null) {
  ensureInspectDrawerElements();
  if (state.compareOpen) closeCompareDrawer();
  if (state.popup && state.popup.isOpen && state.popup.isOpen()) state.popup.remove();
  if (!state.inspectOpen) state.inspectReturnFocus = document.activeElement;
  state.inspectOpen = true;
  state.inspectPayload = payload;
  elements.inspectDrawer.hidden = false;
  document.body.classList.add("inspect-open");
  renderInspectDrawerHtml(html, payload);
  if (!highlightRenderedLinkedCell(lngLat)) {
    state.map.once("idle", function () {
      if (state.inspectOpen) highlightRenderedLinkedCell(lngLat);
    });
  }
  state.permalinkInspect = { lat: Number(lngLat.lat), lon: Number(lngLat.lng) };
  syncPermalink();
  elements.inspectDrawer.focus();
}

function comparisonLocationCard(item) {
  const card = document.createElement("section");
  card.className = "compare-location";
  const title = document.createElement("h3");
  title.textContent = item.label;
  const total = document.createElement("div");
  total.className = "compare-total";
  const value = modeTotal(item);
  total.textContent = value === null ? "n/a" : value.toFixed(1) + " / 100";
  const meta = document.createElement("p");
  meta.className = "popup-muted";
  const mode = item.snapshot.modes[state.selectedGridTravelMode];
  const buildDelta = totalDeltaFromPrevious(item, state.selectedGridTravelMode);
  meta.textContent = (mode ? mode.networkStatus : "unavailable") + " · build " + String(item.buildKey || "unknown").slice(0, 10) +
    (buildDelta === null ? "" : " · build Δ " + formatSigned(buildDelta));
  card.append(title, total, meta);
  return card;
}

function comparisonValue(value, max, quality) {
  const wrap = document.createElement("div");
  wrap.className = "compare-value";
  const label = document.createElement("span");
  label.textContent = value === null ? "n/a" : Number(value).toFixed(1);
  const track = document.createElement("span");
  track.className = "compare-bar-track";
  const bar = document.createElement("span");
  bar.className = "compare-bar" + (quality ? " compare-bar-" + quality : "");
  const width = value === null ? 0 : Math.min(100, Math.abs(Number(value)) / Math.max(max, 0.0001) * 100);
  bar.style.setProperty("--bar-width", width.toFixed(1) + "%");
  track.appendChild(bar);
  wrap.append(label, track);
  return wrap;
}

function renderCompare() {
  if (!elements.compareContent) return;
  elements.compareContent.replaceChildren();
  if (state.compareItems.length !== 2) return;
  const [first, second] = state.compareItems;
  elements.compareContent.append(comparisonLocationCard(first), comparisonLocationCard(second));
  const rows = document.createElement("div");
  rows.className = "compare-rows";
  comparisonRows(first.snapshot, second.snapshot, state.selectedGridTravelMode).forEach(function (entry) {
    const row = document.createElement("div");
    row.className = "compare-row";
    const label = document.createElement("strong");
    label.textContent = entry.label;
    const max = entry.kind === "total" ? 100 : Math.max(Math.abs(entry.a || 0), Math.abs(entry.b || 0), 1);
    let qualityA = "";
    let qualityB = "";
    if (entry.a !== null && entry.b !== null && entry.a !== entry.b) {
      qualityA = entry.a > entry.b ? "better" : "worse";
      qualityB = entry.b > entry.a ? "better" : "worse";
    }
    const delta = document.createElement("span");
    delta.className = "compare-delta";
    delta.textContent = formatSigned(entry.delta);
    row.append(label, comparisonValue(entry.a, max, qualityA), comparisonValue(entry.b, max, qualityB), delta);
    rows.appendChild(row);
  });
  elements.compareContent.appendChild(rows);
  elements.compareStatus.textContent = runtimeGridTravelModeLabel(state.selectedGridTravelMode) + " · Δ is B − A";
  elements.compareSaveBoth.hidden = !state.compareEphemeral;
}

function openCompareDrawer(items, options = {}) {
  if (!Array.isArray(items) || items.length !== 2) return;
  if (state.inspectOpen) closeInspectDrawer({ restoreFocus: false });
  const wasOpen = state.compareOpen;
  if (!options.sharedLoad) state.compareLoadToken += 1;
  state.compareItems = items;
  state.compareOpen = true;
  state.compareEphemeral = Boolean(options.ephemeral);
  if (!wasOpen) state.compareReturnFocus = document.activeElement;
  elements.compareDrawer.hidden = false;
  document.body.classList.add("compare-open");
  renderCompare();
  syncPermalink();
  elements.compareDrawer.focus();
}

function closeCompareDrawer() {
  state.compareLoadToken += 1;
  state.compareOpen = false;
  state.compareItems = [];
  state.compareEphemeral = false;
  elements.compareDrawer.hidden = true;
  document.body.classList.remove("compare-open");
  syncPermalink();
  if (state.compareReturnFocus && typeof state.compareReturnFocus.focus === "function") {
    state.compareReturnFocus.focus();
  }
  state.compareReturnFocus = null;
}

async function openShortlistItem(item) {
  state.map.flyTo({ center: [item.lon, item.lat], zoom: Math.max(state.map.getZoom(), 14) });
  try {
    const payload = await fetchInspectPoint({ lat: item.lat, lng: item.lon }, { cancelPrevious: true });
    const index = state.shortlist.findIndex(function (entry) { return entry.id === item.id; });
    if (index >= 0) {
      state.shortlist[index] = refreshedShortlistItem(state.shortlist[index], payload, state.runtime.build_key);
      persistShortlist(); renderShortlist();
    }
    showGridPopup({ lat: item.lat, lng: item.lon }, inspectPopupHtml(payload, {
      selectedMode: state.selectedGridTravelMode,
      buildMetadata: popupBuildMetadata(),
      allowShortlist: true,
      shortlistSaved: true
    }), payload);
  } catch (error) {
    updateStatus((error && error.message) || "Shortlist location could not be refreshed");
    elements.statusPill.style.display = "";
  }
}

async function refreshOneShortlistItem(id) {
  const index = state.shortlist.findIndex(function (item) { return item.id === id; });
  if (index < 0) return;
  state.shortlist[index] = { ...state.shortlist[index], status: "refreshing", lastError: null };
  renderShortlist();
  try {
    const item = state.shortlist[index];
    const payload = await fetchInspectPoint({ lat: item.lat, lng: item.lon }, { cancelPrevious: false });
    state.shortlist[index] = refreshedShortlistItem(item, payload, state.runtime.build_key);
  } catch (error) {
    state.shortlist[index] = { ...state.shortlist[index], status: "stale", lastError: String(error && error.message || error || "Refresh failed") };
  }
  persistShortlist(); renderShortlist(); updateShortlistMarkers();
  if (state.compareOpen && !state.compareEphemeral) {
    state.compareItems = Array.from(state.compareSelectedIds).map(function (selectedId) {
      return state.shortlist.find(function (item) { return item.id === selectedId; });
    }).filter(Boolean);
    if (state.compareItems.length === 2) renderCompare(); else closeCompareDrawer();
  }
}

function downloadShortlist() {
  const payload = shortlistExport(state.shortlist);
  const blob = new Blob([JSON.stringify(payload, null, 2) + "\n"], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "livability-shortlist-" + new Date().toISOString().slice(0, 10) + ".json";
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(function () { URL.revokeObjectURL(url); }, 0);
}

async function openSharedComparison(points) {
  if (!Array.isArray(points) || points.length !== 2 || !runtimeIsDailyAccessModel(state.runtime)) return;
  if (state.inspectOpen) closeInspectDrawer({ restoreFocus: false });
  const loadToken = ++state.compareLoadToken;
  state.compareOpen = true;
  state.compareEphemeral = true;
  state.compareReturnFocus = document.activeElement;
  elements.compareDrawer.hidden = false;
  document.body.classList.add("compare-open");
  elements.compareStatus.textContent = "Loading shared comparison…";
  elements.compareDrawer.focus();
  try {
    const payloads = await Promise.all(points.map(function (point) {
      return fetchInspectPoint({ lat: point.lat, lng: point.lon }, { cancelPrevious: false });
    }));
    const items = payloads.map(function (payload, index) {
      return createShortlistItem(payload, {
        id: "shared-" + index,
        buildKey: state.runtime.build_key,
        label: "Location " + (index + 1),
        position: index + 1
      });
    });
    if (loadToken !== state.compareLoadToken || !state.compareOpen) return;
    if (items.some(function (item) { return !item; })) throw new Error("One shared point is not a valid land cell");
    openCompareDrawer(items, { ephemeral: true, sharedLoad: true });
  } catch (error) {
    if (loadToken !== state.compareLoadToken) return;
    elements.compareStatus.textContent = (error && error.message) || "Shared comparison could not be loaded";
  }
}

async function refreshShortlistForBuild() {
  if (!runtimeIsDailyAccessModel(state.runtime) || !state.shortlist.some(function (item) { return item.buildKey !== state.runtime.build_key; })) return;
  state.shortlist = await refreshShortlistItems(
    state.shortlist,
    state.runtime.build_key,
    function (point) {
      return fetchInspectPoint({ lat: point.lat, lng: point.lon }, { cancelPrevious: false });
    },
    {
      concurrency: 3,
      onUpdate: function (items) {
        state.shortlist = items;
        renderShortlist();
      }
    }
  );
  persistShortlist(); renderShortlist(); updateShortlistMarkers();
}

function fineSurfaceEnabled() {
  return runtimeFineSurfaceEnabled(state.runtime);
}

// v4 inspect reads the score shards, so it works without fine tiles (dev).
function inspectOnClick() {
  return fineSurfaceEnabled()
    || (runtimeIsDailyAccessModel(state.runtime) && runtimeInspectAvailable(state.runtime));
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
    state.selectedGridLayer,
    state.selectedGridTravelMode
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

function readSavedGridTravelMode() {
  try {
    return runtimeNormalizeGridTravelMode(
      window.sessionStorage.getItem(GRID_TRAVEL_MODE_STORAGE_KEY),
      state.runtime
    );
  } catch (error) {
    return "walk";
  }
}

function saveGridTravelMode(value) {
  try {
    window.sessionStorage.setItem(
      GRID_TRAVEL_MODE_STORAGE_KEY,
      runtimeNormalizeGridTravelMode(value, state.runtime)
    );
  } catch (error) {
    // Session storage is best-effort only.
  }
}

function updateGridLayerNote() {
  if (!elements.gridLayerNote) return;
  elements.gridLayerNote.textContent =
    "Active layer: " + runtimeGridTravelModeLabel(state.selectedGridTravelMode) +
    " · " + runtimeGridScoreLayerLabel(state.selectedGridLayer);
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
      runtimeGridFillColorExpression(state.selectedGridLayer, state.selectedGridTravelMode)
    );
  }
  updateGridLayerNote();
  renderShortlist();
  if (state.compareOpen) renderCompare();
  syncPermalink();
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
    const descriptions = {
      combined: "The everyday essentials, together",
      shops: "Groceries & errands",
      transport: "Stops & connections",
      healthcare: "Care & pharmacies",
      parks: "Green space nearby"
    };
    subtitle.textContent = descriptions[option.value] || "Category score";

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

function buildGridTravelModeControls() {
  if (!elements.gridTravelModeControls) return;
  elements.gridTravelModeControls.replaceChildren();
  const options = runtimeAvailableGridTravelModes(state.runtime);
  elements.gridTravelModeControls.hidden = options.length <= 1;
  options.forEach(function (option) {
    const label = document.createElement("label");
    label.className = "toggle-row grid-layer-row";
    label.htmlFor = "grid-travel-mode-" + option.value;

    const title = document.createElement("strong");
    title.textContent = option.label;

    const input = document.createElement("input");
    input.type = "radio";
    input.name = "grid-travel-mode";
    input.id = "grid-travel-mode-" + option.value;
    input.checked = state.selectedGridTravelMode === option.value;
    input.addEventListener("change", function () {
      if (!input.checked) return;
      state.selectedGridTravelMode = option.value;
      saveGridTravelMode(option.value);
      applyGridLayerSelection();
      if (state.inspectOpen && state.inspectPayload) {
        renderInspectDrawerHtml(inspectPopupHtml(state.inspectPayload, {
          selectedMode: state.selectedGridTravelMode,
          buildMetadata: popupBuildMetadata(),
          allowShortlist: true,
          shortlistSaved: savedCell(state.inspectPayload.cell_id)
        }), state.inspectPayload);
      }
    });

    label.appendChild(title);
    label.appendChild(input);
    elements.gridTravelModeControls.appendChild(label);
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
  const label = state.panelHidden ? "Show panel" : "Hide panel";
  elements.panelToggle.textContent = state.panelHidden ? "›" : "‹";
  elements.panelToggle.setAttribute("aria-label", label);
  elements.panelToggle.title = label;
  elements.panelToggle.setAttribute("aria-expanded", state.panelHidden ? "false" : "true");
}

function ensurePanelHeaderActions() {
  const header = elements.controlPanel && elements.controlPanel.querySelector(".panel-header");
  if (!header || !elements.panelToggle) return;
  let actions = header.querySelector(".panel-header-actions");
  if (!actions) {
    actions = document.createElement("div");
    actions.className = "panel-header-actions";
    header.appendChild(actions);
  }
  actions.appendChild(elements.panelToggle);
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
  return mapSourceLoaded(state.map, "livability", state.gridDebug.sourceLoaded);
}

function queryGridSourceCount(filter) {
  if (!state.map || !livabilitySourceLoaded()) return 0;
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
  if (!state.map || !livabilitySourceLoaded()) return {};
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
  const sourceLoaded = livabilitySourceLoaded();
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
    sourceCount: sourceLoaded ? queryGridSourceCount(lifecycle.filter) : 0,
    renderedCount: queryGridRenderedCount(activeFillLayer),
    sourceResolutions: sourceLoaded ? queryGridSourceResolutionCounts() : {},
    fillLayerPresent: fillLayerPresent,
    outlineLayerPresent: outlineLayerPresent,
    fillVisibility: fillLayerPresent
      ? String(state.map.getLayoutProperty(activeFillLayer, "visibility") || "visible")
      : "none",
    outlineVisibility: outlineLayerPresent
      ? String(state.map.getLayoutProperty(activeOutlineLayer, "visibility") || "visible")
      : "none",
    sourceLoaded: sourceLoaded
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
    state.selectedGridLayer,
    state.selectedGridTravelMode
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
  applyTransportNetworkVisibility();
}

const ROUTE_OVERVIEW_LAYERS = [
  "transport-routes-casing",
  "transport-routes-line",
  "transport-routes-approximate",
  "transport-transfer-hubs"
];
const ROUTE_FOCUS_LAYERS = [
  "transport-focus-casing",
  "transport-focus-line",
  "transport-focus-approximate",
  "transport-focus-arrows",
  "transport-focus-stops",
  "transport-focus-transfers",
  "transport-focus-endpoints"
];

function setOptionalLayerVisibility(layerId, visibility) {
  if (state.map && state.map.getLayer(layerId)) {
    state.map.setLayoutProperty(layerId, "visibility", visibility);
  }
}

function applyTransportNetworkFilters() {
  if (!state.map) return;
  const modeFilter = overviewModeFilter(state.selectedTransportModes);
  if (state.map.getLayer("transport-routes-casing")) {
    state.map.setFilter("transport-routes-casing", modeFilter);
    state.map.setFilter("transport-routes-line", ["all", modeFilter, ["!=", ["get", "geometry_source"], "stop_sequence_approximate"]]);
    state.map.setFilter("transport-routes-approximate", ["all", modeFilter, ["==", ["get", "geometry_source"], "stop_sequence_approximate"]]);
    const hubModeFilters = Array.from(state.selectedTransportModes).map(function (mode) {
      return ["in", "," + mode + ",", ["concat", ",", ["coalesce", ["get", "modes"], ""], ","]];
    });
    state.map.setFilter("transport-transfer-hubs", hubModeFilters.length ? ["any", ...hubModeFilters] : ["==", 1, 0]);
  }
  const patternId = state.selectedRoutePatternId;
  const lineKinds = ["pattern"];
  ["transport-focus-casing", "transport-focus-arrows"].forEach(function (layerId) {
    if (state.map.getLayer(layerId)) state.map.setFilter(layerId, focusFeatureFilter(state.selectedTransportModes, patternId, lineKinds));
  });
  if (state.map.getLayer("transport-focus-line")) {
    state.map.setFilter("transport-focus-line", ["all", focusFeatureFilter(state.selectedTransportModes, patternId, lineKinds), ["!=", ["get", "geometry_source"], "stop_sequence_approximate"]]);
    state.map.setFilter("transport-focus-approximate", ["all", focusFeatureFilter(state.selectedTransportModes, patternId, lineKinds), ["==", ["get", "geometry_source"], "stop_sequence_approximate"]]);
  }
  const patternFilter = patternId ? ["==", ["get", "pattern_id"], patternId] : null;
  ["transport-focus-stops", "transport-focus-endpoints"].forEach(function (layerId) {
    if (!state.map.getLayer(layerId)) return;
    const base = layerId === "transport-focus-endpoints"
      ? ["all", ["==", ["get", "feature_kind"], "stop"], ["==", ["get", "is_endpoint"], true]]
      : ["==", ["get", "feature_kind"], "stop"];
    const stopModeFilter = ["in", ["get", "mode"], ["literal", Array.from(state.selectedTransportModes)]];
    state.map.setFilter(layerId, patternFilter ? ["all", base, patternFilter, stopModeFilter] : ["all", base, stopModeFilter]);
  });
  if (state.map.getLayer("transport-focus-transfers")) {
    const focusHubModeFilters = Array.from(state.selectedTransportModes).map(function (mode) {
      return ["in", "," + mode + ",", ["concat", ",", ["coalesce", ["get", "modes"], ""], ","]];
    });
    state.map.setFilter("transport-focus-transfers", [
      "all",
      ["==", ["get", "feature_kind"], "transfer"],
      focusHubModeFilters.length ? ["any", ...focusHubModeFilters] : ["==", 1, 0]
    ]);
  }
}

function applyTransportNetworkVisibility() {
  if (!state.map) return;
  const enabled = Boolean(state.runtime.transport_network_enabled && state.transportRealityVisible);
  const focused = state.focusedRouteKeys.length > 0;
  ROUTE_OVERVIEW_LAYERS.forEach(function (layerId) {
    setOptionalLayerVisibility(layerId, enabled && !focused ? "visible" : "none");
  });
  ROUTE_FOCUS_LAYERS.forEach(function (layerId) {
    setOptionalLayerVisibility(layerId, enabled && focused ? "visible" : "none");
  });
  applyTransportNetworkFilters();
}

function routeForKey(routeKey) {
  return state.routeIndex.find(function (route) { return route.route_key === routeKey; }) || null;
}

function escapeTransportText(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function ensureRouteIndex() {
  if (state.routeIndexLoaded) return state.routeIndex;
  const url = String(state.runtime.transport_route_index_url || "");
  if (!state.runtime.transport_network_enabled || !url) return [];
  const response = await fetchWithTimeout(url, {}, INSPECT_FETCH_TIMEOUT_MS);
  if (!response.ok) throw new Error("Route index unavailable (" + response.status + ")");
  state.routeIndex = normalizeRouteIndex(await response.json());
  state.routeIndexLoaded = true;
  renderRouteBrowser();
  return state.routeIndex;
}

function routeAssetTransferKeys() {
  const keys = new Set();
  state.routeFeatureCollection.features.forEach(function (feature) {
    if (feature && feature.properties && feature.properties.feature_kind === "transfer") {
      parseRouteKeys(feature.properties.route_keys).forEach(function (key) {
        if (key !== state.selectedRouteKey) keys.add(key);
      });
    }
  });
  return Array.from(keys);
}

async function focusRoutes(routeKeys, selectedRouteKey = null) {
  await ensureRouteIndex();
  const keys = Array.from(new Set((routeKeys || []).map(String))).filter(function (key) {
    const route = routeForKey(key);
    return route && state.selectedTransportModes.has(route.mode);
  });
  if (state.pendingRouteAssets) state.pendingRouteAssets.abort();
  state.pendingRouteAssets = new AbortController();
  const controller = state.pendingRouteAssets;
  try {
    const payloads = await Promise.all(keys.map(async function (key) {
      const route = routeForKey(key);
      const response = await fetchWithTimeout(route.asset_url, { signal: controller.signal }, INSPECT_FETCH_TIMEOUT_MS);
      if (!response.ok) throw new Error("Route detail unavailable (" + response.status + ")");
      return response.json();
    }));
    if (controller.signal.aborted) return;
    state.focusedRouteKeys = keys;
    state.selectedRouteKey = selectedRouteKey && keys.includes(selectedRouteKey) ? selectedRouteKey : null;
    state.selectedRoutePatternId = null;
    state.routeFeatureCollection = mergeRouteFeatureCollections(payloads);
    const source = state.map && state.map.getSource("transport-route-focus");
    if (source) source.setData(state.routeFeatureCollection);
    applyTransportNetworkVisibility();
    renderRouteBrowser();
  } catch (error) {
    if (error && error.name === "AbortError") return;
    updateStatus(error.message || "Unable to load route");
    elements.statusPill.style.display = "";
  }
}

function clearRouteSelection() {
  if (state.pendingRouteAssets) state.pendingRouteAssets.abort();
  state.focusedRouteKeys = [];
  state.selectedRouteKey = null;
  state.selectedRoutePatternId = null;
  state.routeFeatureCollection = emptyRouteFeatureCollection();
  const source = state.map && state.map.getSource("transport-route-focus");
  if (source) source.setData(state.routeFeatureCollection);
  applyTransportNetworkVisibility();
  renderRouteBrowser();
}

function reconcileFocusedRoutesToModes() {
  if (!state.focusedRouteKeys.length) return;
  const allowed = state.focusedRouteKeys.filter(function (key) {
    const route = routeForKey(key);
    return route && state.selectedTransportModes.has(route.mode);
  });
  if (!allowed.length) {
    clearRouteSelection();
    return;
  }
  if (allowed.length !== state.focusedRouteKeys.length) {
    const selected = allowed.includes(state.selectedRouteKey) ? state.selectedRouteKey : null;
    focusRoutes(allowed, selected);
  }
}

function renderRouteBrowser() {
  const target = state.routeBrowserElement;
  if (!target) return;
  target.replaceChildren();
  if (!state.runtime.transport_network_enabled) {
    const message = document.createElement("p");
    message.className = "panel-copy";
    message.textContent = "Route lines are not available in this build; stop points still work.";
    target.appendChild(message);
    return;
  }
  const search = document.createElement("input");
  search.type = "search";
  search.className = "transport-route-search";
  search.placeholder = "Route number, name or terminus";
  search.value = state.routeSearchQuery;
  search.disabled = !state.transportRealityVisible;
  search.addEventListener("input", function () {
    state.routeSearchQuery = search.value;
    renderRouteBrowser();
    const replacement = state.routeBrowserElement && state.routeBrowserElement.querySelector("input[type=search]");
    if (replacement) {
      replacement.focus();
      replacement.setSelectionRange(replacement.value.length, replacement.value.length);
    }
  });
  target.appendChild(search);

  if (state.focusedRouteKeys.length) {
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "transport-route-clear";
    clear.textContent = "Clear selection";
    clear.addEventListener("click", clearRouteSelection);
    target.appendChild(clear);
  }

  const selected = routeForKey(state.selectedRouteKey);
  if (selected) {
    const heading = document.createElement("strong");
    heading.className = "transport-route-selection-title";
    heading.textContent = routeDisplayName(selected);
    target.appendChild(heading);
    const allDirections = document.createElement("button");
    allDirections.type = "button";
    allDirections.className = "transport-direction-button";
    allDirections.textContent = "Both directions";
    allDirections.disabled = !state.selectedRoutePatternId;
    allDirections.addEventListener("click", function () {
      state.selectedRoutePatternId = null;
      applyTransportNetworkFilters();
      renderRouteBrowser();
    });
    target.appendChild(allDirections);
    routeDirectionGroups(selected, state.routeFeatureCollection).forEach(function (group) {
      if (group.directions.length === 1) {
        const direction = group.directions[0];
        const button = document.createElement("button");
        button.type = "button";
        button.className = "transport-direction-button";
        button.textContent = group.label;
        button.disabled = state.selectedRoutePatternId === direction.pattern_id;
        button.addEventListener("click", function () {
          state.selectedRoutePatternId = direction.pattern_id;
          applyTransportNetworkFilters();
          renderRouteBrowser();
        });
        target.appendChild(button);
        return;
      }

      const select = document.createElement("select");
      select.className = "transport-direction-select";
      select.setAttribute("aria-label", group.label + " variants");
      const prompt = document.createElement("option");
      prompt.value = "";
      prompt.textContent = group.label + " (" + group.directions.length + " variants)";
      prompt.disabled = true;
      select.appendChild(prompt);

      const baseLabels = group.directions.map(function (direction) {
        const origin = String(direction.origin_name || "").trim();
        const destination = String(direction.destination_name || "").trim();
        return origin && destination ? origin + " → " + destination : group.label;
      });
      const labelCounts = new Map();
      baseLabels.forEach(function (label) { labelCounts.set(label, (labelCounts.get(label) || 0) + 1); });
      const labelIndexes = new Map();
      group.directions.forEach(function (direction, index) {
        const baseLabel = baseLabels[index];
        const nextIndex = (labelIndexes.get(baseLabel) || 0) + 1;
        labelIndexes.set(baseLabel, nextIndex);
        const option = document.createElement("option");
        option.value = String(direction.pattern_id);
        option.textContent = labelCounts.get(baseLabel) > 1
          ? baseLabel + " · variant " + nextIndex
          : baseLabel;
        select.appendChild(option);
      });
      const selectedInGroup = group.directions.some(function (direction) {
        return String(direction.pattern_id) === String(state.selectedRoutePatternId || "");
      });
      select.value = selectedInGroup ? String(state.selectedRoutePatternId) : "";
      select.addEventListener("change", function () {
        if (!select.value) return;
        state.selectedRoutePatternId = select.value;
        applyTransportNetworkFilters();
        renderRouteBrowser();
      });
      target.appendChild(select);
    });
    const transferKeys = routeAssetTransferKeys();
    if (transferKeys.length) {
      const label = document.createElement("span");
      label.className = "transport-route-meta";
      label.textContent = "Transfers";
      target.appendChild(label);
      transferKeys.slice(0, 12).forEach(function (key) {
        const route = routeForKey(key);
        if (!route) return;
        const button = document.createElement("button");
        button.type = "button";
        button.className = "transport-route-result";
        button.textContent = routeDisplayName(route);
        button.addEventListener("click", function () { focusRoutes([key], key); });
        target.appendChild(button);
      });
    }
    return;
  }

  if (state.focusedRouteKeys.length) {
    const label = document.createElement("span");
    label.className = "transport-route-meta";
    label.textContent = "Routes at selection";
    target.appendChild(label);
    state.focusedRouteKeys.map(routeForKey).filter(Boolean).forEach(function (route) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "transport-route-result";
      const swatch = document.createElement("span");
      swatch.className = "transport-route-swatch";
      swatch.style.background = route.route_color || "#2563eb";
      button.appendChild(swatch);
      button.appendChild(document.createTextNode(routeDisplayName(route)));
      button.addEventListener("click", function () { focusRoutes([route.route_key], route.route_key); });
      target.appendChild(button);
    });
    return;
  }

  const matches = searchRoutes(state.routeIndex, state.routeSearchQuery, state.selectedTransportModes, 12);
  matches.forEach(function (route) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "transport-route-result";
    const swatch = document.createElement("span");
    swatch.className = "transport-route-swatch";
    swatch.style.background = "#" + String(route.route_color || "2563eb").replace(/^#/, "");
    button.appendChild(swatch);
    button.appendChild(document.createTextNode(routeDisplayName(route)));
    button.addEventListener("click", function () { focusRoutes([route.route_key], route.route_key); });
    target.appendChild(button);
  });
}

function applyServiceDesertVisibility() {
  if (!state.map) return;
  state.map.setLayoutProperty(
    "service-deserts-fill",
    "visibility",
    state.serviceDesertsVisible ? "visible" : "none"
  );
}

function landuseFilterSummary() {
  const options = landuseContextOptions(state.runtime);
  const selectedCount = state.selectedLanduseClasses.size;
  const totalCount = options.length;
  return selectedCount === totalCount
    ? "all classes selected"
    : selectedCount + " of " + totalCount + " selected";
}

function updateLanduseNote() {
  if (!elements.landuseNote) return;
  if (!state.runtime.landuse_context_enabled) {
    elements.landuseNote.textContent = "No land-use context overlay in this build";
    return;
  }
  const minZoom = Number(runtimeLanduseContextLayerMinZoom(state.runtime));
  const maxZoom = Number(state.runtime.landuse_context_max_zoom || 11);
  const caveat = "Residential, commercial, industrial, retail, farmland and forest. This visual overlay does not change the score.";
  elements.landuseNote.title = caveat;
  elements.landuseNote.textContent = state.landuseVisible
    ? "On · visible at zoom " + minZoom + "–" + maxZoom
    : "Off";
}

function applyLanduseFilter() {
  if (!state.map) return;
  if (!state.map.getLayer(runtimeLanduseContextFillLayerId)) return;
  const filter = buildLanduseContextLayerFilter({
    selectedClasses: state.selectedLanduseClasses
  });
  state.map.setFilter(runtimeLanduseContextFillLayerId, filter);
  if (state.map.getLayer(runtimeLanduseContextOutlineLayerId)) {
    state.map.setFilter(runtimeLanduseContextOutlineLayerId, filter);
  }
}

function applyLanduseOpacity() {
  if (!state.map) return;
  if (!state.map.getLayer(runtimeLanduseContextFillLayerId)) return;
  const fillOpacity = Number(state.landuseOpacity ?? runtimeLanduseContextDefaultOpacity);
  state.map.setPaintProperty(runtimeLanduseContextFillLayerId, "fill-opacity", fillOpacity);
  if (state.map.getLayer(runtimeLanduseContextOutlineLayerId)) {
    state.map.setPaintProperty(
      runtimeLanduseContextOutlineLayerId,
      "line-opacity",
      Math.max(0, Math.min(0.35, fillOpacity * 0.65))
    );
  }
}

function applyLanduseVisibility() {
  if (!state.map) return;
  if (!state.map.getLayer(runtimeLanduseContextFillLayerId)) return;
  state.map.setLayoutProperty(
    runtimeLanduseContextFillLayerId,
    "visibility",
    state.landuseVisible ? "visible" : "none"
  );
  if (state.map.getLayer(runtimeLanduseContextOutlineLayerId)) {
    state.map.setLayoutProperty(
      runtimeLanduseContextOutlineLayerId,
      "visibility",
      state.landuseVisible ? "visible" : "none"
    );
  }
  applyLanduseFilter();
  applyLanduseOpacity();
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
  elements.noiseNote.title = caveat;
  elements.noiseNote.textContent = state.noiseVisible ? "On" : "Off";
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
  if (elements.transportNetworkControls) elements.transportNetworkControls.replaceChildren();
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
    overlayTitle.textContent = "Transport network";
    if (color) {
      overlayTitle.style.color = color;
    }

    const overlaySubtitle = document.createElement("span");
    const routeCount = Number((state.runtime.transport_route_counts || {}).total || 0);
    overlaySubtitle.textContent = String(transportMappedCount(counts)) + " stops · " + routeCount + " routes";

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
      elements.transportNetworkControls.appendChild(card);
      return;
    }

    const routeBrowser = document.createElement("div");
    routeBrowser.className = "transport-route-browser";
    state.routeBrowserElement = routeBrowser;
    card.appendChild(routeBrowser);

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
        if (entry.type === "mode") reconcileFocusedRoutesToModes();
        applyTransportNetworkVisibility();
        renderRouteBrowser();
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
      renderRouteBrowser();
      if (state.transportRealityVisible) {
        ensureRouteIndex().catch(function (error) {
          updateStatus(error.message || "Route index unavailable");
          elements.statusPill.style.display = "";
        });
      }
    });

    syncTransportInputs();
    renderRouteBrowser();
    elements.transportNetworkControls.appendChild(card);
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

function buildLanduseControls() {
  if (!elements.landuseControls) return;
  elements.landuseControls.replaceChildren();
  if (!state.runtime.landuse_context_enabled) {
    updateLanduseNote();
    return;
  }

  const overlayLabel = document.createElement("label");
  overlayLabel.className = "toggle-row";
  overlayLabel.htmlFor = "landuse-toggle";

  const overlayTextWrap = document.createElement("span");
  overlayTextWrap.className = "toggle-label";

  const overlayTitle = document.createElement("strong");
  overlayTitle.textContent = "Show land-use context";

  const overlaySubtitle = document.createElement("span");
  const minZoom = Number(runtimeLanduseContextLayerMinZoom(state.runtime));
  const maxZoom = Number(state.runtime.landuse_context_max_zoom || 11);
  overlaySubtitle.textContent = "Visible at zoom " + minZoom + "–" + maxZoom;

  const overlayInput = document.createElement("input");
  overlayInput.type = "checkbox";
  overlayInput.id = "landuse-toggle";
  overlayInput.checked = state.landuseVisible;

  overlayTextWrap.appendChild(overlayTitle);
  overlayTextWrap.appendChild(overlaySubtitle);
  overlayLabel.appendChild(overlayTextWrap);
  overlayLabel.appendChild(overlayInput);
  elements.landuseControls.appendChild(overlayLabel);

  const filterDetails = document.createElement("details");
  filterDetails.className = "amenity-tier-details";

  const filterSummary = document.createElement("summary");
  filterSummary.className = "amenity-tier-summary";

  const filterSummaryLabel = document.createElement("span");
  filterSummaryLabel.textContent = "Classes";

  const filterMeta = document.createElement("span");
  filterMeta.className = "amenity-tier-meta";
  filterMeta.textContent = landuseFilterSummary();

  filterSummary.appendChild(filterSummaryLabel);
  filterSummary.appendChild(filterMeta);
  filterDetails.appendChild(filterSummary);

  const filterList = document.createElement("div");
  filterList.className = "amenity-tier-list";
  const filterRows = [];

  function syncLanduseInputs() {
    filterRows.forEach(function (entry) {
      entry.input.checked = state.selectedLanduseClasses.has(entry.value);
      entry.input.disabled = !state.landuseVisible;
    });
    filterMeta.textContent = landuseFilterSummary();
    filterDetails.classList.toggle("is-disabled", !state.landuseVisible);
    if (!state.landuseVisible) {
      filterDetails.open = false;
    }
    updateLanduseNote();
  }

  function appendFilterRow(option) {
    const row = document.createElement("label");
    row.className = "amenity-tier-row";
    row.htmlFor = "landuse-" + option.value;

    const textWrap = document.createElement("span");
    textWrap.className = "toggle-label";

    const color = LANDUSE_CONTEXT_CLASS_COLORS[option.value] || "#d9d9d9";
    const titleRow = document.createElement("span");
    titleRow.className = "landuse-class-title";

    const swatch = document.createElement("span");
    swatch.className = "landuse-class-swatch";
    swatch.style.backgroundColor = color;
    swatch.setAttribute("aria-hidden", "true");

    const title = document.createElement("strong");
    title.textContent = option.label;

    const subtitle = document.createElement("span");
    subtitle.textContent = String(option.count || 0) + " mapped";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.id = "landuse-" + option.value;
    input.checked = state.selectedLanduseClasses.has(option.value);
    input.style.accentColor = color;
    input.addEventListener("change", function () {
      if (input.checked) {
        state.selectedLanduseClasses.add(option.value);
      } else {
        state.selectedLanduseClasses.delete(option.value);
      }
      syncLanduseInputs();
      applyLanduseFilter();
    });

    filterRows.push({ value: option.value, input: input });
    titleRow.appendChild(swatch);
    titleRow.appendChild(title);
    textWrap.appendChild(titleRow);
    textWrap.appendChild(subtitle);
    row.appendChild(textWrap);
    row.appendChild(input);
    filterList.appendChild(row);
  }

  landuseContextOptions(state.runtime).forEach(function (option) {
    appendFilterRow(option);
  });

  const opacityRow = document.createElement("label");
  opacityRow.className = "amenity-tier-row";
  opacityRow.htmlFor = "landuse-opacity";

  const opacityTextWrap = document.createElement("span");
  opacityTextWrap.className = "toggle-label";

  const opacityTitle = document.createElement("strong");
  opacityTitle.textContent = "Context opacity";

  const opacitySubtitle = document.createElement("span");
  opacitySubtitle.textContent = "0 to 1 (default 0.28)";

  const opacityInput = document.createElement("input");
  opacityInput.type = "range";
  opacityInput.min = "0";
  opacityInput.max = "1";
  opacityInput.step = "0.05";
  opacityInput.id = "landuse-opacity";
  opacityInput.value = String(state.landuseOpacity.toFixed(2));
  opacityInput.addEventListener("input", function () {
    const nextOpacity = Number(opacityInput.value);
    if (!Number.isFinite(nextOpacity)) return;
    state.landuseOpacity = Math.max(0, Math.min(1, nextOpacity));
    applyLanduseOpacity();
    filterMeta.textContent = landuseFilterSummary();
  });

  opacityTextWrap.appendChild(opacityTitle);
  opacityTextWrap.appendChild(opacitySubtitle);
  opacityRow.appendChild(opacityTextWrap);
  opacityRow.appendChild(opacityInput);
  filterList.appendChild(opacityRow);

  filterDetails.appendChild(filterList);
  elements.landuseControls.appendChild(filterDetails);

  overlayInput.addEventListener("change", function () {
    state.landuseVisible = overlayInput.checked;
    syncLanduseInputs();
    applyLanduseVisibility();
  });
  syncLanduseInputs();
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

async function fetchInspectPoint(lngLat, options = {}) {
  const cancelPrevious = options.cancelPrevious !== false;
  if (cancelPrevious && state.pendingInspect) {
    state.pendingInspect.abort();
  }
  const controller = new AbortController();
  if (cancelPrevious) state.pendingInspect = controller;
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
    const payload = await response.json();
    if (payload && payload.scoring_model === "daily-access-v4" && payload.scoring_model_version === undefined) {
      payload.scoring_model_version = state.runtime && state.runtime.daily_access
        ? state.runtime.daily_access.score_model_version
        : null;
    }
    return payload;
  } finally {
    if (cancelPrevious && state.pendingInspect === controller) {
      state.pendingInspect = null;
    }
  }
}

async function fetchInspect(lngLat) {
  return fetchInspectPoint(lngLat, { cancelPrevious: true });
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

  if (elements.shortlistCompare) {
    elements.shortlistCompare.addEventListener("click", function () {
      const items = Array.from(state.compareSelectedIds).map(function (id) {
        return state.shortlist.find(function (item) { return item.id === id; });
      }).filter(Boolean);
      openCompareDrawer(items, { ephemeral: false });
    });
    elements.shortlistExport.addEventListener("click", downloadShortlist);
    elements.shortlistClear.addEventListener("click", function () {
      if (!window.confirm("Clear all saved locations?")) return;
      state.shortlist = [];
      state.compareSelectedIds.clear();
      persistShortlist(); renderShortlist(); updateShortlistMarkers(); closeCompareDrawer();
    });
  }

  if (elements.compareClose) {
    elements.compareClose.addEventListener("click", closeCompareDrawer);
    elements.compareShare.addEventListener("click", async function () {
      try {
        await window.navigator.clipboard.writeText(currentMapPermalink(null));
        elements.compareShare.textContent = "Link copied";
        window.setTimeout(function () { elements.compareShare.textContent = "Copy link"; }, 1500);
      } catch (error) {
        elements.compareStatus.textContent = "Copy failed; URL is available in the address bar.";
      }
    });
    elements.compareSaveBoth.addEventListener("click", function () {
      let full = false;
      state.compareItems.forEach(function (item) {
        const result = upsertShortlistItem(state.shortlist, { ...item, id: newShortlistId() });
        state.shortlist = result.items;
        full = full || result.full;
      });
      state.compareEphemeral = false;
      elements.compareSaveBoth.hidden = true;
      persistShortlist(); renderShortlist(); updateShortlistMarkers();
      if (full) elements.compareStatus.textContent = "Shortlist is full; not every location was saved.";
    });
  }

  if (elements.useCurrentBuild) {
    elements.useCurrentBuild.addEventListener("click", function () {
      state.linkedBuildUnavailable = false;
      elements.linkedBuildWarning.hidden = true;
      elements.statusPill.style.display = "none";
      syncPermalink();
    });
  }

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

  document.addEventListener("keydown", function (event) {
    if (state.inspectOpen && event.key === "Escape") {
      closeInspectDrawer();
      return;
    }
    if (!state.compareOpen) return;
    if (event.key === "Escape") {
      closeCompareDrawer();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = Array.from(elements.compareDrawer.querySelectorAll("button:not([disabled]), [href], [tabindex]:not([tabindex='-1'])"));
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (document.activeElement === elements.compareDrawer) {
      event.preventDefault(); first.focus();
    } else if (event.shiftKey && document.activeElement === first) {
      event.preventDefault(); last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault(); first.focus();
    }
  });
}

function initializeMap() {
  const initialView = state.initialUrlState && state.initialUrlState.view;
  state.map = new maplibregl.Map({
    container: elements.map,
    style: buildRuntimeStyle(state.runtime, {
      windowOrigin: window.location.origin,
      selectedGridLayer: state.selectedGridLayer,
      selectedGridTravelMode: state.selectedGridTravelMode
    }),
    center: initialView
      ? [initialView.lon, initialView.lat]
      : [state.runtime.map_center.lon, state.runtime.map_center.lat],
    zoom: initialView ? initialView.zoom : (state.runtime.default_zoom || 6),
    minZoom: MIN_ZOOM,
    maxZoom: Number(state.runtime.max_zoom || 19),
    attributionControl: false,
    hash: false
  });

  state.map.addControl(
    new maplibregl.AttributionControl({ compact: true }),
    "bottom-left"
  );
  // MapLibre initially expands compact attribution as sources arrive. Keep the
  // workspace clear on first load; the standard info button still opens it.
  state.map.once("load", function () {
    const attribution = state.map.getContainer().querySelector(".maplibregl-ctrl-attrib");
    if (attribution) {
      attribution.removeAttribute("open");
      attribution.classList.remove("maplibregl-compact-show");
    }
  });

  state.popup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: true,
    className: "livability-popup",
    maxWidth: "320px"
  });
  state.popup.on("close", function () {
    state.permalinkInspect = null;
    syncPermalink();
  });

  state.map.on("load", function () {
    setResolutionChip();
    applyGridVisibility();
    applyDebugGridVisibility();
    applyAmenityFilter();
    applyLanduseVisibility();
    applyTransportRealityVisibility();
    applyServiceDesertVisibility();
    applyNoiseVisibility();
    installShortlistLayers();
    renderShortlist();
    refreshShortlistForBuild();
    renderBuildMetadata();
    syncPermalink();
    if (state.linkedBuildUnavailable) {
      updateStatus("Linked build is unavailable; showing the current build.");
      elements.statusPill.style.display = "";
    } else {
      updateStatus("");
      elements.statusPill.style.display = "none";
    }
    const initialCompare = state.initialUrlState && state.initialUrlState.compare;
    const initialInspect = state.initialUrlState && state.initialUrlState.inspect;
    if (initialCompare && inspectOnClick()) {
      openSharedComparison(initialCompare);
    } else if (initialInspect) {
      const lngLat = { lat: initialInspect.lat, lng: initialInspect.lon };
      state.map.easeTo({
        center: [lngLat.lng, lngLat.lat],
        zoom: Math.max(state.map.getZoom(), 16),
        duration: 900
      });
      highlightLinkedCell(lngLat);
      if (inspectOnClick()) fetchInspect(lngLat).then(function (payload) {
        showGridPopup(lngLat, inspectPopupHtml(payload, {
          selectedMode: state.selectedGridTravelMode,
          buildMetadata: popupBuildMetadata(),
          allowShortlist: true,
          shortlistSaved: savedCell(payload.cell_id)
        }), payload);
      }).catch(function (error) {
        if (!error || error.name !== "AbortError") {
          updateStatus((error && error.message) || "Linked cell could not be opened");
          elements.statusPill.style.display = "";
        }
      });
    }
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
    syncPermalink();
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
    clearLinkedCellHighlight();
    if (state.map.getLayer("shortlist-pins-circle")) {
      const pins = state.map.queryRenderedFeatures(event.point, { layers: ["shortlist-pins-circle"] });
      const id = pins[0] && String(pins[0].properties.id || "");
      const item = state.shortlist.find(function (entry) { return entry.id === id; });
      if (item) {
        openShortlistItem(item);
        return;
      }
    }
    const clickAction = resolveMapClickAction({
      map: state.map,
      point: event.point,
      fineSurfaceEnabled: inspectOnClick(),
      gridVisible: state.gridVisible,
      activeGridLayerId: activeGridLayerId()
    });
    if (clickAction.type === CLICK_ACTIONS.FINE_INSPECT || clickAction.type === CLICK_ACTIONS.COARSE_GRID) {
      highlightRenderedLinkedCell(event.lngLat, clickAction.features[0] || null);
    }
    if (state.inspectOpen && clickAction.type !== CLICK_ACTIONS.FINE_INSPECT && clickAction.type !== CLICK_ACTIONS.COARSE_GRID) {
      closeInspectDrawer({ restoreFocus: false });
    }

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
      const routeKeys = Array.from(new Set(popupRows.flatMap(function (row) {
        return parseRouteKeys(row.route_keys_json);
      })));
      if (routeKeys.length && state.runtime.transport_network_enabled) {
        focusRoutes(routeKeys).catch(function () {});
      }
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.TRANSPORT_HUB) {
      const properties = clickAction.features[0] && clickAction.features[0].properties || {};
      const keys = parseRouteKeys(properties.route_keys_json || properties.route_keys);
      if (keys.length) focusRoutes(keys);
      state.popup
        .setLngLat(event.lngLat)
        .setHTML('<div class="popup-content"><h3>' + escapeTransportText(properties.hub_name || "Transfer hub") + '</h3><p>' + keys.length + ' connecting routes</p></div>')
        .addTo(state.map);
      return;
    }

    if (clickAction.type === CLICK_ACTIONS.TRANSPORT_ROUTE) {
      const properties = clickAction.features[0] && clickAction.features[0].properties || {};
      const routeKey = String(properties.route_key || "");
      if (routeKey) {
        focusRoutes([routeKey], routeKey);
      }
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
        showGridPopup(event.lngLat, inspectPopupHtml(payload, {
          selectedMode: state.selectedGridTravelMode,
          buildMetadata: popupBuildMetadata(),
          allowShortlist: true,
          shortlistSaved: savedCell(payload.cell_id)
        }), payload);
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
      showGridPopup(event.lngLat, coarseGridPopupHtml(
          clickAction.features[0].properties || {},
          resolutionForZoom(state.map.getZoom()),
          {
            legacyUnavailable: runtimeLegacyFieldsUnavailable(state.runtime).length > 0,
            selectedMode: state.selectedGridTravelMode,
            buildMetadata: popupBuildMetadata()
          }
        ));
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
  ["transport-routes-line", "transport-routes-approximate", "transport-focus-line", "transport-focus-approximate", "transport-transfer-hubs", "transport-focus-transfers"].forEach(function (layerId) {
    if (!state.map.getLayer(layerId)) return;
    state.map.on("mouseenter", layerId, function () { state.map.getCanvas().style.cursor = "pointer"; });
    state.map.on("mouseleave", layerId, function () { state.map.getCanvas().style.cursor = ""; });
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
  state.initialUrlState = parseMapUrlState(window.location.href, runtime);
  state.linkedBuildUnavailable = linkedBuildUnavailable(state.initialUrlState, runtime);
  state.gridVisible = true;
  state.activeGridResolutionM = null;
  state.debugGridVisible = false;
  state.gridDebug = createGridDebugState(DEBUG_GRID_ENABLED);
  state.gridDebugSnapshotFallbackText = "";
  state.selectedGridLayer = state.initialUrlState.layer || readSavedGridLayer();
  state.selectedGridTravelMode = state.initialUrlState.mode || readSavedGridTravelMode();
  state.permalinkInspect = state.initialUrlState.inspect;
  state.shortlist = runtimeIsDailyAccessModel(runtime) ? loadShortlist(window.localStorage) : [];
  state.compareSelectedIds = new Set();
  state.compareItems = [];
  state.compareOpen = false;
  state.compareEphemeral = false;
  state.compareReturnFocus = null;
  state.compareLoadToken = 0;
  state.inspectOpen = false;
  state.inspectPayload = null;
  state.inspectReturnFocus = null;
  state.linkedCellMarker = null;
  state.linkedCellHighlightTimer = null;
  state.linkedCellGeometry = null;
  state.enabledAmenityCategories = new Set();
  state.enabledAmenityTiers = new Map(
    Object.entries(defaultAmenityTierSelections(runtime)).map(function (entry) {
      return [entry[0], new Set(entry[1])];
    })
  );
  state.landuseVisible = false;
  state.selectedLanduseClasses = new Set(defaultLanduseContextSelections(runtime));
  state.landuseOpacity = runtimeLanduseContextDefaultOpacity;
  state.selectedTransportSubtiers = new Set();
  state.selectedTransportBusFrequencies = new Set();
  state.selectedTransportModes = new Set(["bus", "rail", "tram"]);
  state.transportIncludeUnscheduled = false;
  state.transportRequireExceptionOnly = false;
  state.routeIndex = [];
  state.routeIndexLoaded = false;
  state.routeSearchQuery = "";
  state.focusedRouteKeys = [];
  state.selectedRouteKey = null;
  state.selectedRoutePatternId = null;
  state.routeFeatureCollection = emptyRouteFeatureCollection();
  state.routeBrowserElement = null;
  state.noiseVisible = false;
  state.noiseOpacity = runtimeDefaultNoiseOpacity;
  const noiseDefaults = defaultNoiseSelections(runtime);
  state.selectedNoiseMetric = noiseDefaults.metric;
  state.selectedNoiseSources = new Set(noiseDefaults.sources);
  if (state.selectedNoiseSources.has("industry") && state.selectedNoiseSources.size > 1) {
    state.selectedNoiseSources.delete("industry");
  }
  state.selectedNoiseBands = new Set(noiseDefaults.bands || []);
  ensureInspectDrawerElements();
  buildGridLayerControls();
  buildGridTravelModeControls();
  buildAmenityControls();
  buildLanduseControls();
  buildNoiseControls();
  buildTransitControls();
  maybeBuildDebugGridControl();
  wireUi();
  renderShortlist();
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

initializeWorkspace();
ensurePanelHeaderActions();

fetchRuntimePayload()
  .then(initializeApp)
  .catch(function (error) {
    const message = error && error.name === "AbortError"
      ? "Timed out loading runtime"
      : (error && error.message) || "Failed to load runtime";
    updateStatus(message);
    elements.gridToggle.disabled = true;
  });
