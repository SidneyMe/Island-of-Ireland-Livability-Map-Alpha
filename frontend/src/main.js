import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import "./main.css";
import {
  buildStyle as buildRuntimeStyle,
  fineSurfaceEnabled as runtimeFineSurfaceEnabled,
  resolutionForZoom as runtimeResolutionForZoom
} from "./runtime_contract.js";

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const MIN_ZOOM = 5;
const DEBUG_GRID_QUERY_PARAM = "debug-grid";
const SCORE_SECTIONS = ["shops", "transport", "healthcare", "parks"];

const elements = {
  statusPill: document.getElementById("status-pill"),
  controlPanel: document.getElementById("control-panel"),
  panelToggle: document.getElementById("panel-toggle"),
  zoomIn: document.getElementById("zoom-in"),
  zoomOut: document.getElementById("zoom-out"),
  resolutionChip: document.getElementById("resolution-chip"),
  amenityControls: document.getElementById("amenity-controls"),
  amenityNote: document.getElementById("amenity-note"),
  gridToggle: document.getElementById("grid-toggle"),
  map: document.getElementById("map"),
  mapStage: document.getElementById("map-stage")
};

const state = {
  runtime: null,
  map: null,
  popup: null,
  panelHidden: false,
  enabledAmenityCategories: new Set(),
  gridVisible: true,
  debugGridVisible: false
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

function formatResolutionLabel(resolutionM) {
  const resolution = Number(resolutionM);
  if (resolution >= 1000) {
    const km = resolution / 1000;
    return Number.isInteger(km) ? km + "km" : km.toFixed(1) + "km";
  }
  return resolution + "m";
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

function updatePanelVisibility() {
  document.body.classList.toggle("panel-hidden", state.panelHidden);
  elements.panelToggle.textContent = state.panelHidden ? "Show panel" : "Hide panel";
  elements.panelToggle.setAttribute("aria-expanded", state.panelHidden ? "false" : "true");
}

function applyGridVisibility() {
  if (!state.map) return;
  const visibility = state.gridVisible ? "visible" : "none";
  state.map.setLayoutProperty("grid-fill-coarse", "visibility", visibility);
  if (!fineSurfaceEnabled()) return;
  (state.runtime.fine_resolutions_m || []).forEach(function (resolutionM) {
    state.map.setLayoutProperty("surface-" + resolutionM, "visibility", visibility);
  });
}

function applyDebugGridVisibility() {
  if (!state.map) return;
  state.map.setLayoutProperty(
    "grid-fill-debug",
    "visibility",
    state.debugGridVisible ? "visible" : "none"
  );
}

function applyAmenityFilter() {
  if (!state.map) return;
  const enabled = Array.from(state.enabledAmenityCategories);
  const visibility = enabled.length ? "visible" : "none";
  state.map.setLayoutProperty("amenities-circle", "visibility", visibility);
  state.map.setFilter("amenities-circle", ["in", ["get", "category"], ["literal", enabled]]);
}

function buildAmenityControls() {
  elements.amenityControls.replaceChildren();
  const colors = state.runtime.category_colors || {};
  const counts = state.runtime.amenity_counts || {};
  Object.keys(colors)
    .sort()
    .forEach(function (category) {
      const label = document.createElement("label");
      label.className = "toggle-row";
      label.htmlFor = "amenity-" + category;

      const textWrap = document.createElement("span");
      textWrap.className = "toggle-label";

      const title = document.createElement("strong");
      title.textContent = category.charAt(0).toUpperCase() + category.slice(1);
      title.style.color = colors[category];

      const subtitle = document.createElement("span");
      subtitle.textContent = String(counts[category] || 0) + " mapped";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.id = "amenity-" + category;
      input.checked = false;
      input.addEventListener("change", function () {
        if (input.checked) {
          state.enabledAmenityCategories.add(category);
        } else {
          state.enabledAmenityCategories.delete(category);
        }
        updateAmenityNote();
        applyAmenityFilter();
      });

      textWrap.appendChild(title);
      textWrap.appendChild(subtitle);
      label.appendChild(textWrap);
      label.appendChild(input);
      elements.amenityControls.appendChild(label);
    });
  updateAmenityNote();
}

function maybeBuildDebugGridControl() {
  const url = new URL(window.location.href);
  if (url.searchParams.get(DEBUG_GRID_QUERY_PARAM) !== "1") return;
  const gridSection = elements.gridToggle.closest(".panel-section");
  if (!gridSection) return;

  const label = document.createElement("label");
  label.className = "toggle-row";
  label.htmlFor = "grid-debug-toggle";

  const text = document.createElement("span");
  text.textContent = "Show coarse vector debug grid";

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
  gridSection.appendChild(label);
}

function amenityPopupHtml(properties) {
  return (
    '<div class="popup-content">' +
      "<h3>" + escapeHtml(properties.category || "Amenity") + "</h3>" +
      "<p>" + escapeHtml(properties.source_ref || "OSM feature") + "</p>" +
    "</div>"
  );
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
    const score = Number((payload.component_scores || {})[category] || 0).toFixed(1);
    return "<li><strong>" + title + "</strong>: " + score + " points, " + count + " found</li>";
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
    const score = Number(properties["score_" + category] || 0).toFixed(1);
    return "<li><strong>" + title + "</strong>: " + score + " points, " + count + " found</li>";
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
  const params = new URLSearchParams({
    lat: String(lngLat.lat),
    lon: String(lngLat.lng),
    zoom: String(state.map.getZoom())
  });
  const response = await fetch(String(state.runtime.inspect_url) + "?" + params.toString());
  if (!response.ok) {
    const payload = await response.json().catch(function () {
      return {};
    });
    throw new Error(payload.error || response.statusText);
  }
  return response.json();
}

function wireUi() {
  elements.panelToggle.addEventListener("click", function () {
    state.panelHidden = !state.panelHidden;
    updatePanelVisibility();
    if (state.map) {
      window.setTimeout(function () {
        state.map.resize();
      }, 190);
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
    updateStatus("");
    elements.statusPill.style.display = "none";
  });

  state.map.on("zoom", setResolutionChip);
  state.map.on("zoomend", setResolutionChip);

  state.map.on("click", async function (event) {
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
        updateStatus(error.message || "Inspect failed");
        elements.statusPill.style.display = "";
      }
      return;
    }

    const gridFeatures = state.map.queryRenderedFeatures(event.point, { layers: ["grid-fill-coarse"] });
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

  state.map.on("error", function (event) {
    const message = (event && event.error && event.error.message) || "Map error";
    updateStatus(message);
    elements.statusPill.style.display = "";
  });
}

function initializeApp(runtime) {
  state.runtime = runtime;
  state.gridVisible = true;
  buildAmenityControls();
  maybeBuildDebugGridControl();
  wireUi();
  updatePanelVisibility();
  initializeMap();
}

fetch("/api/runtime")
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
    updateStatus(error.message || "Failed to load runtime");
    elements.gridToggle.disabled = true;
  });
