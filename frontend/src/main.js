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
  buildStyle as buildRuntimeStyle,
  fineSurfaceEnabled as runtimeFineSurfaceEnabled,
  resolutionForZoom as runtimeResolutionForZoom
} from "./runtime_contract.js";
import { transportRealityPopupHtml } from "./transport_reality_popup.js";

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const MIN_ZOOM = 5;
const DEBUG_GRID_QUERY_PARAM = "debug-grid";
const SCORE_SECTIONS = ["shops", "transport", "healthcare", "parks"];
const RUNTIME_FETCH_TIMEOUT_MS = 10000;
const INSPECT_FETCH_TIMEOUT_MS = 15000;

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
  mapStage: document.getElementById("map-stage")
};

const state = {
  runtime: null,
  map: null,
  popup: null,
  panelHidden: false,
  enabledAmenityCategories: new Set(),
  enabledAmenityTiers: new Map(),
  gridVisible: true,
  debugGridVisible: false,
  transportRealityVisible: false,
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

function applyTransportRealityVisibility() {
  if (!state.map) return;
  state.map.setLayoutProperty(
    "transport-reality-circle",
    "visibility",
    state.transportRealityVisible ? "visible" : "none"
  );
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
  const controls = [];
  if (state.runtime.transport_reality_enabled) {
    controls.push({
      id: "transport-reality-toggle",
      label: "Show active vs inactive stops",
      checked: false,
      onChange: function (checked) {
        state.transportRealityVisible = checked;
        applyTransportRealityVisibility();
      }
    });
  }
  if (state.runtime.service_deserts_enabled) {
    controls.push({
      id: "service-deserts-toggle",
      label: "Show service desert overlay",
      checked: false,
      onChange: function (checked) {
        state.serviceDesertsVisible = checked;
        applyServiceDesertVisibility();
      }
    });
  }

  controls.forEach(function (control) {
    const label = document.createElement("label");
    label.className = "toggle-row";
    label.htmlFor = control.id;

    const text = document.createElement("span");
    text.textContent = control.label;

    const input = document.createElement("input");
    input.type = "checkbox";
    input.id = control.id;
    input.checked = control.checked;
    input.addEventListener("change", function () {
      control.onChange(input.checked);
    });

    label.appendChild(text);
    label.appendChild(input);
    elements.transitControls.appendChild(label);
  });

  if (elements.transportRealityDownload) {
    const href = state.runtime.transport_reality_download_url || "/exports/transport-reality.zip";
    elements.transportRealityDownload.href = href;
    elements.transportRealityDownload.style.display = state.runtime.transport_reality_enabled ? "" : "none";
  }

  if (elements.transitNote) {
    const analysisDate = state.runtime.transit_analysis_date || "unknown date";
    elements.transitNote.textContent = "As of " + analysisDate;
  }
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

  state.map.on("zoom", setResolutionChip);
  state.map.on("zoomend", setResolutionChip);

  state.map.on("click", async function (event) {
    const transportRealityFeatures = state.map.queryRenderedFeatures(event.point, { layers: ["transport-reality-circle"] });
    if (transportRealityFeatures.length > 0) {
      state.popup
        .setLngLat(event.lngLat)
        .setHTML(transportRealityPopupHtml(transportRealityFeatures[0].properties || {}))
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
  });
}

function initializeApp(runtime) {
  state.runtime = runtime;
  state.gridVisible = true;
  state.enabledAmenityCategories = new Set();
  state.enabledAmenityTiers = new Map(
    Object.entries(defaultAmenityTierSelections(runtime)).map(function (entry) {
      return [entry[0], new Set(entry[1])];
    })
  );
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
