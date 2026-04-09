import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Protocol } from "pmtiles";
import "./main.css";

// Wire the pmtiles:// protocol into MapLibre once at module load.
const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const BASEMAP_RASTER = "https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png";
const MIN_ZOOM = 5;
const MAX_ZOOM = 14;

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
  enabledAmenityCategories: new Set()
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

function setResolutionChip() {
  if (!state.map) {
    elements.resolutionChip.textContent = "Grid --";
    return;
  }
  const zoom = state.map.getZoom();
  let resolution = "20km";
  if (zoom >= 11) resolution = "5km";
  else if (zoom >= 8) resolution = "10km";
  elements.resolutionChip.textContent = "Grid " + resolution;
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

function gridFillColorExpression() {
  // GPU-side data-driven choropleth on total_score (0..100, viridis-ish).
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

function buildStyle(runtime) {
  const colors = runtime.category_colors || {};
  const pmtilesUrl = "pmtiles://" + window.location.origin + (runtime.pmtiles_url || "/tiles/livability.pmtiles");
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
      }
    },
    layers: [
      { id: "basemap", type: "raster", source: "basemap" },
      {
        id: "grid-fill",
        type: "fill",
        source: "livability",
        "source-layer": "grid",
        paint: {
          "fill-color": gridFillColorExpression(),
          "fill-opacity": 0.55,
          "fill-outline-color": "rgba(0,0,0,0.15)"
        }
      },
      {
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
            14, 5
          ],
          "circle-color": amenityCircleColorExpression(colors),
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 1
        }
      }
    ]
  };
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

function gridPopupHtml(properties) {
  const score = Number(properties.total_score || 0).toFixed(1);
  const sections = [
    ["Shops", properties.count_shops || 0, properties.score_shops || 0],
    ["Transport", properties.count_transport || 0, properties.score_transport || 0],
    ["Healthcare", properties.count_healthcare || 0, properties.score_healthcare || 0],
    ["Parks", properties.count_parks || 0, properties.score_parks || 0]
  ];
  const listHtml = sections
    .map(function (section) {
      return (
        "<li><strong>" + section[0] + "</strong>: " +
        Number(section[2]).toFixed(1) + " points, " + section[1] + " found</li>"
      );
    })
    .join("");
  return (
    '<div class="popup-content">' +
      "<h3>Walk score " + score + " / 100</h3>" +
      "<p>Cell " + escapeHtml(properties.cell_id || "unknown") + "</p>" +
      "<p>Resolution: " + escapeHtml(String(properties.resolution_m || "?")) + "m</p>" +
      "<ul>" + listHtml + "</ul>" +
    "</div>"
  );
}

function amenityPopupHtml(properties) {
  return (
    '<div class="popup-content">' +
      "<h3>" + escapeHtml(properties.category || "Amenity") + "</h3>" +
      "<p>" + escapeHtml(properties.source_ref || "OSM feature") + "</p>" +
    "</div>"
  );
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
    if (!state.map) return;
    const visibility = elements.gridToggle.checked ? "visible" : "none";
    state.map.setLayoutProperty("grid-fill", "visibility", visibility);
  });

  window.addEventListener("resize", function () {
    if (state.map) state.map.resize();
  });
}

function initializeMap() {
  state.map = new maplibregl.Map({
    container: elements.map,
    style: buildStyle(state.runtime),
    center: [state.runtime.map_center.lon, state.runtime.map_center.lat],
    zoom: state.runtime.default_zoom || 6,
    minZoom: MIN_ZOOM,
    maxZoom: MAX_ZOOM,
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
    updateStatus("");
    elements.statusPill.style.display = "none";
  });

  state.map.on("zoom", setResolutionChip);
  state.map.on("zoomend", setResolutionChip);

  state.map.on("click", "grid-fill", function (event) {
    const feature = event.features && event.features[0];
    if (!feature) return;
    state.popup
      .setLngLat(event.lngLat)
      .setHTML(gridPopupHtml(feature.properties || {}))
      .addTo(state.map);
  });

  state.map.on("click", "amenities-circle", function (event) {
    const feature = event.features && event.features[0];
    if (!feature) return;
    state.popup
      .setLngLat(event.lngLat)
      .setHTML(amenityPopupHtml(feature.properties || {}))
      .addTo(state.map);
  });

  state.map.on("mouseenter", "grid-fill", function () {
    state.map.getCanvas().style.cursor = "pointer";
  });
  state.map.on("mouseleave", "grid-fill", function () {
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
  buildAmenityControls();
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
