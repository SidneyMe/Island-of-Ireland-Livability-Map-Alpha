import { formatResolutionLabel } from "../grid_debug.js";

const SCORE_SECTIONS = ["shops", "transport", "healthcare", "parks", "railway_proximity", "road_proximity"];
const SCORE_LABELS = {
  railway_proximity: "Railway proximity",
  road_proximity: "Road proximity"
};

function scoreLabel(category) {
  return SCORE_LABELS[category] || (category.charAt(0).toUpperCase() + category.slice(1));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function modeAwareHtml(payload) {
  const modeAware = payload && payload.mode_aware;
  if (!modeAware || typeof modeAware !== "object") {
    return "";
  }
  const total = Number(modeAware.total_score);
  const version = modeAware.scoring_model_version || "unknown";
  const weights = modeAware.weights || {};
  const availability = modeAware.mode_available || {};
  const weightText = ["walk", "bike", "transit"].map(function (mode) {
    return mode + " " + Math.round(Number(weights[mode] || 0) * 100) + "%";
  }).join(", ");
  const availableText = ["walk", "bike", "transit"].filter(function (mode) {
    return Boolean(availability[mode]);
  }).join(", ") || "walk";
  return (
    '<div class="popup-score-variant">' +
      "<h4>Mode-aware score " + (Number.isFinite(total) ? total.toFixed(1) : "n/a") + " / 100</h4>" +
      "<p>Model: " + escapeHtml(version) + "</p>" +
      "<p>Weights: " + escapeHtml(weightText) + "</p>" +
      "<p>Available modes: " + escapeHtml(availableText) + "</p>" +
    "</div>"
  );
}

export function formatEffectiveUnits(value) {
  const numeric = Number(value || 0);
  return numeric.toFixed(2).replace(/\.?0+$/, "");
}

export function inspectPopupHtml(payload) {
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
    const title = scoreLabel(category);
    const count = Number((payload.counts || {})[category] || 0);
    const clusterCount = Number((payload.cluster_counts || {})[category] || 0);
    const effectiveUnits = formatEffectiveUnits((payload.effective_units || {})[category] || 0);
    const score = Number((payload.component_scores || {})[category] || 0).toFixed(1);
    if (category === "railway_proximity" || category === "road_proximity") {
      return (
        "<li><strong>" + title + "</strong>: " +
        score + " points</li>"
      );
    }
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
      modeAwareHtml(payload) +
    "</div>"
  );
}

export function coarseGridPopupHtml(properties, fallbackResolutionM = 50) {
  const listHtml = SCORE_SECTIONS.map(function (category) {
    const title = scoreLabel(category);
    const count = Number(properties["count_" + category] || 0);
    const clusterCount = Number(properties["cluster_" + category] || 0);
    const effectiveUnits = formatEffectiveUnits(properties["effective_units_" + category] || 0);
    const score = Number(properties["score_" + category] || 0).toFixed(1);
    if (category === "railway_proximity" || category === "road_proximity") {
      return (
        "<li><strong>" + title + "</strong>: " +
        score + " points</li>"
      );
    }
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
      "<p>Visible grid: " + escapeHtml(formatResolutionLabel(properties.resolution_m || fallbackResolutionM)) + "</p>" +
      "<ul>" + listHtml + "</ul>" +
      modeAwareHtml(properties) +
    "</div>"
  );
}
