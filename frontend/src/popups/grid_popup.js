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

const NEED_LABELS = {
  food: "Food shops",
  other_retail: "Other retail",
  primary_care: "Primary care",
  pharmacy: "Pharmacy",
  green: "Parks and green space",
  transport: "Public transport"
};
const NETWORK_STATUS_LABELS = {
  ok: "walking network reached",
  partial: "partly reached",
  no_walking_network: "no walking network within 250 m",
  outside_mask: "water"
};

function isMissing(value) {
  return value === null || value === undefined || !Number.isFinite(Number(value));
}

function pointsText(value) {
  return isMissing(value) ? "n/a" : Number(value).toFixed(1) + " points";
}

function dailyAccessSectionHtml(values) {
  const components = values.component_scores || {};
  const penalties = values.penalties || {};
  const access = values.service_access || {};
  const componentItems = ["shops", "transport", "healthcare", "parks"].map(function (category) {
    return "<li><strong>" + scoreLabel(category) + "</strong>: " + pointsText(components[category]) + "</li>";
  }).join("");
  const penaltyItems = ["railway_proximity", "road_proximity"].map(function (penalty) {
    const value = penalties[penalty];
    return "<li><strong>" + scoreLabel(penalty) + "</strong>: " +
      pointsText(isMissing(value) ? null : -Math.abs(Number(value))) + "</li>";
  }).join("");
  const accessItems = Object.keys(NEED_LABELS).map(function (need) {
    const value = access[need];
    return "<li>" + NEED_LABELS[need] + ": " + (isMissing(value) ? "n/a" : Math.round(Number(value) * 100) + "%") + "</li>";
  }).join("");
  return "<ul>" + componentItems + penaltyItems + "</ul><p>Access</p><ul>" + accessItems + "</ul>";
}

// Daily access v4: values come from the score shards; there are no raw
// counts, clusters or effective units to show (and no zeros in their place).
function dailyAccessInspectHtml(payload) {
  const status = String(payload.network_status || "outside_mask");
  const visible = payload.visible;
  let visibleHtml = "";
  if (visible && typeof visible === "object") {
    const coverage = Number(visible.network_coverage_share);
    visibleHtml =
      '<div class="popup-score-variant">' +
        "<h4>Visible " + escapeHtml(formatResolutionLabel(visible.resolution_m)) + " cell: " +
          (isMissing(visible.total_score) ? "n/a" : Number(visible.total_score).toFixed(1) + " / 100") +
        "</h4>" +
        "<p>Network: " + escapeHtml(NETWORK_STATUS_LABELS[visible.network_status] || visible.network_status) +
          (Number.isFinite(coverage) ? " (" + Math.round(coverage * 100) + "% of land)" : "") + "</p>" +
        (isMissing(visible.total_score) ? "" : dailyAccessSectionHtml(visible)) +
      "</div>";
  }
  if (!payload.valid_land) {
    return (
      '<div class="popup-content">' +
        "<h3>No land cell</h3>" +
        "<p>The 50m daily access surface is transparent at this location.</p>" +
        visibleHtml +
      "</div>"
    );
  }
  return (
    '<div class="popup-content">' +
      "<h3>Daily access score " + Number(payload.total_score || 0).toFixed(1) + " / 100</h3>" +
      "<p>Exact surface: " + escapeHtml(formatResolutionLabel(payload.resolution_m || 50)) + "</p>" +
      "<p>Network: " + escapeHtml(NETWORK_STATUS_LABELS[status] || status) + "</p>" +
      dailyAccessSectionHtml(payload) +
      visibleHtml +
      "<p>Model: " + escapeHtml(payload.scoring_model) + "</p>" +
    "</div>"
  );
}

export function inspectPopupHtml(payload) {
  if (payload && payload.scoring_model === "daily-access-v4") {
    return dailyAccessInspectHtml(payload);
  }
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

export function coarseGridPopupHtml(properties, fallbackResolutionM = 50, options = {}) {
  const legacyUnavailable = Boolean(options && options.legacyUnavailable);
  const listHtml = SCORE_SECTIONS.map(function (category) {
    const title = scoreLabel(category);
    const count = Number(properties["count_" + category] || 0);
    const clusterCount = Number(properties["cluster_" + category] || 0);
    const effectiveUnits = formatEffectiveUnits(properties["effective_units_" + category] || 0);
    const score = Number(properties["score_" + category] || 0).toFixed(1);
    if (category === "railway_proximity" || category === "road_proximity" || legacyUnavailable) {
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
      "<h3>" + (legacyUnavailable ? "Daily access score " : "Walk score ") + Number(properties.total_score || 0).toFixed(1) + " / 100</h3>" +
      "<p>Visible grid: " + escapeHtml(formatResolutionLabel(properties.resolution_m || fallbackResolutionM)) + "</p>" +
      "<ul>" + listHtml + "</ul>" +
      (legacyUnavailable ? "<p>Raw counts, clusters and effective units are not part of this model.</p>" : "") +
      modeAwareHtml(properties) +
    "</div>"
  );
}
