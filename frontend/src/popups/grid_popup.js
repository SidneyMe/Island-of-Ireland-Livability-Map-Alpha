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

function buildMetadataHtml(options = {}) {
  const metadata = options.buildMetadata;
  const permalink = options.permalink !== false;
  const details = metadata && (metadata.model || metadata.buildKey)
    ? "<p class=\"popup-muted\">Model: " + escapeHtml(metadata.model || "unknown") +
      " · build " + escapeHtml(String(metadata.buildKey || "unknown").slice(0, 10)) + "</p>"
    : "";
  return details + (permalink
    ? '<button class="popup-permalink" type="button">Copy link to this cell</button>'
    : "");
}

function shortlistButtonHtml(options = {}) {
  if (!options.allowShortlist) return "";
  return '<button class="popup-shortlist" type="button"' +
    (options.shortlistSaved ? " disabled" : "") + ">" +
    (options.shortlistSaved ? "Saved" : "Save to shortlist") + "</button>";
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
  const contextLabels = { industrial_context: "Industrial context", over_concentration: "Venue concentration", pitch_noise: "Pitch noise", rail_station_access: "Rail / tram station access", green_buffer: "Residential green buffer" };
  const penaltyItems = ["railway_proximity", "road_proximity", "industrial_context", "over_concentration", "pitch_noise"].filter(function (name) {
    return name in penalties;
  }).map(function (penalty) {
    const value = penalties[penalty];
    return "<li><strong>" + (contextLabels[penalty] || scoreLabel(penalty)) + "</strong>: " +
      pointsText(isMissing(value) ? null : -Math.abs(Number(value))) + "</li>";
  }).join("");
  const accessItems = Object.keys(NEED_LABELS).map(function (need) {
    const value = access[need];
    return "<li>" + NEED_LABELS[need] + ": " + (isMissing(value) ? "n/a" : Math.round(Number(value) * 100) + "%") + "</li>";
  }).join("");
  const bonusItems = Object.entries(values.bonuses || {}).map(function ([name, value]) {
    return "<li><strong>" + escapeHtml(contextLabels[name] || name) + "</strong>: +" + pointsText(value) + "</li>";
  }).join("");
  const context = values.context;
  const contextHtml = context ? '<p class="score-context">Land-use coverage: ' + Math.round(context.landuse_coverage * 100) + "% · Residential: " + Math.round(context.residential_share * 100) + "% · Industrial: " + Math.round(context.industrial_share * 100) + "%</p>" : "";
  return '<div class="score-breakdown"><ul class="score-metrics">' + componentItems + penaltyItems + bonusItems +
    '</ul>' + contextHtml + '<h5>Access</h5><ul class="score-access">' + accessItems + "</ul></div>";
}

// Daily access v4: values come from the score shards; there are no raw
// counts, clusters or effective units to show (and no zeros in their place).
const MODE_LABELS = { walk: "Walk", bike: "Bike", mode_aware: "Mode-aware" };

function selectedModeValues(payload, selectedMode) {
  const modes = payload && payload.modes;
  return modes && modes[selectedMode] ? modes[selectedMode] : payload;
}

function modeComparisonHtml(payload) {
  const modes = payload && payload.modes;
  if (!modes || typeof modes !== "object") return "";
  const items = ["walk", "bike", "mode_aware"].filter(function (mode) {
    return modes[mode] && !isMissing(modes[mode].total_score);
  }).map(function (mode) {
    return "<li><strong>" + MODE_LABELS[mode] + "</strong>: " +
      Number(modes[mode].total_score).toFixed(1) + " / 100</li>";
  }).join("");
  return items ? '<div class="popup-score-variant"><h4>Travel modes</h4><ul class="score-mode-list">' + items + "</ul></div>" : "";
}

function dailyAccessInspectHtml(payload, options = {}) {
  const selectedMode = String(options.selectedMode || "walk");
  const selected = selectedModeValues(payload, selectedMode);
  const status = String(selected.network_status || "outside_mask");
  const visible = payload.visible;
  const selectedVisible = visible ? selectedModeValues(visible, selectedMode) : null;
  let visibleHtml = "";
  if (selectedVisible && typeof selectedVisible === "object") {
    const coverage = Number(selectedVisible.network_coverage_share);
    visibleHtml =
      '<div class="popup-score-variant">' +
        "<h4>Visible " + escapeHtml(formatResolutionLabel(visible.resolution_m)) + " cell: " +
          (isMissing(selectedVisible.total_score) ? "n/a" : Number(selectedVisible.total_score).toFixed(1) + " / 100") +
        "</h4>" +
        "<p>Network: " + escapeHtml(NETWORK_STATUS_LABELS[selectedVisible.network_status] || selectedVisible.network_status) +
          (Number.isFinite(coverage) ? " (" + Math.round(coverage * 100) + "% of land)" : "") + "</p>" +
        (isMissing(selectedVisible.total_score) ? "" : dailyAccessSectionHtml(selectedVisible)) +
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
      "<h3>" + escapeHtml(MODE_LABELS[selectedMode] || "Walk") + " score " +
        Number(selected.total_score || 0).toFixed(1) + " / 100</h3>" +
      "<p>Exact surface: " + escapeHtml(formatResolutionLabel(payload.resolution_m || 50)) + "</p>" +
      "<p>Network: " + escapeHtml(NETWORK_STATUS_LABELS[status] || status) + "</p>" +
      dailyAccessSectionHtml(selected) +
      modeComparisonHtml(payload) +
      visibleHtml +
      "<p>Model: " + escapeHtml(payload.scoring_model) + "</p>" +
      shortlistButtonHtml(options) +
      buildMetadataHtml(options) +
    "</div>"
  );
}

export function inspectPopupHtml(payload, options = {}) {
  if (payload && payload.scoring_model === "daily-access-v4") {
    return dailyAccessInspectHtml(payload, options);
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
      buildMetadataHtml(options) +
    "</div>"
  );
}

export function coarseGridPopupHtml(properties, fallbackResolutionM = 50, options = {}) {
  const legacyUnavailable = Boolean(options && options.legacyUnavailable);
  const selectedMode = String(options && options.selectedMode || "walk");
  const modePrefix = selectedMode === "walk" ? "" : selectedMode + "_";
  const selectedTotal = properties[modePrefix + "total_score"];
  const extraLabels = {industrial_context: "Industrial context", over_concentration: "Venue concentration", pitch_noise: "Pitch noise", rail_station_access: "Rail / tram station access", green_buffer: "Residential green buffer"};
  const contextItems = legacyUnavailable ? Object.entries(extraLabels).filter(function ([name]) {
    return !isMissing(properties[modePrefix + "score_" + name]);
  }).map(function ([name, label]) {
    return "<li><strong>" + label + "</strong>: " + pointsText(properties[modePrefix + "score_" + name]) + "</li>";
  }).join("") : "";
  const listHtml = SCORE_SECTIONS.map(function (category) {
    const title = scoreLabel(category);
    const count = Number(properties["count_" + category] || 0);
    const clusterCount = Number(properties["cluster_" + category] || 0);
    const effectiveUnits = formatEffectiveUnits(properties["effective_units_" + category] || 0);
    const score = Number(properties[modePrefix + "score_" + category] || 0).toFixed(1);
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
      "<h3>" + (legacyUnavailable ? escapeHtml(MODE_LABELS[selectedMode] || "Walk") + " score " : "Walk score ") +
        Number(isMissing(selectedTotal) ? (properties.total_score || 0) : selectedTotal).toFixed(1) + " / 100</h3>" +
      "<p>Visible grid: " + escapeHtml(formatResolutionLabel(properties.resolution_m || fallbackResolutionM)) + "</p>" +
      "<ul>" + listHtml + contextItems + "</ul>" +
      (legacyUnavailable ? "<p>Raw counts, clusters and effective units are not part of this model.</p>" : "") +
      modeAwareHtml(properties) +
      buildMetadataHtml(options) +
    "</div>"
  );
}
