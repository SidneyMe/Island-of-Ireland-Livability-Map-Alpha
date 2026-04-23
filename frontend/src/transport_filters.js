const TRANSPORT_SUBTIER_ORDER = [
  "mon_sun",
  "mon_sat",
  "tue_sun",
  "weekdays_only",
  "weekends_only",
  "single_day_only",
  "partial_week"
];

const TRANSPORT_SUBTIER_LABELS = {
  mon_sun: "Whole week",
  mon_sat: "Mon-Sat",
  tue_sun: "Tue-Sun",
  weekdays_only: "Weekdays only",
  weekends_only: "Weekends only",
  single_day_only: "Single-day only",
  partial_week: "Partial week",
  unscheduled: "Unscheduled"
};

const TRANSPORT_MODE_ORDER = ["tram", "rail"];

const TRANSPORT_MODE_LABELS = {
  tram: "Tram",
  rail: "Rail"
};

function _normalizedCounts(rawCounts) {
  const normalized = {};
  if (!rawCounts || typeof rawCounts !== "object" || Array.isArray(rawCounts)) {
    return normalized;
  }
  Object.keys(rawCounts).forEach(function (key) {
    const count = Number(rawCounts[key]);
    if (!key || !Number.isFinite(count) || count < 0) return;
    normalized[key] = count;
  });
  return normalized;
}

function transportSubtierLabel(value) {
  return TRANSPORT_SUBTIER_LABELS[String(value || "").trim()] || "No recent public transport tier";
}

function transportTierOptions(runtime) {
  const counts = _normalizedCounts(runtime && runtime.transport_subtier_counts);
  return TRANSPORT_SUBTIER_ORDER.map(function (subtier) {
    return {
      value: subtier,
      label: transportSubtierLabel(subtier),
      count: Number(counts[subtier] || 0)
    };
  });
}

function transportModeLabel(value) {
  return TRANSPORT_MODE_LABELS[String(value || "").trim()] || String(value || "").trim();
}

function transportModeOptions(runtime) {
  const counts = _normalizedCounts(runtime && runtime.transport_mode_counts);
  return TRANSPORT_MODE_ORDER.map(function (mode) {
    return {
      value: mode,
      label: transportModeLabel(mode),
      count: Number(counts[mode] || 0)
    };
  });
}

function transportFlagCounts(runtime) {
  return _normalizedCounts(runtime && runtime.transport_flag_counts);
}

function routeModeTokenFilter(mode) {
  const token = String(mode || "").trim();
  return [
    "in",
    "," + token + ",",
    ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]
  ];
}

function buildTransportLayerFilter(options = {}) {
  const selectedSubtiers = Array.from(options.selectedSubtiers || []);
  const selectedModes = Array.from(options.selectedModes || []);
  const includeUnscheduled = Boolean(options.includeUnscheduled);
  const requireExceptionOnly = Boolean(options.requireExceptionOnly);
  const clauses = [];
  const selectedClauses = [];

  if (selectedSubtiers.length > 0) {
    selectedClauses.push([
      "in",
      ["coalesce", ["get", "bus_service_subtier"], ""],
      ["literal", selectedSubtiers]
    ]);
  }
  selectedModes.forEach(function (mode) {
    if (!String(mode || "").trim()) return;
    selectedClauses.push(routeModeTokenFilter(mode));
  });
  if (includeUnscheduled) {
    selectedClauses.push(["==", ["get", "is_unscheduled_stop"], 1]);
  }
  if (selectedClauses.length === 1) {
    clauses.push(selectedClauses[0]);
  } else if (selectedClauses.length > 1) {
    clauses.push(["any", ...selectedClauses]);
  }
  if (requireExceptionOnly) {
    clauses.push(["==", ["get", "has_exception_only_service"], 1]);
  }
  if (!clauses.length) {
    return null;
  }
  if (clauses.length === 1) {
    return clauses[0];
  }
  return ["all", ...clauses];
}

export {
  TRANSPORT_MODE_LABELS,
  TRANSPORT_MODE_ORDER,
  TRANSPORT_SUBTIER_LABELS,
  TRANSPORT_SUBTIER_ORDER,
  buildTransportLayerFilter,
  routeModeTokenFilter,
  transportFlagCounts,
  transportModeLabel,
  transportModeOptions,
  transportSubtierLabel,
  transportTierOptions
};
