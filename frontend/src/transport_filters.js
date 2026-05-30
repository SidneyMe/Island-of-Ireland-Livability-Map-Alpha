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

const TRANSPORT_MODE_ORDER = ["bus", "tram", "rail"];

const TRANSPORT_MODE_LABELS = {
  bus: "Bus",
  tram: "Tram",
  rail: "Rail"
};

const TRANSPORT_BUS_FREQUENCY_ORDER = [
  "frequent",
  "moderate",
  "low_frequency",
  "very_low_frequency",
  "token_skeletal"
];

const TRANSPORT_BUS_FREQUENCY_LABELS = {
  frequent: "Frequent (<=15 min)",
  moderate: "Moderate (16-30 min)",
  low_frequency: "Low frequency (31-60 min)",
  very_low_frequency: "Very low frequency (61-120 min)",
  token_skeletal: "Token / skeletal (>120 min)"
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
  const flagCounts = _normalizedCounts(runtime && runtime.transport_flag_counts);
  return TRANSPORT_MODE_ORDER.map(function (mode) {
    const count = mode === "bus"
      ? Number(flagCounts.has_any_bus_service || 0)
      : Number(counts[mode] || 0);
    return {
      value: mode,
      label: transportModeLabel(mode),
      count: count
    };
  });
}

function transportBusFrequencyLabel(value) {
  return TRANSPORT_BUS_FREQUENCY_LABELS[String(value || "").trim()] || "";
}

function transportBusFrequencyOptions(runtime) {
  const counts = _normalizedCounts(runtime && runtime.transport_bus_frequency_counts);
  return TRANSPORT_BUS_FREQUENCY_ORDER.map(function (tier) {
    return {
      value: tier,
      label: transportBusFrequencyLabel(tier),
      count: Number(counts[tier] || 0)
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
  const selectedBusFrequencies = Array.from(options.selectedBusFrequencies || []);
  const selectedModes = Array.from(options.selectedModes || []);
  const includeUnscheduled = Boolean(options.includeUnscheduled);
  const requireExceptionOnly = Boolean(options.requireExceptionOnly);
  const selectedModeSet = new Set(
    selectedModes.map(function (value) { return String(value || "").trim(); }).filter(Boolean)
  );
  const allKnownModesSelected = (
    selectedModeSet.size === TRANSPORT_MODE_ORDER.length &&
    TRANSPORT_MODE_ORDER.every(function (mode) {
      return selectedModeSet.has(mode);
    })
  );
  const busConstraintClauses = [];
  const modeBranches = [];
  function anyEquals(getExpr, selectedValues) {
    const values = Array.from(selectedValues || [])
      .map(function (value) { return String(value || "").trim(); })
      .filter(Boolean);
    if (!values.length) return null;
    const equalityClauses = values.map(function (value) {
      return ["==", getExpr, value];
    });
    return equalityClauses.length === 1 ? equalityClauses[0] : ["any", ...equalityClauses];
  }

  const subtierClause = anyEquals(
    ["coalesce", ["get", "bus_service_subtier"], ""],
    selectedSubtiers
  );
  if (subtierClause) {
    busConstraintClauses.push(subtierClause);
  }
  const frequencyClause = anyEquals(
    ["coalesce", ["get", "bus_frequency_tier"], ""],
    selectedBusFrequencies
  );
  if (frequencyClause) {
    busConstraintClauses.push(frequencyClause);
  }

  const busFiltersRequested = busConstraintClauses.length > 0 || includeUnscheduled || requireExceptionOnly;
  const busSelected = selectedModeSet.has("bus");
  const railSelected = selectedModeSet.has("rail");
  const tramSelected = selectedModeSet.has("tram");

  if (allKnownModesSelected && !busFiltersRequested) {
    return null;
  }

  if (busSelected || (!selectedModeSet.size && busFiltersRequested)) {
    const busBranch = [["==", ["get", "has_any_bus_service"], 1], ...busConstraintClauses];
    if (requireExceptionOnly) {
      busBranch.push(["==", ["get", "has_exception_only_service"], 1]);
    }
    const busBranches = [busBranch.length === 1 ? busBranch[0] : ["all", ...busBranch]];
    if (includeUnscheduled) {
      busBranches.push(["==", ["get", "is_unscheduled_stop"], 1]);
    }
    modeBranches.push(busBranches.length === 1 ? busBranches[0] : ["any", ...busBranches]);
  }
  if (railSelected) {
    modeBranches.push(routeModeTokenFilter("rail"));
  }
  if (tramSelected) {
    modeBranches.push(routeModeTokenFilter("tram"));
  }

  if (!modeBranches.length) {
    return null;
  }
  if (modeBranches.length === 1) {
    return modeBranches[0];
  }
  return ["any", ...modeBranches];
}

export {
  TRANSPORT_BUS_FREQUENCY_LABELS,
  TRANSPORT_BUS_FREQUENCY_ORDER,
  TRANSPORT_MODE_LABELS,
  TRANSPORT_MODE_ORDER,
  TRANSPORT_SUBTIER_LABELS,
  TRANSPORT_SUBTIER_ORDER,
  buildTransportLayerFilter,
  routeModeTokenFilter,
  transportBusFrequencyLabel,
  transportBusFrequencyOptions,
  transportFlagCounts,
  transportModeLabel,
  transportModeOptions,
  transportSubtierLabel,
  transportTierOptions
};
