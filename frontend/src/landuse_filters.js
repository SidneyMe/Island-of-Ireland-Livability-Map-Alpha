const LANDUSE_CONTEXT_CLASS_ORDER = [
  "residential",
  "commercial",
  "industrial",
  "retail",
  "farmland",
  "forest"
];

const LANDUSE_CONTEXT_CLASS_LABELS = {
  residential: "Residential",
  commercial: "Commercial",
  industrial: "Industrial",
  retail: "Retail",
  farmland: "Farmland",
  forest: "Forest"
};

const LANDUSE_CONTEXT_CLASS_COLORS = {
  residential: "#d8e6d4",
  commercial: "#f2c7a0",
  industrial: "#c8c6d7",
  retail: "#f3b6b6",
  farmland: "#efe0a8",
  forest: "#a3d29d"
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

function landuseContextLabel(value) {
  return LANDUSE_CONTEXT_CLASS_LABELS[String(value || "").trim()] || String(value || "").trim();
}

function landuseContextOptions(runtime) {
  const counts = _normalizedCounts(runtime && runtime.landuse_context_counts);
  return LANDUSE_CONTEXT_CLASS_ORDER.map(function (landuseClass) {
    return {
      value: landuseClass,
      label: landuseContextLabel(landuseClass),
      count: Number(counts[landuseClass] || 0)
    };
  });
}

function defaultLanduseContextSelections(runtime) {
  return landuseContextOptions(runtime).map(function (option) {
    return option.value;
  });
}

function buildLanduseContextLayerFilter(options = {}) {
  const selectedClasses = Array.from(options.selectedClasses || [])
    .map(function (value) { return String(value || "").trim(); })
    .filter(Boolean);
  if (!selectedClasses.length) {
    return null;
  }
  const selectedClassSet = new Set(selectedClasses);
  const allKnownSelected = (
    selectedClassSet.size === LANDUSE_CONTEXT_CLASS_ORDER.length &&
    LANDUSE_CONTEXT_CLASS_ORDER.every(function (value) {
      return selectedClassSet.has(value);
    })
  );
  if (allKnownSelected) {
    return null;
  }
  if (selectedClasses.length === 1) {
    return ["==", ["get", "class"], selectedClasses[0]];
  }
  return ["in", ["get", "class"], ["literal", selectedClasses]];
}

function landuseContextFillColorExpression() {
  const expr = ["match", ["get", "class"]];
  LANDUSE_CONTEXT_CLASS_ORDER.forEach(function (landuseClass) {
    expr.push(landuseClass, LANDUSE_CONTEXT_CLASS_COLORS[landuseClass]);
  });
  expr.push("#d9d9d9");
  return expr;
}

export {
  LANDUSE_CONTEXT_CLASS_COLORS,
  LANDUSE_CONTEXT_CLASS_LABELS,
  LANDUSE_CONTEXT_CLASS_ORDER,
  buildLanduseContextLayerFilter,
  defaultLanduseContextSelections,
  landuseContextFillColorExpression,
  landuseContextLabel,
  landuseContextOptions
};
