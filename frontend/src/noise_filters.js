const NOISE_METRIC_ORDER = ["Lden", "Lnight"];
const NOISE_SOURCE_ORDER = ["road", "rail", "airport", "industry", "consolidated"];
const NOISE_BAND_ORDER = ["45-49", "50-54", "55-59", "60-64", "65-69", "70-74", "70+", "75+"];

const NOISE_METRIC_LABELS = {
  Lden: "Day-evening-night",
  Lnight: "Night"
};

const NOISE_SOURCE_LABELS = {
  road: "Road",
  rail: "Rail",
  airport: "Airport",
  industry: "Industry",
  consolidated: "Consolidated"
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

function _orderedOptions(order, counts, labelFn) {
  const seen = new Set();
  const options = [];
  order.forEach(function (value) {
    seen.add(value);
    if (!Number(counts[value] || 0)) return;
    options.push({
      value: value,
      label: labelFn(value),
      count: Number(counts[value] || 0)
    });
  });
  Object.keys(counts).sort().forEach(function (value) {
    if (seen.has(value)) return;
    options.push({
      value: value,
      label: labelFn(value),
      count: Number(counts[value] || 0)
    });
  });
  return options;
}

function noiseMetricLabel(value) {
  return NOISE_METRIC_LABELS[String(value || "").trim()] || String(value || "").trim();
}

function noiseSourceLabel(value) {
  return NOISE_SOURCE_LABELS[String(value || "").trim()] || String(value || "").trim();
}

function noiseMetricOptions(runtime) {
  return _orderedOptions(
    NOISE_METRIC_ORDER,
    _normalizedCounts(runtime && runtime.noise_metric_counts),
    noiseMetricLabel
  );
}

function noiseSourceOptions(runtime) {
  return _orderedOptions(
    NOISE_SOURCE_ORDER,
    _normalizedCounts(runtime && runtime.noise_source_counts),
    noiseSourceLabel
  );
}

function noiseBandOptions(runtime) {
  return _orderedOptions(
    NOISE_BAND_ORDER,
    _normalizedCounts(runtime && runtime.noise_band_counts),
    function (value) { return value + " dB"; }
  );
}

function defaultNoiseSelections(runtime) {
  const metricOptions = noiseMetricOptions(runtime);
  const defaultMetric = metricOptions.some(function (option) {
    return option.value === "Lden";
  }) ? "Lden" : (metricOptions[0] && metricOptions[0].value) || "Lden";
  return {
    metric: defaultMetric,
    sources: noiseSourceOptions(runtime).map(function (option) { return option.value; }),
    bands: noiseBandOptions(runtime).map(function (option) { return option.value; })
  };
}

function buildNoiseLayerFilter(options = {}) {
  const metric = String(options.metric || "Lden").trim();
  const selectedSources = Array.from(options.selectedSources || []);
  const selectedBands = Array.from(options.selectedBands || []);
  const clauses = [["==", ["get", "metric"], metric]];

  if (selectedSources.length > 0) {
    clauses.push([
      "in",
      ["coalesce", ["get", "source_type"], ""],
      ["literal", selectedSources]
    ]);
  } else {
    clauses.push(["in", ["get", "source_type"], ["literal", []]]);
  }

  if (selectedBands.length > 0) {
    clauses.push([
      "in",
      ["coalesce", ["get", "db_value"], ""],
      ["literal", selectedBands]
    ]);
  } else {
    clauses.push(["in", ["get", "db_value"], ["literal", []]]);
  }

  return ["all", ...clauses];
}

export {
  NOISE_BAND_ORDER,
  NOISE_METRIC_LABELS,
  NOISE_METRIC_ORDER,
  NOISE_SOURCE_LABELS,
  NOISE_SOURCE_ORDER,
  buildNoiseLayerFilter,
  defaultNoiseSelections,
  noiseBandOptions,
  noiseMetricLabel,
  noiseMetricOptions,
  noiseSourceLabel,
  noiseSourceOptions
};
