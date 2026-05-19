const NOISE_METRIC_ORDER = ["Lden", "Lnight"];
const NOISE_SOURCE_ORDER = ["road", "rail"];
const NOISE_BAND_ORDER = ["45-49", "50-54", "55-59", "60-64", "65-69", "70+", "70-74", "75+", "80+"];

const NOISE_METRIC_LABELS = {
  Lden: "Day-evening-night",
  Lnight: "Night"
};

const NOISE_SOURCE_LABELS = {
  road: "Road",
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

function noiseBandLabel(value) {
  return String(value || "").trim();
}

function noiseBandOptions(runtime, metric = null) {
  const metricKey = String(metric || "").trim();
  const byMetric = runtime && runtime.noise_band_counts_by_metric && typeof runtime.noise_band_counts_by_metric === "object"
    ? runtime.noise_band_counts_by_metric
    : null;
  const metricCounts = byMetric && metricKey
    ? _normalizedCounts(byMetric[metricKey])
    : null;
  return _orderedOptions(
    NOISE_BAND_ORDER,
    metricCounts || _normalizedCounts(runtime && runtime.noise_band_counts),
    noiseBandLabel
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
    bands: noiseBandOptions(runtime, defaultMetric).map(function (option) { return option.value; })
  };
}

function buildNoiseLayerFilter(options = {}) {
  const metric = String(options.metric || "Lden").trim();
  const selectedSources = Array.from(options.selectedSources || []);
  const clauses = [
    ["in", ["get", "kind"], ["literal", selectedSources]],
    ["==", ["get", "metric"], metric],
    ["==", ["get", "method"], "official_derived_grid_proxy"]
  ];

  return ["all", ...clauses];
}

export {
  NOISE_METRIC_LABELS,
  NOISE_METRIC_ORDER,
  NOISE_SOURCE_LABELS,
  NOISE_SOURCE_ORDER,
  NOISE_BAND_ORDER,
  buildNoiseLayerFilter,
  defaultNoiseSelections,
  noiseBandLabel,
  noiseBandOptions,
  noiseMetricLabel,
  noiseMetricOptions,
  noiseSourceLabel,
  noiseSourceOptions
};
