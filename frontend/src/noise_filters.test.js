import assert from "node:assert/strict";

import {
  buildNoiseLayerFilter,
  defaultNoiseSelections,
  noiseBandOptions,
  noiseMetricOptions,
  noiseSourceOptions
} from "./noise_filters.js";

const runtime = {
  noise_metric_counts: { Lden: 10, Lnight: 8 },
  noise_source_counts: { road: 5, rail: 3, airport: 2 },
  noise_band_counts: { "55-59": 4, "60-64": 3, "65-69": 2, "75+": 1, "45-49": 2, "50-54": 2, "70+": 1 },
  noise_band_counts_by_metric: {
    Lden: { "55-59": 4, "60-64": 3, "65-69": 2, "75+": 1 },
    Lnight: { "45-49": 2, "50-54": 2, "55-59": 2, "60-64": 1, "70+": 1 }
  }
};

{
  assert.deepEqual(noiseMetricOptions(runtime).map(function (option) {
    return option.value;
  }), ["Lden", "Lnight"]);
  assert.deepEqual(noiseSourceOptions(runtime).map(function (option) {
    return option.value;
  }), ["road", "rail", "airport"]);
  assert.deepEqual(noiseBandOptions(runtime, "Lden").map(function (option) {
    return option.value;
  }), ["55-59", "60-64", "65-69", "75+"]);
  assert.deepEqual(noiseBandOptions(runtime, "Lnight").map(function (option) {
    return option.value;
  }), ["45-49", "50-54", "55-59", "60-64", "70+"]);
}

{
  const roadOnlyRuntime = {
    noise_metric_counts: { Lden: 2, Lnight: 1 },
    noise_source_counts: { road: 3 }
  };
  assert.deepEqual(noiseSourceOptions(roadOnlyRuntime).map(function (option) {
    return option.value;
  }), ["road"]);
}

{
  assert.deepEqual(defaultNoiseSelections(runtime), {
    metric: "Lden",
    sources: ["road", "rail", "airport"],
    bands: ["55-59", "60-64", "65-69", "75+"]
  });
}

{
  assert.deepEqual(buildNoiseLayerFilter({
    metric: "Lden",
    selectedSources: new Set(["road", "rail", "airport"]),
    selectedBands: new Set(["55-59", "60-64"])
  }), [
    "all",
    ["in", ["get", "kind"], ["literal", ["road", "rail", "airport"]]],
    ["==", ["get", "metric"], "Lden"],
    [
      "in",
      ["get", "method"],
      ["literal", ["official_derived_grid_proxy", "official_resolved_contour"]]
    ]
  ]);
}

{
  assert.deepEqual(buildNoiseLayerFilter({
    metric: "Lden",
    selectedSources: new Set(),
    selectedBands: new Set()
  }), [
    "all",
    ["in", ["get", "kind"], ["literal", []]],
    ["==", ["get", "metric"], "Lden"],
    [
      "in",
      ["get", "method"],
      ["literal", ["official_derived_grid_proxy", "official_resolved_contour"]]
    ]
  ]);
}

console.log("frontend noise filter checks passed");
