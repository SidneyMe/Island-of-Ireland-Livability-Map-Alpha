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
  noise_source_counts: { road: 5 },
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
  }), ["road"]);
  assert.deepEqual(noiseBandOptions(runtime, "Lden").map(function (option) {
    return option.value;
  }), ["55-59", "60-64", "65-69", "75+"]);
  assert.deepEqual(noiseBandOptions(runtime, "Lnight").map(function (option) {
    return option.value;
  }), ["45-49", "50-54", "55-59", "60-64", "70+"]);
}

{
  assert.deepEqual(defaultNoiseSelections(runtime), {
    metric: "Lden",
    sources: ["road"],
    bands: ["55-59", "60-64", "65-69", "75+"]
  });
}

{
  assert.deepEqual(buildNoiseLayerFilter({
    metric: "Lden",
    selectedSources: new Set(["road"]),
    selectedBands: new Set(["55-59", "60-64"])
  }), [
    "all",
    ["==", ["get", "kind"], "road"],
    ["==", ["get", "metric"], "Lden"],
    ["==", ["get", "method"], "official_derived_grid_proxy"],
    ["in", ["coalesce", ["get", "kind"], ""], ["literal", ["road"]]],
    ["in", ["coalesce", ["get", "band"], ["get", "db_value"], ""], ["literal", ["55-59", "60-64"]]]
  ]);
}

{
  assert.deepEqual(buildNoiseLayerFilter({
    metric: "Lden",
    selectedSources: new Set(),
    selectedBands: new Set()
  }), [
    "all",
    ["==", ["get", "kind"], "road"],
    ["==", ["get", "metric"], "Lden"],
    ["==", ["get", "method"], "official_derived_grid_proxy"],
    ["in", ["get", "kind"], ["literal", []]],
    ["in", ["get", "band"], ["literal", []]]
  ]);
}

console.log("frontend noise filter checks passed");
