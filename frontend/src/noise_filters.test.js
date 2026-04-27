import assert from "node:assert/strict";

import {
  buildNoiseLayerFilter,
  defaultNoiseSelections,
  noiseBandOptions,
  noiseMetricOptions,
  noiseSourceOptions
} from "./noise_filters.js";

const runtime = {
  noise_metric_counts: { Lden: 10, Lnight: 7 },
  noise_source_counts: { road: 5, rail: 3, airport: 2 },
  noise_band_counts: { "55-59": 4, "75+": 1, "50-54": 2 }
};

{
  assert.deepEqual(noiseMetricOptions(runtime).map(function (option) {
    return option.value;
  }), ["Lden", "Lnight"]);
  assert.deepEqual(noiseSourceOptions(runtime).map(function (option) {
    return option.value;
  }).slice(0, 3), ["road", "rail", "airport"]);
  assert.deepEqual(noiseBandOptions(runtime).map(function (option) {
    return option.value;
  }).filter(function (value) {
    return ["50-54", "55-59", "75+"].includes(value);
  }), ["50-54", "55-59", "75+"]);
}

{
  assert.deepEqual(defaultNoiseSelections(runtime), {
    metric: "Lden",
    sources: ["road", "rail", "airport"],
    bands: ["50-54", "55-59", "75+"]
  });
}

{
  assert.deepEqual(buildNoiseLayerFilter({
    metric: "Lnight",
    selectedSources: new Set(["road", "rail"]),
    selectedBands: new Set(["55-59", "60-64"])
  }), [
    "all",
    ["==", ["get", "metric"], "Lnight"],
    ["in", ["coalesce", ["get", "source_type"], ""], ["literal", ["road", "rail"]]],
    ["in", ["coalesce", ["get", "db_value"], ""], ["literal", ["55-59", "60-64"]]]
  ]);
}

console.log("frontend noise filter checks passed");
