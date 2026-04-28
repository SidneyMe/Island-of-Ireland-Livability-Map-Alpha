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

// 70+ appears in runtime counts and is included in filter UI
{
  const runtime70plus = {
    noise_metric_counts: { Lden: 5 },
    noise_source_counts: { airport: 3 },
    noise_band_counts: { "70+": 12, "75+": 4, "70-74": 8 }
  };

  const bandValues = noiseBandOptions(runtime70plus).map(function (option) {
    return option.value;
  });
  // 70+ must appear
  assert.ok(bandValues.includes("70+"), "70+ must appear in band options");
  // 70+ must appear before 75+ in stable canonical order
  const idx70plus = bandValues.indexOf("70+");
  const idx75plus = bandValues.indexOf("75+");
  assert.ok(idx70plus < idx75plus, "70+ must come before 75+ in band order");
  // 70+ is selected by default
  const defaults70plus = defaultNoiseSelections(runtime70plus);
  assert.ok(defaults70plus.bands.includes("70+"), "70+ must be selected by default");
  // 70+ does not get dropped from filter expression
  const filter70plus = buildNoiseLayerFilter({
    metric: "Lden",
    selectedSources: new Set(["airport"]),
    selectedBands: new Set(["70+", "75+"])
  });
  assert.deepEqual(filter70plus[3], [
    "in",
    ["coalesce", ["get", "db_value"], ""],
    ["literal", ["70+", "75+"]]
  ]);
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
