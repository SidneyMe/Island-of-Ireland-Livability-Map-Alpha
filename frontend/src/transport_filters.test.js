import assert from "node:assert/strict";

import {
  buildTransportLayerFilter,
  routeModeTokenFilter,
  transportBusFrequencyLabel,
  transportBusFrequencyOptions,
  transportFlagCounts,
  transportModeOptions,
  transportSubtierLabel,
  transportTierOptions
} from "./transport_filters.js";

const runtime = {
  transport_subtier_counts: {
    mon_sun: 10,
    mon_sat: 8,
    weekdays_only: 3
  },
  transport_bus_frequency_counts: {
    frequent: 5,
    moderate: 4,
    token_skeletal: 1
  },
  transport_mode_counts: {
    tram: 4,
    rail: 6
  },
  transport_flag_counts: {
    is_unscheduled_stop: 2,
    has_exception_only_service: 4,
    has_any_bus_service: 21,
    has_daily_bus_service: 10
  }
};

assert.equal(transportSubtierLabel("mon_sun"), "Whole week");
assert.equal(transportSubtierLabel("single_day_only"), "Single-day only");
assert.equal(transportSubtierLabel(null), "No recent public transport tier");
assert.equal(transportBusFrequencyLabel("frequent"), "Frequent (<=15 min)");
assert.equal(transportBusFrequencyLabel("very_low_frequency"), "Very low frequency (61-120 min)");

assert.deepEqual(
  transportTierOptions(runtime).map(function (entry) {
    return [entry.value, entry.count];
  }),
  [
    ["mon_sun", 10],
    ["mon_sat", 8],
    ["tue_sun", 0],
    ["weekdays_only", 3],
    ["weekends_only", 0],
    ["single_day_only", 0],
    ["partial_week", 0]
  ]
);

assert.deepEqual(
  transportBusFrequencyOptions(runtime).map(function (entry) {
    return [entry.value, entry.label, entry.count];
  }),
  [
    ["frequent", "Frequent (<=15 min)", 5],
    ["moderate", "Moderate (16-30 min)", 4],
    ["low_frequency", "Low frequency (31-60 min)", 0],
    ["very_low_frequency", "Very low frequency (61-120 min)", 0],
    ["token_skeletal", "Token / skeletal (>120 min)", 1]
  ]
);

assert.deepEqual(
  transportModeOptions(runtime).map(function (entry) {
    return [entry.value, entry.label, entry.count];
  }),
  [
    ["tram", "Tram", 4],
    ["rail", "Rail", 6]
  ]
);

assert.deepEqual(transportFlagCounts(runtime), runtime.transport_flag_counts);

assert.equal(buildTransportLayerFilter({}), null);
assert.deepEqual(
  routeModeTokenFilter("rail"),
  ["in", ",rail,", ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]]
);
assert.deepEqual(
  buildTransportLayerFilter({
    selectedSubtiers: new Set(["mon_sat", "weekdays_only"]),
    selectedBusFrequencies: new Set(["frequent"]),
    selectedModes: new Set(["rail"]),
    includeUnscheduled: true,
    requireExceptionOnly: true
  }),
  [
    "all",
    [
      "any",
      ["in", ["coalesce", ["get", "bus_service_subtier"], ""], ["literal", ["mon_sat", "weekdays_only"]]],
      ["in", ["coalesce", ["get", "bus_frequency_tier"], ""], ["literal", ["frequent"]]],
      ["in", ",rail,", ["concat", ",", ["coalesce", ["get", "route_modes"], ""], ","]],
      ["==", ["get", "is_unscheduled_stop"], 1]
    ],
    ["==", ["get", "has_exception_only_service"], 1]
  ]
);

console.log("transport filter checks passed");
