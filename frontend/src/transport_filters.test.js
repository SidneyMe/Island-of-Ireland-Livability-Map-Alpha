import assert from "node:assert/strict";

import {
  buildTransportLayerFilter,
  transportFlagCounts,
  transportSubtierLabel,
  transportTierOptions
} from "./transport_filters.js";

const runtime = {
  transport_subtier_counts: {
    mon_sun: 10,
    mon_sat: 8,
    weekdays_only: 3
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

assert.deepEqual(transportFlagCounts(runtime), runtime.transport_flag_counts);

assert.equal(buildTransportLayerFilter({}), null);
assert.deepEqual(
  buildTransportLayerFilter({
    selectedSubtiers: new Set(["mon_sat", "weekdays_only"]),
    includeUnscheduled: true,
    requireExceptionOnly: true
  }),
  [
    "all",
    [
      "any",
      ["in", ["coalesce", ["get", "bus_service_subtier"], ""], ["literal", ["mon_sat", "weekdays_only"]]],
      ["==", ["get", "is_unscheduled_stop"], 1]
    ],
    ["==", ["get", "has_exception_only_service"], 1]
  ]
);

console.log("transport filter checks passed");
