import assert from "node:assert/strict";

import {
  dedupeExactRoutePatterns,
  focusFeatureFilter,
  mergeRouteFeatureCollections,
  normalizeRouteIndex,
  overviewModeFilter,
  parseRouteKeys,
  routeDirectionGroups,
  routeDisplayName,
  searchRoutes
} from "./route_network.js";

assert.deepEqual(parseRouteKeys('["feed:1","feed:2","feed:1"]'), ["feed:1", "feed:2"]);
assert.deepEqual(parseRouteKeys("feed:1, feed:2"), ["feed:1", "feed:2"]);

const routes = normalizeRouteIndex({ routes: [
  {
    route_key: "feed:100X",
    route_short_name: "100X",
    route_long_name: "Dundalk–Dublin",
    mode: "bus",
    asset_url: "/transit/routes/build/a.geojson",
    directions: [{ pattern_id: "p1", origin_name: "Dundalk", destination_name: "Dublin" }]
  },
  { route_key: "rail:DART", route_short_name: "DART", mode: "rail", asset_url: "/b.geojson" }
] });
assert.equal(routeDisplayName(routes[0]), "100X · Dundalk–Dublin");
assert.deepEqual(searchRoutes(routes, "dublin", new Set(["bus"])).map((route) => route.route_key), ["feed:100X"]);
assert.deepEqual(searchRoutes(routes, "dart", new Set(["bus"])), []);
assert.deepEqual(overviewModeFilter(new Set(["rail"])), ["in", ["get", "mode"], ["literal", ["rail"]]]);
assert.ok(JSON.stringify(focusFeatureFilter(new Set(["bus"]), "p1", ["pattern"])).includes("p1"));
assert.equal(mergeRouteFeatureCollections([
  { type: "FeatureCollection", features: [{ id: 1 }] },
  { type: "FeatureCollection", features: [{ id: 2 }] }
]).features.length, 2);

function pattern(patternId, coordinates = [[-6.3, 53.3], [-6.2, 53.4]]) {
  return {
    type: "Feature",
    geometry: { type: "LineString", coordinates: coordinates },
    properties: {
      feature_kind: "pattern",
      route_key: "nta:red",
      pattern_id: patternId,
      mode: "tram",
      direction_id: 1,
      headsign: "The Point",
      origin_name: "Tallaght",
      destination_name: "The Point",
      geometry_source: "gtfs_shape"
    }
  };
}

function stop(patternId, stopId, sequence) {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [-6.3 + sequence / 100, 53.3] },
    properties: {
      feature_kind: "stop",
      route_key: "nta:red",
      pattern_id: patternId,
      feed_id: "nta",
      stop_id: stopId,
      stop_sequence: sequence
    }
  };
}

const deduped = dedupeExactRoutePatterns({
  type: "FeatureCollection",
  features: [
    pattern("p1"), stop("p1", "a", 1), stop("p1", "b", 2),
    pattern("p2"), stop("p2", "a", 1), stop("p2", "b", 2),
    pattern("p3", [[-6.3, 53.3], [-6.1, 53.4]]), stop("p3", "a", 1), stop("p3", "b", 2)
  ]
});
assert.deepEqual(
  deduped.features.filter((feature) => feature.properties.feature_kind === "pattern").map((feature) => feature.properties.pattern_id),
  ["p1", "p3"]
);
assert.equal(deduped.features.some((feature) => feature.properties.pattern_id === "p2"), false);

const directionGroups = routeDirectionGroups({
  route_key: "nta:red",
  directions: [
    { pattern_id: "p1", label: "The Point", origin_name: "Tallaght", destination_name: "The Point" },
    { pattern_id: "p2", label: "The Point", origin_name: "Tallaght", destination_name: "The Point" },
    { pattern_id: "p3", label: "The Point", origin_name: "Saggart", destination_name: "The Point" },
    { pattern_id: "p4", label: "Saggart", origin_name: "The Point", destination_name: "Saggart" }
  ]
}, deduped);
assert.deepEqual(directionGroups.map((group) => [group.label, group.directions.map((direction) => direction.pattern_id)]), [
  ["The Point", ["p1", "p3"]]
]);

console.log("frontend route network checks passed");
