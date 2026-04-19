import assert from "node:assert/strict";

import {
  buildAmenityLayerFilter,
  defaultAmenityTierSelections,
  formatAmenityLabel,
  tierOptionsForCategory,
} from "./amenity_filters.js";

const runtime = {
  amenity_tier_counts: {
    shops: { corner: 12, regular: 30, supermarket: 8, mall: 1 },
    transport: {},
    healthcare: { local: 6, clinic: 2, hospital: 1, emergency_hospital: 1 },
    parks: { pocket: 14, neighbourhood: 5, district: 2, regional: 1 },
  },
};

{
  assert.equal(formatAmenityLabel("shops"), "Shops");
  assert.equal(formatAmenityLabel("emergency_hospital"), "Emergency hospital");
}

{
  assert.deepEqual(tierOptionsForCategory(runtime, "shops"), [
    "corner",
    "regular",
    "supermarket",
    "mall",
  ]);
  assert.deepEqual(tierOptionsForCategory(runtime, "transport"), []);
}

{
  assert.deepEqual(defaultAmenityTierSelections(runtime), {
    shops: ["corner", "regular", "supermarket", "mall"],
    healthcare: ["local", "clinic", "hospital", "emergency_hospital"],
    parks: ["pocket", "neighbourhood", "district", "regional"],
  });
}

{
  const filter = buildAmenityLayerFilter(runtime, new Set(["transport"]), new Map());
  assert.deepEqual(filter, ["==", ["get", "category"], "transport"]);
}

{
  const filter = buildAmenityLayerFilter(
    runtime,
    new Set(["shops", "transport"]),
    new Map([["shops", new Set(["corner", "mall"])]]),
  );
  assert.deepEqual(filter, [
    "any",
    [
      "all",
      ["==", ["get", "category"], "shops"],
      ["in", ["coalesce", ["get", "tier"], ""], ["literal", ["corner", "mall"]]],
    ],
    ["==", ["get", "category"], "transport"],
  ]);
}

{
  const filter = buildAmenityLayerFilter(
    runtime,
    new Set(["parks"]),
    new Map([["parks", new Set()]]),
  );
  assert.deepEqual(filter, ["in", ["get", "category"], ["literal", []]]);
}

console.log("frontend amenity filter checks passed");
