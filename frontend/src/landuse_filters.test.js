import assert from "node:assert/strict";

import {
  buildLanduseContextLayerFilter,
  defaultLanduseContextSelections,
  landuseContextFillColorExpression,
  landuseContextOptions
} from "./landuse_filters.js";

const runtime = {
  landuse_context_counts: {
    residential: 4,
    commercial: 2,
    industrial: 1,
    retail: 3,
    farmland: 6,
    forest: 5
  }
};

{
  const options = landuseContextOptions(runtime);
  assert.deepEqual(
    options.map(function (option) { return option.value; }),
    ["residential", "commercial", "industrial", "retail", "farmland", "forest"]
  );
  assert.deepEqual(
    options.map(function (option) { return option.count; }),
    [4, 2, 1, 3, 6, 5]
  );
  assert.equal(options[0].label, "Residential");
  assert.equal(options[5].label, "Forest");
}

{
  assert.deepEqual(
    defaultLanduseContextSelections(runtime),
    ["residential", "commercial", "industrial", "retail", "farmland", "forest"]
  );
}

{
  assert.equal(buildLanduseContextLayerFilter({ selectedClasses: [] }), null);
  assert.deepEqual(
    buildLanduseContextLayerFilter({
      selectedClasses: new Set(["residential"])
    }),
    ["==", ["get", "class"], "residential"]
  );
  assert.deepEqual(
    buildLanduseContextLayerFilter({
      selectedClasses: new Set(["residential", "forest"])
    }),
    ["in", ["get", "class"], ["literal", ["residential", "forest"]]]
  );
}

{
  assert.deepEqual(landuseContextFillColorExpression().slice(0, 2), ["match", ["get", "class"]]);
  assert.ok(landuseContextFillColorExpression().includes("#a3d29d"));
}

console.log("frontend land-use filter checks passed");
