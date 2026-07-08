import assert from "node:assert/strict";

import {
  sourceExists,
  sourceLoaded
} from "./map_source_guard.js";

{
  let loadedCalls = 0;
  const map = {
    getSource(sourceId) {
      assert.equal(sourceId, "livability");
      return undefined;
    },
    isSourceLoaded() {
      loadedCalls += 1;
      throw new Error("There is no source with ID 'livability'");
    }
  };

  assert.equal(sourceExists(map, "livability"), false);
  assert.equal(sourceLoaded(map, "livability", true), false);
  assert.equal(loadedCalls, 0);
}

{
  const map = {
    getSource(sourceId) {
      assert.equal(sourceId, "livability");
      return { id: "livability" };
    },
    isSourceLoaded(sourceId) {
      assert.equal(sourceId, "livability");
      return true;
    }
  };

  assert.equal(sourceExists(map, "livability"), true);
  assert.equal(sourceLoaded(map, "livability"), true);
}

{
  const map = {
    getSource() {
      return { id: "livability" };
    },
    isSourceLoaded() {
      throw new Error("source loading state unavailable");
    }
  };

  assert.equal(sourceLoaded(map, "livability", true), true);
}

console.log("map source guard checks passed");
