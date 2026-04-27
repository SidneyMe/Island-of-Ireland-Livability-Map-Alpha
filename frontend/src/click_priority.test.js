import assert from "node:assert/strict";

import {
  CLICK_ACTIONS,
  resolveMapClickAction
} from "./click_priority.js";

function fakeFeature(id, properties = {}) {
  return { id, properties };
}

function fakeMap(featuresByLayer) {
  const calls = [];
  return {
    calls,
    queryRenderedFeatures(point, options) {
      assert.deepEqual(point, { x: 10, y: 20 });
      const layerId = options.layers[0];
      calls.push(layerId);
      return featuresByLayer[layerId] || [];
    }
  };
}

{
  const map = fakeMap({
    "service-deserts-fill": [fakeFeature("desert")]
  });
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });

  assert.equal(action.type, CLICK_ACTIONS.SERVICE_DESERT);
  assert.equal(action.features[0].id, "desert");
  assert.deepEqual(map.calls, [
    "transport-reality-circle",
    "amenities-circle",
    "service-deserts-fill"
  ]);
}

{
  const map = fakeMap({
    "noise-fill": [fakeFeature("noise")]
  });
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });

  assert.equal(action.type, CLICK_ACTIONS.NOISE);
  assert.equal(action.features[0].id, "noise");
  assert.deepEqual(map.calls, [
    "transport-reality-circle",
    "amenities-circle",
    "service-deserts-fill",
    "noise-fill"
  ]);
}

{
  const map = fakeMap({
    "transport-reality-circle": [fakeFeature("transport")],
    "amenities-circle": [fakeFeature("amenity")],
    "service-deserts-fill": [fakeFeature("desert")]
  });
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });

  assert.equal(action.type, CLICK_ACTIONS.TRANSPORT);
  assert.equal(action.features[0].id, "transport");
  assert.deepEqual(map.calls, ["transport-reality-circle"]);
}

{
  const map = fakeMap({
    "amenities-circle": [fakeFeature("amenity")],
    "service-deserts-fill": [fakeFeature("desert")]
  });
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });

  assert.equal(action.type, CLICK_ACTIONS.AMENITY);
  assert.equal(action.features[0].id, "amenity");
  assert.deepEqual(map.calls, ["transport-reality-circle", "amenities-circle"]);
}

{
  const map = fakeMap({
    "grid-fill-active": [fakeFeature("grid", { total_score: 42 })]
  });
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: false,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });

  assert.equal(action.type, CLICK_ACTIONS.COARSE_GRID);
  assert.equal(action.features[0].id, "grid");
  assert.deepEqual(map.calls, [
    "transport-reality-circle",
    "amenities-circle",
    "service-deserts-fill",
    "noise-fill",
    "grid-fill-active"
  ]);
}

{
  const map = fakeMap({});
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });

  assert.equal(action.type, CLICK_ACTIONS.FINE_INSPECT);
  assert.deepEqual(map.calls, [
    "transport-reality-circle",
    "amenities-circle",
    "service-deserts-fill",
    "noise-fill"
  ]);
}

console.log("frontend click priority checks passed");
