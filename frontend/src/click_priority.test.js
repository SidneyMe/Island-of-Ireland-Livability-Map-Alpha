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
    "transport-focus-transfers",
    "transport-transfer-hubs",
    "transport-focus-line",
    "transport-focus-approximate",
    "transport-routes-line",
    "transport-routes-approximate",
    "amenities-circle",
    "service-deserts-fill"
  ]);
}

{
  const map = fakeMap({
    "noise-proxy-fill": [fakeFeature("noise")]
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
    "transport-focus-transfers",
    "transport-transfer-hubs",
    "transport-focus-line",
    "transport-focus-approximate",
    "transport-routes-line",
    "transport-routes-approximate",
    "amenities-circle",
    "service-deserts-fill",
    "noise-proxy-fill"
  ]);
}

{
  const map = fakeMap({
    "transport-routes-line": [fakeFeature("route", { route_key: "feed:10" })],
    "amenities-circle": [fakeFeature("amenity")]
  });
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });
  assert.equal(action.type, CLICK_ACTIONS.TRANSPORT_ROUTE);
  assert.equal(action.features[0].properties.route_key, "feed:10");
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
  assert.deepEqual(map.calls, [
    "transport-reality-circle",
    "transport-focus-transfers",
    "transport-transfer-hubs",
    "transport-focus-line",
    "transport-focus-approximate",
    "transport-routes-line",
    "transport-routes-approximate",
    "amenities-circle"
  ]);
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
    "transport-focus-transfers",
    "transport-transfer-hubs",
    "transport-focus-line",
    "transport-focus-approximate",
    "transport-routes-line",
    "transport-routes-approximate",
    "amenities-circle",
    "service-deserts-fill",
    "noise-proxy-fill",
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
    "transport-focus-transfers",
    "transport-transfer-hubs",
    "transport-focus-line",
    "transport-focus-approximate",
    "transport-routes-line",
    "transport-routes-approximate",
    "amenities-circle",
    "service-deserts-fill",
    "noise-proxy-fill"
  ]);
}

{
  // A style without the noise overlay: the layer is skipped, not queried.
  const map = fakeMap({});
  map.getLayer = function (layerId) {
    return layerId === "noise-proxy-fill" ? undefined : { id: layerId };
  };
  const action = resolveMapClickAction({
    map,
    point: { x: 10, y: 20 },
    fineSurfaceEnabled: true,
    gridVisible: true,
    activeGridLayerId: "grid-fill-active"
  });
  assert.equal(action.type, CLICK_ACTIONS.FINE_INSPECT);
  assert.ok(!map.calls.includes("noise-proxy-fill"));
}

console.log("frontend click priority checks passed");
