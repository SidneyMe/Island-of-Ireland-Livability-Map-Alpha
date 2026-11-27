import assert from "node:assert/strict";
import { linkedBuildUnavailable, mapPermalink, parseMapUrlState } from "./url_state.js";

const runtime = {
  min_zoom: 5,
  max_zoom: 19,
  build_key: "build-current",
  daily_access: { available_modes: ["walk", "bike", "mode_aware"] }
};

{
  const state = parseMapUrlState(
    "https://example.test/?debug-grid=1#v=53.3498,-6.2603,14&m=bike&c=parks&b=build-old&cell=53.35,-6.26&cmp=53.35,-6.26;51.9,-8.47",
    runtime
  );
  assert.deepEqual(state.view, { lat: 53.3498, lon: -6.2603, zoom: 14 });
  assert.equal(state.mode, "bike");
  assert.equal(state.layer, "parks");
  assert.deepEqual(state.inspect, { lat: 53.35, lon: -6.26 });
  assert.deepEqual(state.compare, [{ lat: 53.35, lon: -6.26 }, { lat: 51.9, lon: -8.47 }]);
  assert.equal(linkedBuildUnavailable(state, runtime), true);
}

{
  const href = mapPermalink("https://example.test/map?debug-grid=1", {
    lat: 53.349812,
    lon: -6.26031,
    zoom: 14.125,
    mode: "mode_aware",
    layer: "healthcare",
    buildKey: "build-current",
    inspect: { lat: 53.35, lon: -6.26 },
    compare: [{ lat: 53.35, lon: -6.26 }, { lat: 51.9, lon: -8.47 }]
  });
  const parsed = parseMapUrlState(href, runtime);
  assert.equal(new URL(href).search, "?debug-grid=1");
  assert.deepEqual(parsed.view, { lat: 53.34981, lon: -6.26031, zoom: 14.13 });
  assert.equal(parsed.mode, "mode_aware");
  assert.equal(parsed.layer, "healthcare");
  assert.equal(parsed.buildKey, "build-current");
  assert.equal(parsed.inspect, null);
  assert.deepEqual(parsed.compare, [{ lat: 53.35, lon: -6.26 }, { lat: 51.9, lon: -8.47 }]);
}

{
  const invalid = parseMapUrlState("https://example.test/#v=999,wat,99&m=car&c=bogus&cell=x,y", runtime);
  assert.equal(invalid.view, null);
  assert.equal(invalid.mode, null);
  assert.equal(invalid.layer, null);
  assert.equal(invalid.inspect, null);
  assert.equal(invalid.compare, null);
  assert.equal(parseMapUrlState("https://example.test/#cmp=53,-6", runtime).compare, null);
  assert.equal(parseMapUrlState("https://example.test/#cmp=53,-6;52,-7;51,-8", runtime).compare, null);
}

console.log("frontend URL state checks passed");

