import assert from "node:assert/strict";

import {
  buildGridDebugSnapshot,
  copyGridDebugSnapshot,
  createGridDebugState,
  debugGridEnabledFromUrl,
  formatSourceResolutions,
  renderGridDebugCard
} from "./grid_debug.js";

const runtime = { build_profile: "full" };

function makeCardElements() {
  return {
    section: { hidden: true },
    diagnosis: { textContent: "" },
    snapshotFallback: { hidden: true, textContent: "" },
    values: {
      enabled: { textContent: "" },
      zoom: { textContent: "" },
      resolutionM: { textContent: "" },
      gridVisible: { textContent: "" },
      activeFilter: { textContent: "" },
      sourceCount: { textContent: "" },
      renderedCount: { textContent: "" },
      sourceResolutions: { textContent: "" },
      fillLayer: { textContent: "" },
      fillLayerId: { textContent: "" },
      outlineLayer: { textContent: "" },
      outlineLayerId: { textContent: "" },
      sourceLoaded: { textContent: "" },
      lastSourceEvent: { textContent: "" },
      lastMapError: { textContent: "" }
    }
  };
}

function makeDebugState(overrides = {}) {
  return Object.assign(createGridDebugState(true), {
    zoom: 14,
    resolutionM: 500,
    gridVisible: true,
    activeFillLayerId: "grid-fill-active",
    activeOutlineLayerId: "grid-outline-active",
    activeFilter: "resolution_m == 500",
    sourceCount: 18,
    renderedCount: 12,
    sourceResolutions: { 500: 18, 1000: 4 },
    fillLayerPresent: true,
    outlineLayerPresent: true,
    fillVisibility: "visible",
    outlineVisibility: "visible",
    sourceLoaded: true,
    lastSourceEvent: "sourcedata/content/idle",
    lastMapError: "none"
  }, overrides);
}

{
  assert.equal(debugGridEnabledFromUrl("/"), false);
  assert.equal(debugGridEnabledFromUrl("/?debug-grid=0"), false);
  assert.equal(debugGridEnabledFromUrl("/?debug-grid=1"), true);
  assert.equal(debugGridEnabledFromUrl("http://127.0.0.1:8000/"), false);
  assert.equal(debugGridEnabledFromUrl("http://127.0.0.1:8000/?debug-grid=0"), false);
  assert.equal(debugGridEnabledFromUrl("http://127.0.0.1:8000/?debug-grid=1"), true);
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, createGridDebugState(false));
  assert.equal(elements.section.hidden, true);
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, makeDebugState());
  assert.equal(elements.section.hidden, false);
  assert.equal(elements.values.enabled.textContent, "on");
  assert.equal(elements.values.sourceResolutions.textContent, "500:18, 1000:4");
}

{
  assert.equal(formatSourceResolutions(null), "none");
  assert.equal(formatSourceResolutions({}), "none");
  assert.equal(
    formatSourceResolutions({ 2500: 0, 1000: 7, 500: 12 }),
    "500:12, 1000:7"
  );
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, makeDebugState({
    zoom: 12,
    resolutionM: 2500,
    activeFilter: "resolution_m == 2500"
  }));
  assert.equal(elements.values.zoom.textContent, "z12.00");
  assert.equal(elements.values.resolutionM.textContent, "2.5km");

  renderGridDebugCard(elements, runtime, makeDebugState({
    zoom: 14,
    resolutionM: 500,
    activeFilter: "resolution_m == 500"
  }));
  assert.equal(elements.values.zoom.textContent, "z14.00");
  assert.equal(elements.values.resolutionM.textContent, "500m");
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, makeDebugState({
    sourceCount: 9,
    renderedCount: 0
  }));
  assert.equal(
    elements.diagnosis.textContent,
    "Tile data loaded, but grid is not rendering"
  );
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, makeDebugState({
    sourceCount: 0,
    renderedCount: 0
  }));
  assert.equal(
    elements.diagnosis.textContent,
    "No matching grid features in the current viewport"
  );
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, makeDebugState({
    activeFillLayerId: null,
    fillLayerPresent: false
  }));
  assert.equal(
    elements.diagnosis.textContent,
    "Active grid layer was not rebuilt correctly"
  );
}

{
  const elements = makeCardElements();
  renderGridDebugCard(elements, runtime, makeDebugState({
    fillVisibility: "none",
    outlineVisibility: "none"
  }));
  assert.equal(
    elements.diagnosis.textContent,
    "Active grid layers are present but hidden"
  );
}

{
  const debugState = makeDebugState({
    sourceCount: 11,
    renderedCount: 0,
    sourceResolutions: { 500: 11, 1000: 3 }
  });
  const expectedSnapshot = [
    "build_profile=full",
    "zoom=z14.00",
    "active_resolution=500m",
    "source_count=11",
    "rendered_count=0",
    "source_resolutions=500:11, 1000:3",
    "fill_layer=present + visible (grid-fill-active)",
    "outline_layer=present + visible (grid-outline-active)",
    "filter=resolution_m == 500",
    "last_source_event=sourcedata/content/idle",
    "last_map_error=none"
  ].join("\n");
  const snapshot = buildGridDebugSnapshot(runtime, debugState);
  let copiedText = "";
  const copied = await copyGridDebugSnapshot(
    {
      writeText: async function (text) {
        copiedText = text;
      }
    },
    snapshot
  );

  assert.equal(snapshot, expectedSnapshot);
  assert.equal(copied, true);
  assert.equal(copiedText, expectedSnapshot);
}

console.log("frontend grid debug checks passed");
