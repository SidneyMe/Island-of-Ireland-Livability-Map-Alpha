import assert from "node:assert/strict";
import { buildMetadataView } from "./build_metadata.js";

const view = buildMetadataView({
  build_key: "build-1234567890",
  scoring_model: "daily-access-v4",
  data_provenance: {
    build: { key: "build-1234567890", completed_at: "2026-11-27T12:00:00Z" },
    osm: { import_fingerprint: "osm-abcdefghijk", imported_at: "2026-11-26T09:00:00Z" },
    gtfs: { analysis_date: "2026-11-27", sources: [{ id: "tfi", label: "TFI" }] },
    model: { name: "daily-access-v4", version: 3 },
    freshness: { status: "current", warnings: [] }
  }
});
assert.equal(view.statusLabel, "Current");
assert.equal(view.modelLabel, "daily-access-v4 · model 3");
assert.match(view.osmLabel, /2026-11-26/);
assert.match(view.gtfsLabel, /TFI/);
assert.equal(view.buildLabel, "build-1234");

console.log("frontend build metadata checks passed");
