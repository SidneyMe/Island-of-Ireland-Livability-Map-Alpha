import assert from "node:assert/strict";

import { coarseGridPopupHtml, inspectPopupHtml } from "./popups/grid_popup.js";

{
  const html = inspectPopupHtml({
    valid_land: true,
    resolution_m: 50,
    visible_resolution_m: 250,
    effective_area_ratio: 1,
    counts: { shops: 2, transport: 1, healthcare: 0, parks: 3 },
    cluster_counts: { shops: 1, transport: 1, parks: 2 },
    effective_units: { shops: 4.5, transport: 2, parks: 3 },
    component_scores: {
      shops: 16.2,
      transport: 4.5,
      healthcare: 0,
      parks: 12.5,
      railway_proximity: -3.8,
      road_proximity: -2.1
    },
    total_score: 29.4,
    mode_aware: {
      scoring_model_version: "v2-mode-aware-dual-2026-08-14",
      weights: { walk: 0.5, bike: 0.25, transit: 0.25 },
      mode_available: { walk: true, bike: true, transit: false },
      total_score: 34.2
    }
  });

  assert.match(html, /Walk score 29\.4 \/ 100/);
  assert.match(html, /Mode-aware score 34\.2 \/ 100/);
  assert.match(html, /walk 50%, bike 25%, transit 25%/);
  assert.match(html, /Shops/);
  assert.match(html, /Railway proximity/);
  assert.match(html, /-3\.8 points/);
  assert.match(html, /Road proximity/);
  assert.match(html, /-2\.1 points/);
}

{
  const html = coarseGridPopupHtml({
    total_score: 42.1,
    resolution_m: 250,
    count_shops: 3,
    cluster_shops: 2,
    effective_units_shops: 5.5,
    score_shops: 18.3,
    count_transport: 1,
    cluster_transport: 1,
    effective_units_transport: 2,
    score_transport: 6.0,
    count_healthcare: 0,
    cluster_healthcare: 0,
    effective_units_healthcare: 0,
    score_healthcare: 0,
    count_parks: 4,
    cluster_parks: 3,
    effective_units_parks: 4,
    score_parks: 10.2,
    score_railway_proximity: -2.5,
    score_road_proximity: -1.4
  });

  assert.match(html, /Railway proximity/);
  assert.match(html, /-2\.5 points/);
  assert.match(html, /Road proximity/);
  assert.match(html, /-1\.4 points/);
}

console.log("grid popup checks passed");
