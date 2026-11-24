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

{
  // Daily access v4 inspect: stored values, no counts/clusters/effective units.
  const html = inspectPopupHtml({
    scoring_model: "daily-access-v4",
    valid_land: true,
    resolution_m: 50,
    network_status: "ok",
    total_score: 61.25,
    component_scores: { shops: 30, transport: 21, healthcare: 8, parks: 4 },
    penalties: { railway_proximity: 0, road_proximity: 1.75 },
    service_access: { food: 0.9, other_retail: 0.2, primary_care: 0.5, pharmacy: 0.4, green: 0.3, transport: 0.7 },
    counts: null,
    cluster_counts: null,
    effective_units: null,
    legacy_fields_unavailable: ["counts", "cluster_counts", "effective_units"],
    visible_resolution_m: 5000,
    visible: {
      resolution_m: 5000,
      total_score: 40.5,
      network_status: "partial",
      network_coverage_share: 0.82,
      component_scores: { shops: 20, transport: 10, healthcare: 6, parks: 5 },
      penalties: { railway_proximity: 0.5, road_proximity: 0 },
      service_access: { food: 0.6, other_retail: 0.1, primary_care: 0.3, pharmacy: 0.2, green: 0.4, transport: 0.5 }
    }
  });
  assert.match(html, /Daily access score 61\.3 \/ 100/);
  assert.match(html, /Food shops: 90%/);
  assert.match(html, /Road proximity<\/strong>: -1\.8 points/);
  assert.match(html, /82% of land/);
  assert.match(html, /Model: daily-access-v4/);
  assert.doesNotMatch(html, /raw|clusters|effective units/);
}

{
  // Without a zoom the visible cell is null: nothing invented.
  const html = inspectPopupHtml({ scoring_model: "daily-access-v4", valid_land: false, visible: null });
  assert.match(html, /No land cell/);
  assert.doesNotMatch(html, /Visible/);
}

{
  const html = coarseGridPopupHtml(
    { total_score: 33, resolution_m: 5000, score_shops: 20, count_shops: 0, cluster_shops: 0, effective_units_shops: 0 },
    5000,
    { legacyUnavailable: true }
  );
  assert.match(html, /Daily access score 33\.0/);
  assert.doesNotMatch(html, /0 raw/);
  assert.match(html, /not part of this model/);
}

console.log("grid popup checks passed");
