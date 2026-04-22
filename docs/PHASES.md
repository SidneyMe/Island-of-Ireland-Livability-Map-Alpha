# Implementation Phases

Implementation notes for the [README roadmap](../README.md#roadmap). This file is the working document — opinionated, detailed, and expected to change as design decisions are made. The README lists *what* is going to be built; this file is *how* and *in what order*.

Phases are ordered by dependency. Within a phase, items can proceed in parallel. Phase numbers reflect unblock order, not priority — Phase 0 guardrails are boring but have to exist before scoring work can move safely, and the public launch in Phase 8 is deliberately at the end: the hosted demo is meant to drop as a finished thing, not as a bit-by-bit dripfeed. Code lives on GitHub from day one and updates land there continuously, but the public-facing site exists once there is something worth landing on.

Each item lists:

- **What** — the concrete deliverable.
- **Why** — what it changes about the scoring model or the product.
- **How** — the implementation outline. Not a spec, but enough to start.
- **Depends on** — items that must land before this one can.
- **Open questions** — decisions that still need to be made before implementation.

Tags:

- **[blocker]** — other phases depend on this item.
- **[design-needed]** — implementation is gated on a written design note answering specific modelling questions.
- **[independent]** — can be picked up without touching the core scoring pipeline.

---

## Conventions

### Cache and schema versioning

`config.py` exposes three independent version numbers. Bump the right one when a phase lands changes, otherwise the pipeline ships stale results without noticing.

- **`GRID_GEOMETRY_SCHEMA_VERSION`** — bump when the grid itself changes shape (cell size, projection, clipping). Phase 0's coastal grid clipping is the textbook case.
- **`CACHE_SCHEMA_VERSION`** — bump when the *meaning* of the cached scoring outputs changes. Any new scoring component, any new modifier, or any change to how an existing score is computed bumps this. Phases 1, 2, 3, 4, and 5 will all touch it.
- **`PMTILES_SCHEMA_VERSION`** — bump when the structure of the published PMTiles archive changes (new layers, renamed properties, removed columns). Phase 6's per-cell breakdowns and layer toggles bump this because they store new per-component fields.

When in doubt, bump the narrower version first and let the existing hash machinery in `config.py` decide whether the change cascades.

---

## Phase 0 — Foundation and guardrails

Everything in this phase is either a scoring prerequisite or a cleanup that stops being fixable later. None of it depends on the rest of the roadmap.

### ~~Livability sanity fixture [blocker]~~

**What:** A versioned JSON file of 20–40 hand-picked reference locations across Ireland, each with an expected score range and a short rationale.

**Why:** Without a regression fixture, every scoring change is a guess. The fixture pins the model to lived experience: Dublin city centre is not the top score; a quiet suburb with a GP, a park, and a supermarket often is. Every later phase in this document touches the scoring pipeline, and this is the only thing that will catch regressions before they ship.

**How:**

- Seed the fixture with deliberate traps:
  - Single-building amenity clusters (shops + pharmacy + transport behind one counter) — should not score like a real neighbourhood.
  - Noisy but nominally-walkable areas (next to a railway, under a flight path) — should be penalized.
  - Quiet places with few services but high pleasantness — should score respectably.
  - Rural areas with ghost bus stops — should not score above real-service towns once Phase 1 lands.
- Each entry: lat/lon, category tags (rural/urban/coastal/noise), expected score range, short rationale.
- Add a `scripts/sanity_check.py` runner that computes scores at each fixture point and asserts they fall in the expected range.
- Wire the runner into CI so scoring changes can't silently break the fixture.

**Open questions:**

- Expected scores are hand-assigned and will drift as the model matures. Track the fixture's history in git so drift is visible; treat large fixture rewrites as a deliberate design decision, not a quiet update.

### ~~Coastal grid-cell clipping [independent]~~

**What:** Clip grid cells straddling the coastline to land, so coastal cells don't look artificially sparse.

**Why:** A coastal cell that is 30% land and 70% sea is currently scored as if it were 100% land, which drags its density metrics down. Clipping fixes the density denominator.

**How:**

- Intersect grid polygons with the combined ROI + NI land boundary (already in `boundaries/`).
- Store the clipped geometry as the cell's effective area.
- Normalize amenity density and park area by clipped area, not raw cell area.
- Affects geometry hashing — bump `GRID_GEOMETRY_SCHEMA_VERSION` in `config.py`.

**Done so far:**

- Grid cells now persist `effective_area_m2` and `effective_area_ratio` derived from the existing clipped metric geometry.
- Cache validation now requires the clipped-area metadata, so old grid shells are rebuilt instead of being reused silently.
- `grid_walk` now stores the clipped-area fields, with compatibility `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` upgrades for existing databases.
- Coastal amenity-density normalization now applies to `shops`, `transport`, and `healthcare`, using a clipped-area ratio floor of `0.25`.
- Park scoring now uses reachable polygon park area rather than raw reachable park-feature count, still normalized by the same clipped-area ratio floor of `0.25`.
- Raw park counts remain raw feature counts for publishing/UI compatibility; only park scoring uses the interim reachable-area lookup.
- `GRID_GEOMETRY_SCHEMA_VERSION` was bumped to reflect the new persisted grid metadata.
- This phase item is still not fully done: park scoring is now area-based, but the broader Phase 2 park-size-tier redesign is still outstanding.

### ~~Remove Windows-only assumptions [independent]~~

**What:** The code path should run on Linux/macOS in principle. Testing and documentation for those platforms remain out of scope.

**How:**

- Audit `config.py`, `main.py`, and the `precompute/` pipeline for hardcoded `.exe` suffixes and PowerShell-only paths.
- `_default_walkgraph_bin()` in `config.py` already checks a hardcoded `walkgraph.exe`; make it platform-aware.
- Replace any remaining string-join path operations with `pathlib.Path`.
- Add POSIX-equivalent setup commands alongside the PowerShell ones in the README.

### ~~Basic CI [independent]~~

**What:** Run Python and Rust tests on every push.

**How:**

- GitHub Actions workflow.
- Matrix: Python 3.12, Rust stable. Start with a Windows runner (matches current reality); add a Linux runner once Windows-specific assumptions are cleared.
- Run `python -m unittest discover -s tests -t . -p "test_*.py"` and `cargo test` in `walkgraph/`.
- Run the sanity fixture once it exists.
- Skip the full precompute in CI — it needs the OSM extract, which is too large to download on every push.

### ~~Automated OSM re-import and precompute [independent]~~

**What:** Scheduled refresh so the published map doesn't slowly drift out of date.

**How:**

- Script that downloads the latest Ireland + NI extract from Geofabrik (or equivalent), runs `--refresh-import` then `--precompute`, and publishes the resulting PMTiles.
- Runs on a cron — weekly or monthly depending on how much OSM churn matters at the scoring granularity.
- Emits a manifest with the extract date so the Phase 6 data-freshness indicator can pick it up.
- Tolerates failures without clobbering the last good build.

---

## Phase 1 — Service reality check

Every stop the model counts is assumed to exist in practice. Right now that assumption is wrong: derelict stations, routes cancelled during COVID that were never restored, school-only runs tagged as general transit, rural services that run on an informal schedule. Filtering phantom stops is a prerequisite for any transport scoring improvement, because there is no point tiering a bus stop by frequency if its effective frequency is zero.

Phase 1's outputs are used directly by Phase 3's transport scoring overhaul.

### ~~GTFS ingestion~~ [blocker]

**What:** Pull and normalize GTFS feeds from NTA (Republic) and Translink (Northern Ireland).

**How:**

- Download GTFS zips from the published feed URLs on a schedule.
- Parse `stops.txt`, `stop_times.txt`, `trips.txt`, `calendar.txt`, `calendar_dates.txt`, `routes.txt`.
- Load into PostGIS under a dedicated schema (`transit_raw` or similar).
- Keep `stop_id` as the join key; geocode stops by their lat/lon.
- Track the feed version so stale feeds don't silently poison scores.

### ~~OSM ↔ GTFS cross-reference~~ [superseded]

**What:** Match OSM `highway=bus_stop` / `railway=station` nodes to GTFS stops.

**Why this was dropped:** OSM Ireland coverage is ~12k transport stops versus ~29k in the GTFS feeds — a ~60% gap. With the majority of real stops absent from OSM, cross-referencing OSM nodes against GTFS is the wrong direction. The implementation went GTFS-first instead: transport scoring sources stops directly from GTFS, inherently excluding phantom and inactive stops without needing to join against incomplete OSM data. The standalone dataset therefore uses GTFS stop IDs as its primary key, not OSM node refs.

### ~~Phantom stop detection [blocker]~~

**What:** Flag stops with zero scheduled services in the last 30 days as inactive and exclude them from scoring.

**How:**

- Compute real service events per stop by joining `stop_times.txt` × `calendar.txt` × `calendar_dates.txt` over a rolling 30-day window.
- Binary flag: zero scheduled departures in the window → inactive.
- Store the flag on the GTFS stop in the derived transport layer.

**Open questions:**

- How strict is "zero"? A stop with one departure a month is technically active but practically not. The binary active/inactive call is enough for this phase; frequency-weighting is Phase 3's job.
- How to handle the cross-border case — a stop in NI matched against a Republic feed, or vice versa.

### ~~School-only route filtering~~

**What:** Routes that only run during term-time school hours shouldn't count as general transit access.

**How:**

- Detect school-only routes by their `calendar.txt` service pattern: only weekday mornings and afternoons, with obvious term-time gaps from `calendar_dates.txt`.
- Flag matching stops as school-only; exclude from general transit scoring.
- Optionally keep them in a separate "school runs" overlay — useful for parents, not for general livability.

### ~~Service desert overlay~~

**What:** Grid cells where nominal OSM transport features resolve to zero real weekly departures. Exposed as a dedicated map layer.

**How:**

- After phantom-stop detection, compute per-cell real-departure totals.
- Cells with at least one OSM stop but zero real departures get the service-desert flag.
- Render as a distinct overlay layer in the UI.

### Standalone reality dataset [hosted publishing pending Phase 8]

**What:** A downloadable GeoJSON of active vs inactive Irish transport stops, licensed ODbL, built to be cited independently of the livability map.

**How:**

- Export the GTFS-derived stops with: active/inactive status, last-departure date, route modes served.
- Publish on the hosted site as a download — blocked on Phase 8 hosting.
- Include a short README in the archive explaining the methodology and caveats. The caveats matter — the dataset should be defensible to journalists, researchers, and transport authorities, not just useful for scoring.

**Done so far:** ZIP export (GeoJSON + manifest + README) is generated locally to `cache/exports/` after each transit reality refresh. Publishing to the hosted site is Phase 8.

---

## Phase 2 — Scoring model v2

The v1 model counts features by presence. A corner shop scores the same as a supermarket, a 7 m² pocket park the same as Phoenix Park, a rural GP the same as a major hospital. v2 introduces sub-tiering, variety, distance decay, mode awareness, and land-use context.

### ~~Sub-tier shops~~

**What:** corner shop → regular shop → supermarket → mall / retail cluster.

**How:**

- Start from the `shop=*` tag tree:
  - `shop=convenience` and named chains (Spar, Centra, Londis, Gala, Mace) → corner shop.
  - `shop=supermarket` with a small footprint → regular shop.
  - `shop=supermarket` with a large footprint or `shop=wholesale` → supermarket.
  - `shop=mall`, or a dense retail cluster in a single building → mall / retail cluster.
- Footprint thresholds need calibration against the sanity fixture.
- The existing `CAPS = {"shops": 5, ...}` in `config.py` becomes a per-tier table rather than a single cap.

### ~~Sub-tier healthcare~~

**What:** pharmacy / GP → clinic / health centre → hospital → major hospital with A&E.

**How:**

- `amenity=pharmacy`, `amenity=doctors` → tier 1.
- `amenity=clinic`, `amenity=health_centre` → tier 2.
- `amenity=hospital` without `emergency=yes` → tier 3.
- `amenity=hospital` with `emergency=yes` → tier 4.
- Cross-reference with HSE facility lists for ROI when they're available as open data.

### ~~Sub-tier parks by area~~

**What:** Pocket park / playground (<0.5 ha) → neighbourhood park (0.5–5 ha) → district park (5–25 ha) → regional park / nature reserve (25+ ha).

**How:**

- Compute polygon area for each `leisure=park`, `leisure=nature_reserve`, `leisure=garden`, `leisure=playground`.
- Tier by area; score accordingly.
- A cell close to a 100 ha park scores significantly higher on parks than a cell close to a playground, even though both are "one park".
- Area thresholds calibrated against the sanity fixture — Phoenix Park should not dominate the entire model just because it's enormous.

### Variety signal

**What:** Count distinct clusters, not distinct tags. "3 services in 3 buildings" beats "3 services behind one counter."

**How:**

- Cluster amenities by physical proximity — DBSCAN-style on a small radius (~25 m).
- Count distinct clusters per category within the walk radius.
- A cell whose reachable amenities all fall into one cluster takes a variety penalty: the count is capped at the cluster count, not the feature count.

### Distance-decay scoring

**What:** Replace the binary in-range / out-of-range cutoff with a smooth decay curve.

**How:**

- Exponential or half-Gaussian decay over walk distance.
- Half-distance parameter per category — shops closer matter more than parks.
- Parameters calibrated against the sanity fixture.
- Interacts with the fixed 500 m walk radius currently in `config.py`. The radius becomes a search bound, not a scoring cutoff; features beyond it contribute zero, features inside it contribute per the decay curve.

### Mode-aware scoring [design-needed]

**What:** Score with walking, cycling, and transit-chained trips, not just a fixed 500 m walk radius.

**How:**

- Extend the Rust walkgraph helper to emit per-mode reach polygons (walk at 500 m / 1 km / 1.5 km, cycle at 3 km / 5 km).
- Score each mode separately and combine with weighted blending.
- Transit-chained reach requires Phase 1 + Phase 3 transit data before it's feasible.

**Open questions:**

- How to combine modes without over-rewarding places that are great for walking *and* cycling *and* transit. Likely a max-of with diminishing returns on the second-best mode, to prevent "good at everything" from dominating "good at one thing by a lot".

### Land-use context layer [design-needed]

**What:** Residential / commercial / industrial classification driving concentration penalties and green-buffer bonuses.

**How:**

- Rasterize `landuse=*` tags (residential, commercial, industrial, retail, farmland, forest) onto a fine grid.
- Use the neighbourhood composition around each cell to modulate other scores:
  - Industrial neighbours → penalty.
  - Residential plus green buffer → small bonus.
- The layer is also used by the Phase 4 over-concentration penalty and the Phase 4 conditional pitch noise.

**Open questions:**

- Bonus and penalty curves need a design note. Over-eager penalties will mislabel genuine mixed-use neighbourhoods as industrial hellholes.

---

## Phase 3 — Transport scoring overhaul

Depends entirely on Phase 1. Every bullet assumes phantom stops are filtered and GTFS departures are loaded.

### Departures per stop per day

**What:** Replace presence-of-stop with scheduled-departures-per-day as the transport feature weight.

**How:**

- Compute from `stop_times.txt` × `calendar.txt` for a representative weekday, Saturday, and Sunday.
- Store per-stop `(weekday_deps, saturday_deps, sunday_deps)`.
- Use the average as the primary signal and the minimum (usually Sunday) as a floor — a stop that only runs weekdays is worth less than one that runs every day.

### Mode tiering

**What:** Luas → rail → high-frequency bus → rural bus.

**How:**

- Base weight per mode, multiplied by the frequency factor from departures-per-stop.
- **Luas:** anywhere on the Green or Red line gets a significant bonus — high frequency, high capacity, high reliability.
- **Rail:** strong bonus for cells within walking distance of an active station.
- **High-frequency bus:** ≥4 departures per hour during the day.
- **Rural bus:** minimal weight even when technically present.

### Rail proximity sweet spot

**What:** Reward walking distance to a station, penalize direct adjacency to the tracks.

**How:**

- Distance-decay bonus centred ~300–500 m from the station — close enough to walk, far enough to avoid track noise.
- Overlaps with the Phase 4 railway-track noise penalty; the net effect is a sweet-spot curve with a rewarding plateau.

### Rural bus minimal weight

**What:** Cap rural bus contributions so a single low-frequency stop cannot match urban transit access.

**How:**

- Keep surviving rural stops in the scoring model to not completely zero out rural areas.
- Hard cap on their contribution, enforced at the category level.
- The filtering in Phase 1 already removes the ghost stops; this cap handles the ones that are real but infrequent.

---

## Phase 4 — Noise and nuisance penalty layer

All items here subtract from a cell's score. They are what makes dense-but-quiet suburbs score above noisy city centres with the same amenity count — the core thesis of the livability map.

### Railway track proximity

**What:** Penalty for cells directly adjacent to active railway tracks.

**How:**

- Buffer active `railway=rail` lines (exclude `railway=disused`, `railway=abandoned`) by 50–200 m.
- Penalty decays with distance.
- Only applies to tracks with actual scheduled service from Phase 1 GTFS — disused lines shouldn't penalize the areas next to them.

### Motorway and major-road noise

**What:** Penalty for cells directly adjacent to motorways and primary roads.

**How:**

- Buffer `highway=motorway`, `highway=trunk`, `highway=primary` by class-specific distances (motorway buffer > primary buffer).
- Penalty weighted by the road's speed class.
- Stacks additively with the over-concentration penalty below.

### Flight paths and airport proximity

**What:** Penalty for cells under flight paths or close to airport boundaries.

**How:**

- Static penalty for cells within a radius of any active airport (`aeroway=aerodrome` with scheduled commercial service).
- Flight-path overlay requires external data (IAA / NATS published approach paths). Start with airport-radius only; add flight paths when a data source is confirmed available under a compatible license.

### Over-concentration penalty [design-needed]

**What:** Too much nightlife, fast food, or retail relative to residential context becomes a penalty, not a bonus.

**How:**

- For each cell, compute density of `amenity=fast_food`, `amenity=bar`, `amenity=pub`, `amenity=nightclub`, `shop=*`.
- Compare against residential context from the Phase 2 land-use layer.
- High amenity density + low residential context = city-centre strip = penalty.
- High amenity density + high residential context = mixed-use neighbourhood = bonus.

**Open questions:**

- Thresholds and neighbourhood window size. Over-concentration is context-dependent and the model needs a written design note before implementation — this is the single easiest place to produce nonsense scores.

### Conditional pitch noise [design-needed]

**What:** Sports pitches only count as a noise source when they are lit (floodlit = evening use) or embedded in a dense residential context.

**How:**

- Filter `leisure=pitch` by `lit=yes` or by intersection with residential land use.
- Apply a small penalty to the immediately surrounding cells.

**Open questions:**

- What counts as "dense residential context" for the conditional. Probably reuses the Phase 2 land-use layer once it exists; the threshold is part of the same design note.

---

## Phase 5 — New categories and modifiers

Additive refinements to the scoring model. Each item is independent and can be picked up in any order, but they all benefit from Phase 2's core model being stable first — calibrating a modifier on top of an unstable base is wasted effort.

### Daily-convenience anchors

**What:** Add `amenity=drinking_water`, `amenity=parcel_locker`, `amenity=shelter` as convenience anchors.

**How:**

- New scoring category: `convenience_anchors` with a small cap.
- Counts as a quality-of-daily-life signal: presence is a small positive, absence is not a strong negative.

### Separate chemists from pharmacies

**What:** A chemist (Boots, McCabes) selling toiletries scores under daily convenience. A dispensing pharmacy scores under healthcare.

**How:**

- OSM tag split: `shop=chemist` → convenience; `amenity=pharmacy` → healthcare.
- The current v1 model mislabels chemists as healthcare, inflating scores on shopping streets.

### Night-usability modifier

**What:** Footpaths, crossings, and stops with lighting usable at night score higher than those without.

**How:**

- Filter OSM features by `lit=yes`.
- Apply as a modifier on walking ways and transport stops.
- Coverage is patchy — fall back neutral where the tag is missing, don't penalize the absence of a positive tag.

### Surface-quality modifier

**What:** Prefer paved over unpaved, well-surfaced over broken.

**How:**

- Use `surface=*` and `smoothness=*` on `highway=footway` and `highway=path`.
- Lower score for `surface=unpaved`, `surface=gravel`, `smoothness=bad`.
- Applies to walking reachability, not the final livability score directly.

### Shade and pleasantness modifier [design-needed]

**What:** Small bonus for walking ways near street trees or tree rows.

**How:**

- Use `natural=tree`, `natural=tree_row`, plus park canopy boundaries.
- Compute tree density within a short buffer of walking ways.

**Open questions:**

- "Pleasantness" is subjective and easy to game. A design note should define what the modifier actually rewards and what it deliberately ignores. Easy to turn into noise.

### Waterfront bonus [design-needed]

**What:** Paths and parks adjacent to water score a small bonus.

**How:**

- Buffer `natural=coastline`, `waterway=river`, `natural=water`.
- Bonus applied to parks and walkable ways within the buffer.

**Open questions:**

- Adjacency threshold and bonus magnitude. A canal path is different from a cliff-top sea walk; the modifier should acknowledge the difference.

### Community and public-realm anchors

**What:** Community centres, libraries, benches, picnic tables, public toilets as small bonuses.

**How:**

- `amenity=community_centre`, `amenity=library`, `amenity=bench`, `amenity=toilets`, `leisure=picnic_table`.
- Small bonus per feature, capped low — these are quality-of-life signals, not primary drivers.

### Dual-role nightlife features [design-needed]

**What:** Pubs, bars, and fast food that add value in moderation and penalize over-concentration.

**How:**

- Builds on the Phase 4 over-concentration penalty.
- At low-to-moderate density: small positive.
- At high density relative to residential context: negative.

**Open questions:**

- The moderation/concentration threshold is the whole question. This is the hardest calibration problem in the roadmap — the design note for it is shared with the Phase 4 over-concentration penalty.

---

## Phase 6 — UI

Improvements to the local frontend. Most of these are also prerequisites for the Phase 8 public launch — without per-cell breakdowns, layer toggles, and the rest of this phase, there is nothing meaningful to put on a public site.

### ~~Per-cell score breakdowns on click~~

**What:** Clicking a grid cell opens a panel showing the component scores (shops, transport, healthcare, parks, plus modifiers once they exist) and the top features driving each.

**How:**

- Precompute stores per-cell component scores, not just the combined score.
- The amenity features referenced in the breakdown come from the amenity layer already baked into the PMTiles archive.
- Frontend handles the click, queries the tile at the click location, and renders a side panel with the breakdown.
- Enables the weight sliders below - once components are stored separately, client-side reweighting is trivial.

**Done so far:**

- Coarse `grid` tiles now carry per-category raw counts, cluster counts, effective units, component scores, and `total_score` for popup inspection.
- `/api/inspect` returns the exact 50 m fine-surface breakdown for a clicked location, including component scores, raw counts, cluster counts, effective units, visible resolution, and land coverage.
- The frontend now opens a click popup using coarse tile properties when that is enough and falls back to `/api/inspect` for exact fine-surface details.
- This item is functionally done even though the UI is currently a popup rather than a dedicated side panel, and it does not yet list individual top amenity drivers.

### Layer toggles

**What:** Switch the map view between the combined score and any single category.

**How:**

- Each category is stored as a separate component score (shared infrastructure with per-cell breakdowns above).
- UI toggle group; swap the fill-color expression on the MapLibre layer.
- Combined view stays the default.

### Data freshness indicator

**What:** Visible label showing the OSM extract date currently in use.

**How:**

- Read from the precompute manifest written by the automated-refresh job in Phase 0.
- Surface in the site footer and in the score breakdown panel.

### Shareable permalinks

**What:** URL encodes viewport and weight sliders so a specific view can be shared or bookmarked.

**How:**

- Hash fragment (not query string) so it never hits the server.
- Encode lat/lon/zoom plus the active category layer plus, eventually, the weight vector.
- Restore on page load.

### User-adjustable weight sliders

**What:** Let users score for their own priorities rather than the built-in defaults.

**How:**

- Sliders for each category weight.
- Score recomputed in the browser from precomputed component scores — no server round-trip.
- Permalink encoding includes the weight vector.
- Saved to local storage so the user's preferred weights persist across sessions.

### Shortlist mode

**What:** Save multiple locations and view their breakdowns in one panel.

**How:**

- Pin-drop UI, local storage for persistence.
- Side panel lists all shortlisted locations with their component scores.
- Export as JSON for users who want to keep the list externally.

### Compare mode

**What:** Pin two locations side by side with their score breakdowns.

**How:**

- Two-up panel layout.
- Highlight differences in component scores with colour-coded bars.
- Share link encodes both pins.

### Isochrone overlay

**What:** Click anywhere, draw the reachable polygon at 5 / 10 / 15 minutes on foot.

**How:**

- The Rust walkgraph helper already computes reachability — extend it to return a polygon for a given origin plus time budget.
- Not precomputed — computed on demand, cached in the browser.
- Overlay rendered as a semi-transparent fill layer on top of the score grid.

### "Is this wrong?" feedback loop

**What:** Direct link to edit the relevant feature in OpenStreetMap. Turn user corrections into upstream contributions.

**How:**

- Every amenity in the breakdown panel gets an "edit on OSM" link.
- Deep-link into the iD editor: `https://www.openstreetmap.org/edit?node=<id>` or equivalent for ways and relations.
- This is the single best lever for improving the data the map depends on — a user who corrects an OSM tag improves every other OSM-derived project simultaneously.

---

## Phase 7 — External validation [design-needed]

### External dataset comparison

**What:** Compare the scoring model against an independent index (CSO deprivation indices or equivalent) as a secondary sanity check.

**How:**

- Load the external dataset at whatever granularity it's published (electoral divisions, small areas).
- Aggregate livability scores to the same geography.
- Compute correlation and visualize disagreements.
- Investigate clusters where the scoring model and the external dataset disagree — those are the places the model is either wrong or uncovering something the index isn't capturing.

**Open questions:**

- Which external dataset. CSO deprivation indices measure socioeconomic deprivation, not walkability or livability — they are a sanity bound, not ground truth. The phase deliberately waits until the model is mature enough for the comparison to be meaningful.
- Methodology and fit function. A written design note is required before this phase starts; running a correlation without a hypothesis produces a number that looks authoritative and means nothing.

---

## Phase 8 — Public launch

The hosted demo is the project's public moment, and it deliberately sits at the end. The intent is to ship it as a finished thing — the local pipeline works end-to-end, the scoring model has been through Phases 1–5, the UI has been through Phase 6, and there is a real story to tell. Until that point the project lives on GitHub and updates land there continuously, but the public-facing site exists once there is something worth landing on.

### Hosted static demo

**What:** A public site serving the precomputed PMTiles plus a stripped-down frontend. No backend, no database.

**How:**

- Build the frontend with `npm run build`.
- Upload `livability.pmtiles` to an object store with HTTP range-request support (Cloudflare R2, S3).
- Deploy `static/dist/` and the frontend HTML on any static host (GitHub Pages, Cloudflare Pages).
- MapLibre reads PMTiles directly over HTTP range requests — no tile server required.
- Long cache headers on the PMTiles since each precompute build is content-addressed via `PMTILES_SCHEMA_VERSION` and the build hash.

**Open questions:**

- Hosting budget. Ireland PMTiles is small; bandwidth is the only real cost. Cloudflare R2 has no egress fees and is the obvious default.

### Privacy promise [independent]

**What:** Documented statement on the hosted demo: no individual tracking, no query logging, no behavioural profiling, only anonymous aggregate visit counts.

**How:**

- Visible link in the site footer.
- Implementation: count visits at the edge (Cloudflare, or equivalent), not per-user. No cookies, no fingerprinting.
- All score queries run client-side in the browser. The hosted server never sees where the user clicked.

### Methodology page

**What:** Plain-language "how the score is computed" page on the public site. Lists what the score measures, what it deliberately does not, and the known limitations.

**Why:** When non-technical visitors see a coloured heatmap, they treat it as fact. Without an accessible methodology page, anyone whose neighbourhood scores red can reasonably assume the map is wrong, biased, or made up. The page is what you point at when someone asks "what does this number actually mean" — trust infrastructure for the launch, not engineering work.

**How:**

- One page on the hosted site, plain English, no jargon.
- Sections: what the score measures, what it doesn't, the data sources, the known limitations (lifted from the README's Known Limitations section), and the licence.
- Linked from the site footer alongside the privacy promise.
- Updated every time the scoring model changes — the visible scoring model version below makes "which version is this page describing" answerable.

### Visible scoring model version

**What:** Small label in the page footer showing the active scoring model version (e.g. `scoring model v1.3`) alongside the OSM extract date.

**Why:** The scoring model will keep changing after launch. Without a visible version number, screenshots from before and after a model change look identical, and people will assume the map is unreliable or that the project flip-flopped. The label makes "which version of the model is this" instantly answerable from any screenshot.

**How:**

- Add a `SCORING_MODEL_VERSION` constant to `config.py`. Bump it on any user-visible scoring change.
- Write the version into the precompute manifest alongside the OSM extract date.
- Surface in the site footer next to the Phase 6 data freshness indicator — same plumbing, same panel.
