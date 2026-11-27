# Daily Access v4 context model

Implemented 2026-11-27. These are explicit starting modelling parameters,
not measured noise levels. All numerical settings live in
`config.DAILY_ACCESS_CONTEXT_PARAMS` and are part of the score hash.

## Final score

Four category components retain their existing meaning. Add station access
and residential green bonuses, subtract existing railway/road penalties and
the three context penalties, then clamp to 0–100. Context penalties sum to
at most 20 points; if necessary scale all three proportionally. A cell
without a routing network has total zero and no bonuses.

Use S(x)=3t²−2t³, t=clamp(x,0,1), for every smooth transition.

## Station access

GTFS public, active rail/tram stops only; no OSM fallback. Find the nearest
reachable rail and tram stop separately using the WALK graph, connectors,
and exact same-edge routing. Resolve ties by stable feed-scoped source id.
Choose the nearest before applying the nonmonotonic curve: a more distant
station must not replace the nearest simply to get a larger bonus.

The curve is zero through 100 m, rises to one at 300 m, stays one through
500 m, falls to zero at 1200 m (rail) or 1000 m (tram). Multiply by existing
GTFS tier quality, and 3 points (rail) or 2 points (tram). Multiply again by
S((distance_to_active_corridor−50)/150). Take the maximum of rail/tram,
never their sum. Missing corridor geometry disables bonuses from that
feed with a published warning. The existing track penalty still applies.

Walking bonuses are computed once in canonical walk score shards and
reused by bike and mode-aware. They do not expand with bike speed.

## Land use and green buffer

Use EPSG:2157 circles, clipped to the full land mask. Input queries include
a 600 m buffer: pitch-centred 300 m context can extend beyond a cell's
300 m venue window. Green geometry comes from eligible v4 public green sites.

In the 300 m window, repair geometries, union within classes, and assign
overlaps in order: industrial, retail, commercial, residential, farmland,
forest. R and I are residential and industrial shares of LAND area, not
shares of known mapped land. C is coverage; Q=S((C−0.3)/0.3).

Industrial penalty = 8 Q S((I−0.1)/0.4).

In a 200 m window let G be the unioned public green area divided by land
area. Green bonus = 2 Q S((R−0.2)/0.3) S(G/0.2) walk_green_access.
Unknown land use and inaccessible green space cannot produce a bonus.

## Venue concentration

An independent osm2pgsql `nuisance` raw table preserves shops, pubs, bars,
nightclubs, fast food and pitches without changing amenity classification.
Exclude non-operational lifecycle objects and excluded v4 shop kinds.
Reuse evidence-based, non-chaining v4 deduplication: distinct businesses
in the same building remain distinct. A venue with multiple roles uses
its strongest role (nightlife wins ties).

Within 300 m, density is per km² of land. Nightclub weight 3, pub/bar 1;
fast food 2, eligible shop 1. Let these weighted densities be N and T.

- R_eff = Q R + (1−Q) 0.5.
- Nightlife = 8 S((N−10)/30) (0.5+0.5 R_eff).
- Retail = 4 Q S((T−100)/200) (1−R).
- Concentration = nightlife + retail, at most 12 points.

Residential context increases nightlife nuisance and reduces the retail
penalty. One pub or a few ordinary shops have zero concentration penalty.

## Pitches

An operational pitch qualifies when lit=yes, or when its representative
point's 300 m land-use window has Q=1 and R≥0.4. Explicit lit=no does not
cancel the residential condition. Unknown lighting/context is neutral.
Polygon distance is zero inside and distance to the boundary outside;
point sources use point distance. Full penalty through 25 m, smooth fade
to zero at 150 m. Maximum 4 points when lit, otherwise 2 for qualifying
residential context. Take the strongest pitch; adjacent fields do not sum.

## Artifact and release contract

Score schema lists four components, five penalties and two bonuses by
name. Applied values are positive in shards/inspect; PMTiles penalty
contributions have a negative sign. Runtime averages stored channels by
land area. Mode-aware applies the shared effects once, after access blend.
Diagnostics include coverage, shares, densities, chosen station labels and
walking distances, qualifying pitch source/distance/reason. Official noise
PMTiles remain display-only. V3 runtime scoring is unchanged.

Importer/reach/score/tile versions invalidate incompatible artifacts.
Graph keys depend on extract/geometry, not context curves. Curve and
context-threshold changes invalidate scores and rendering; station search
bound changes also require reach invalidation. New builds fail closed on
missing named channels. Keep the previous active build until candidate
validation and PMTiles bake succeed.

Run targeted Python/Rust/frontend tests, build the test profile without
publishing, run the pinned shard verifier, and generate the read-only
`scripts/daily_access_context_report.py --shell-dir ... --score-dir ...`
report before publication. Its baseline is the same-input counterfactual
without the new modifiers, not a claim about an older OSM snapshot.
