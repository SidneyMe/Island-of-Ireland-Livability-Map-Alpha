# Self-hosted basemap

The app no longer uses CARTO or any other hosted map resource at runtime. The
background map is a local Protomaps vector PMTiles archive, with local sprites
and complete glyph ranges for each referenced font stack.

## Build

Install Docker Desktop and make sure `docker compose version` succeeds. Place
the Ireland + Northern Ireland OSM PBF at:

```text
osm/ireland-and-northern-ireland-latest.osm.pbf
```

Build (or rebuild when the PBF or pinned inputs change):

```text
python main.py basemap
```

The output is `.livability_cache/basemap/basemap.pmtiles`. The normal
`python main.py import` path checks this artifact after a successful import and
rebuilds it only when its manifest is stale. Use `python main.py basemap --force`
for an explicit rebuild.

The Planetiler JVM heap defaults to 5 GB (`-Xmx5g` via `JAVA_TOOL_OPTIONS` in
`basemap/docker-compose.yml`) because the JVM's own default, a quarter of the
Docker VM, is about 2 GB on an 8 GB Docker Desktop VM and runs out of heap in
the OSM stage. Override it with `BASEMAP_JAVA_HEAP` (for example `6g`), and
keep it below the memory Docker Desktop gives its VM.

Run locally with a missing basemap only when inspecting the rest of the stack:

```text
python main.py serve
```

For deployment, require all local map artifacts before startup:

```text
python main.py serve --deployment
```

Copy `.livability_cache/basemap/basemap.pmtiles` and `static/basemap/` with the
application deployment. PMTiles requests require byte-range support.

## Pins and provenance

The builder and exported style are both locked to Protomaps basemaps commit
`a50c699adc60a45c899971b1e11275e61f13bfbf` (tile schema v4). Font and sprite
assets are independently locked to basemaps-assets commit
`028c18f713baecad011301ff7a69acc39bcc2ae7`. The cache manifest records these
pins, the PBF fingerprint, ancillary input checksums, the Docker image ID, and
the PMTiles and vendored-asset checksums.

Observed production build (2026-09-25, Geofabrik extract of 2026-09-25, Docker
Desktop VM with 8 GB / 16 CPUs, `-Xmx5g`): 21m54s once ancillary inputs are
cached, a 567 MB `basemap.pmtiles` (1.8 GB of intermediate features), and 2.7 GB
of cached ancillary downloads under `.livability_cache/basemap/work/data`
(OSM water and land polygons are about 0.9 GB each; the first run spends most
of its time downloading them). The default 2 GB JVM heap failed in the OSM
stage with `OutOfMemoryError`.

CI builds the documented `--area=monaco` Docker fixture to validate the pinned
profile path. It does not build, and is not evidence for, the Ireland/NI
production archive.

## Data and attribution

- [OpenStreetMap](https://www.openstreetmap.org/copyright) and the OSM-derived
  coastline/water polygons are ODbL; attribution remains visible in the map.
- [Natural Earth](https://www.naturalearthdata.com/) is public domain.
- [Daylight Landcover](https://github.com/daylightmap/distribution) is derived
  from ESA WorldCover and requires CC-BY attribution.
- The light-style POI icons derive from [Mapzen Tangram icons](https://github.com/tangrams/icons), MIT licensed.
- The basemap profile is [Protomaps Basemaps](https://github.com/protomaps/basemaps),
  built with [Planetiler](https://github.com/onthegomap/planetiler); glyph and
  sprite assets come from [Protomaps basemaps-assets](https://github.com/protomaps/basemaps-assets).

All glyph PBF ranges (256 range files each) for every font family referenced by
the exported style are vendored: Noto Sans Regular, Medium, Italic, and the
upstream Devanagari fallback. MapLibre chooses a glyph range at runtime, so
reducing these assets to currently observed Irish text is unsafe.
