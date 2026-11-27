import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const main = fs.readFileSync(path.join(here, "main.js"), "utf8");
const css = fs.readFileSync(path.join(here, "..", "..", "static", "app.css"), "utf8");

assert.match(main, /function highlightLinkedCell\(lngLat\)/);
assert.match(main, /new maplibregl\.Marker/);
assert.match(main, /zoom: Math\.max\(state\.map\.getZoom\(\), 16\)/);
assert.match(main, /else if \(initialInspect\)/);
assert.match(main, /queryRenderedFeatures\(point, \{ layers: \[layerId\] \}\)/);
assert.match(main, /cellGeometryCenter\(geometry\)[\s\S]*linkedCellMarker\.setLngLat\(center\)/);
assert.match(main, /id: "linked-cell-highlight-line"/);
assert.match(main, /state\.map\.on\("click"[\s\S]*clearLinkedCellHighlight\(\)/);
assert.match(css, /@keyframes permalink-cell-pulse/);
assert.match(css, /\.permalink-cell-highlight-core[\s\S]*rotate\(45deg\)/);
assert.match(css, /prefers-reduced-motion: reduce/);

console.log("frontend permalink highlight checks passed");
