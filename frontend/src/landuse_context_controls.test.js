import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const mainJs = fs.readFileSync(path.join(here, "main.js"), "utf8");

assert.ok(
  mainJs.includes('overlayTitle.textContent = "Show land-use context";'),
  "land-use toggle label must be present"
);
assert.ok(
  mainJs.includes('overlaySubtitle.textContent = "Phase 2 context overlay (z" + minZoom + "-z" + maxZoom + ")";'),
  "land-use subtitle must describe the zoom range"
);
assert.ok(
  mainJs.includes('const caveat = "Phase 2 land-use context overlay. Visible at z" + minZoom + "-z" + maxZoom + ". Residential, commercial, industrial, retail, farmland, forest. Not yet used in scoring.";'),
  "land-use note must explain the overlay and its scope"
);
assert.ok(
  mainJs.includes('landuseControls: document.getElementById("landuse-controls")'),
  "land-use controls container must exist"
);
assert.ok(
  mainJs.includes('landuseNote: document.getElementById("landuse-note")'),
  "land-use note element must be wired"
);
assert.ok(
  mainJs.includes("LANDUSE_CONTEXT_CLASS_COLORS"),
  "land-use controls must use the map layer color palette"
);
assert.ok(
  mainJs.includes('swatch.className = "landuse-class-swatch";'),
  "land-use class rows must show a color swatch"
);
assert.ok(
  mainJs.includes("input.style.accentColor = color;"),
  "land-use checkboxes must reuse the class color"
);

console.log("frontend land-use control checks passed");
