import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const mainJs = fs.readFileSync(path.join(here, "main.js"), "utf8");
const noiseFiltersJs = fs.readFileSync(path.join(here, "noise_filters.js"), "utf8");

assert.ok(
  mainJs.includes('overlayTitle.textContent = "Show noise overlay";'),
  "noise toggle label must use noise overlay wording"
);
assert.ok(
  mainJs.includes('overlaySubtitle.textContent = "Official-derived transport/industry noise overlay";'),
  "noise subtitle must state transport/industry overlay wording"
);
assert.ok(
  mainJs.includes("Official-derived noise overlay. Road/rail use grid proxy; airport/industry use resolved official noise polygons. Not measured point noise."),
  "noise caveat text must be present"
);
assert.equal(
  mainJs.includes("Show official contours"),
  false,
  "noise UI must not use official contour wording"
);
assert.equal(
  mainJs.includes("measured dB"),
  false,
  "noise UI must not claim measured dB"
);
assert.equal(
  mainJs.includes("industry"),
  true,
  "noise UI must mention industry support"
);
assert.equal(
  mainJs.includes("input.name = \"noise-metric\""),
  true,
  "noise metric selector inputs must be present in control builder"
);
assert.equal(
  noiseFiltersJs.includes('Lden: "Day-evening-night"'),
  true,
  "noise metric selector must include Lden label"
);
assert.equal(
  noiseFiltersJs.includes('Lnight: "Night"'),
  true,
  "noise metric selector must include Lnight label"
);
assert.ok(
  mainJs.includes('opacityInput.id = "noise-opacity";'),
  "noise opacity slider id must exist"
);
assert.ok(
  mainJs.includes('opacityInput.min = "0";') &&
    mainJs.includes('opacityInput.max = "1";') &&
    mainJs.includes('opacityInput.step = "0.05";'),
  "noise opacity slider range must be 0..1 with step 0.05"
);
assert.ok(
  mainJs.includes('state.map.setPaintProperty("noise-proxy-fill", "fill-opacity", state.noiseOpacity);'),
  "noise opacity must map to fill-opacity paint property"
);
assert.ok(
  mainJs.includes("state.noiseVisible = false;"),
  "noise layer must be hidden by default"
);
assert.ok(
  mainJs.includes('state.map.setLayoutProperty(\n    "noise-proxy-fill",\n    "visibility",\n    state.noiseVisible ? "visible" : "none"\n  );'),
  "noise toggle must control layer visibility"
);

console.log("frontend noise proxy control checks passed");
