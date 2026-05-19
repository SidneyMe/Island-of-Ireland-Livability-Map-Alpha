import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const mainJs = fs.readFileSync(path.join(here, "main.js"), "utf8");

assert.ok(
  mainJs.includes('overlayTitle.textContent = "Show road Lden proxy";'),
  "noise toggle label must use road Lden proxy wording"
);
assert.ok(
  mainJs.includes('overlaySubtitle.textContent = "Official-derived grid proxy, not measured dB";'),
  "noise subtitle must state grid proxy / not measured wording"
);
assert.ok(
  mainJs.includes("Approximate road Lden proxy from official-derived noise grid."),
  "noise caveat text must be present"
);
assert.equal(
  mainJs.includes("Show official noise contours"),
  false,
  "noise UI must not use official contour wording"
);
assert.equal(
  mainJs.includes("Lden and Lnight polygons"),
  false,
  "noise UI must not mention Lnight polygons in phase A"
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
