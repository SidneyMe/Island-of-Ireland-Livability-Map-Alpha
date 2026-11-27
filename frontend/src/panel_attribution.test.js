import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { LIVABILITY_ATTRIBUTION, NOISE_ATTRIBUTION } from "./runtime_contract.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const main = fs.readFileSync(path.join(here, "main.js"), "utf8");
const css = fs.readFileSync(path.join(here, "..", "..", "static", "app.css"), "utf8");
const html = fs.readFileSync(path.join(here, "..", "..", "static", "index.html"), "utf8");

assert.match(html, /class="panel-header-actions"[\s\S]*id="panel-toggle"/);
assert.doesNotMatch(html, /id="panel-toggle"[\s\S]*<\/button>[\s\S]*<\/div>[\s\S]*<aside id="control-panel"/);
assert.match(main, /elements\.panelToggle\.textContent = state\.panelHidden \? "›" : "‹"/);
assert.match(css, /\.maplibregl-ctrl-bottom-left[\s\S]*transition: left 180ms ease/);
assert.match(css, /body\.panel-hidden #control-panel[\s\S]*background: transparent;[\s\S]*pointer-events: none;/);
assert.match(css, /body\.panel-hidden #panel-toggle[\s\S]*pointer-events: auto;/);
assert.match(css, /body\.panel-hidden \.maplibregl-ctrl-bottom-left[\s\S]*left: 78px/);
assert.match(css, /\.maplibregl-ctrl-attrib\.maplibregl-compact-show \.maplibregl-ctrl-attrib-inner[\s\S]*display: flex !important/);
assert.match(css, /\.maplibregl-ctrl-attrib-button[\s\S]*left: auto !important;[\s\S]*right: 0 !important;/);
assert.match(css, /\.maplibregl-ctrl-attrib-button[\s\S]*background-repeat: no-repeat !important;/);
assert.match(css, /\.maplibregl-ctrl-attrib\.maplibregl-compact-show[\s\S]*padding: 8px 38px 8px 9px !important;/);
assert.match(css, /\.maplibregl-ctrl-attrib\.maplibregl-compact:not\(\.maplibregl-compact-show\)[\s\S]*width: 28px !important;/);
assert.match(css, /body\.panel-hidden \.maplibregl-ctrl-bottom-left[\s\S]*left: 14px !important;[\s\S]*bottom: 96px !important;/);
assert.match(main, /function ensurePanelHeaderActions\(\)[\s\S]*actions\.appendChild\(elements\.panelToggle\)/);
assert.match(main, /ensurePanelHeaderActions\(\);\s*fetchRuntimePayload\(\)/);
assert.match(LIVABILITY_ATTRIBUTION, /class="map-attribution-item"/);
assert.match(NOISE_ATTRIBUTION, /class="map-attribution-item"/);

console.log("frontend panel and attribution layout checks passed");
