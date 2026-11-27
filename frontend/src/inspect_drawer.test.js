import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const main = fs.readFileSync(path.join(here, "main.js"), "utf8");
const css = fs.readFileSync(path.join(here, "..", "..", "static", "app.css"), "utf8");
const html = fs.readFileSync(path.join(here, "..", "..", "static", "index.html"), "utf8");

assert.match(html, /id="inspect-drawer" class="inspect-drawer"/);
assert.match(html, /id="inspect-content"/);
assert.match(html, /id="inspect-close"/);
assert.match(main, /function showGridPopup\(lngLat, html, payload = null\)[\s\S]*elements\.inspectDrawer\.hidden = false/);
assert.match(main, /function closeInspectDrawer\(options = \{\}\)/);
assert.match(main, /function ensureInspectDrawerElements\(\)/);
assert.match(main, /\(document\.getElementById\("app"\) \|\| document\.body\)\.appendChild\(drawer\)/);
assert.match(main, /elements\.inspectContent\.innerHTML = html/);
assert.match(main, /state\.inspectOpen && event\.key === "Escape"/);
assert.match(main, /id: "linked-cell-highlight-halo"/);
assert.match(main, /highlightRenderedLinkedCell\(event\.lngLat, clickAction\.features\[0\] \|\| null\)/);
assert.match(main, /function closeInspectDrawer[\s\S]*clearLinkedCellHighlight\(\)/);
assert.match(css, /\.inspect-drawer[\s\S]*width: min\(460px, calc\(100vw - 414px\)\)/);
assert.match(css, /@media \(max-width: 820px\)[\s\S]*\.inspect-drawer[\s\S]*max-height: 72vh/);

console.log("frontend cell inspect drawer checks passed");
