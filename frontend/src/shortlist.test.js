import assert from "node:assert/strict";
import {
  SHORTLIST_MAX_ITEMS,
  comparisonRows,
  createShortlistItem,
  loadShortlist,
  moveShortlistItem,
  refreshShortlistItems,
  renameShortlistItem,
  saveShortlist,
  shortlistExport,
  toggleComparisonSelection,
  totalDeltaFromPrevious,
  upsertShortlistItem
} from "./shortlist.js";

function payload(cell = "50:1:2", total = 50) {
  const mode = { total_score: total, network_status: "ok", component_scores: { shops: 20, transport: 15, healthcare: 10, parks: 5 }, penalties: { road_proximity: 2 }, bonuses: { green_buffer: 1 } };
  return { scoring_model: "daily-access-v4", valid_land: true, cell_id: cell, lat: 53.3, lon: -6.2, resolution_m: 50, modes: { walk: mode, bike: { ...mode, total_score: total + 5 }, mode_aware: { ...mode, total_score: total + 3 } } };
}

const memory = new Map();
const storage = { getItem: (key) => memory.get(key) || null, setItem: (key, value) => memory.set(key, value) };
const first = createShortlistItem(payload(), { id: "one", buildKey: "build-a", position: 1, now: "2026-11-27T00:00:00Z" });
assert.equal(first.snapshot.modes.bike.total, 55);
saveShortlist(storage, [first, { broken: true }]);
assert.equal(loadShortlist(storage).length, 1);
assert.equal(renameShortlistItem([first], "one", "  Home  ")[0].label, "Home");
assert.equal(renameShortlistItem([first], "one", "x".repeat(100))[0].label.length, 60);
assert.equal(moveShortlistItem([first, { ...first, id: "two" }], "two", -1)[0].id, "two");

const duplicate = createShortlistItem(payload(), { id: "duplicate", buildKey: "build-a", position: 2 });
assert.equal(upsertShortlistItem([first], duplicate).items[0].id, "one");
const full = Array.from({ length: SHORTLIST_MAX_ITEMS }, (_, index) => ({ ...first, id: String(index), cellId: "cell-" + index }));
assert.equal(upsertShortlistItem(full, { ...first, id: "overflow", cellId: "new" }).full, true);

const refreshed = await refreshShortlistItems([first, { ...first, id: "bad", cellId: "bad", lon: 99 }], "build-b", async ({ lon }) => {
  if (lon === 99) throw new Error("offline");
  return payload("50:1:2", 57);
});
assert.equal(refreshed[0].buildKey, "build-b");
assert.equal(totalDeltaFromPrevious(refreshed[0], "walk"), 7);
assert.equal(refreshed[1].status, "stale");
const stale = await refreshShortlistItems([{ ...first, lon: 99 }], "build-b", async () => { throw new Error("offline"); });
assert.equal(stale[0].status, "stale");

const rows = comparisonRows(first.snapshot, refreshed[0].snapshot, "walk");
assert.equal(rows.find((row) => row.key === "total").delta, 7);
assert.equal(rows.find((row) => row.key === "road_proximity").a, -2);
const exported = JSON.stringify(shortlistExport([first], "2026-11-27T00:00:00Z"));
assert.doesNotMatch(exported, /score_dir|shell_dir|graph_dir/);
let selected = toggleComparisonSelection(new Set(), "one", true).selected;
selected = toggleComparisonSelection(selected, "two", true).selected;
const third = toggleComparisonSelection(selected, "three", true);
assert.equal(third.blocked, true);
assert.deepEqual(Array.from(third.selected), ["one", "two"]);

console.log("frontend shortlist checks passed");
