export const SHORTLIST_STORAGE_KEY = "livability-shortlist-v1";
export const SHORTLIST_SCHEMA_VERSION = 1;
export const SHORTLIST_MAX_ITEMS = 20;
export const SHORTLIST_LABEL_MAX_LENGTH = 60;

const MODES = ["walk", "bike", "mode_aware"];
const COMPONENTS = ["shops", "transport", "healthcare", "parks"];
const PENALTIES = ["railway_proximity", "road_proximity", "industrial_context", "over_concentration", "pitch_noise"];
const BONUSES = ["rail_station_access", "green_buffer"];

function finiteOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function valueMap(source, keys) {
  const values = source && typeof source === "object" ? source : {};
  return Object.fromEntries(keys.map(function (key) {
    return [key, finiteOrNull(values[key])];
  }));
}

function normalizeMode(values) {
  if (!values || typeof values !== "object") return null;
  return {
    total: finiteOrNull(values.total_score),
    networkStatus: String(values.network_status || "unknown"),
    components: valueMap(values.component_scores, COMPONENTS),
    penalties: valueMap(values.penalties, PENALTIES),
    bonuses: valueMap(values.bonuses, BONUSES)
  };
}

export function normalizeInspectSnapshot(payload, buildKey) {
  if (!payload || payload.scoring_model !== "daily-access-v4" || !payload.valid_land) return null;
  const lat = finiteOrNull(payload.lat);
  const lon = finiteOrNull(payload.lon);
  const cellId = String(payload.cell_id || "").trim();
  if (lat === null || lon === null || !cellId) return null;
  const declaredModes = payload.modes && typeof payload.modes === "object" ? payload.modes : {};
  const modes = {};
  MODES.forEach(function (mode) {
    const values = mode === "walk" && !declaredModes.walk ? payload : declaredModes[mode];
    const normalized = normalizeMode(values);
    if (normalized) modes[mode] = normalized;
  });
  if (!modes.walk) return null;
  return {
    buildKey: String(buildKey || ""),
    scoringModel: "daily-access-v4",
    scoringModelVersion: payload.scoring_model_version ?? null,
    cellId,
    lat,
    lon,
    resolutionM: Number(payload.resolution_m || 50),
    modes
  };
}

function normalizeStoredSnapshot(value) {
  if (!value || value.scoringModel !== "daily-access-v4") return null;
  const lat = finiteOrNull(value.lat);
  const lon = finiteOrNull(value.lon);
  const cellId = String(value.cellId || "").trim();
  if (!cellId || lat === null || lon === null || !value.modes || typeof value.modes !== "object") return null;
  const modes = {};
  MODES.forEach(function (mode) {
    const stored = value.modes[mode];
    if (!stored || typeof stored !== "object") return;
    modes[mode] = {
      total: finiteOrNull(stored.total),
      networkStatus: String(stored.networkStatus || "unknown"),
      components: valueMap(stored.components, COMPONENTS),
      penalties: valueMap(stored.penalties, PENALTIES),
      bonuses: valueMap(stored.bonuses, BONUSES)
    };
  });
  if (!modes.walk) return null;
  return {
    buildKey: String(value.buildKey || ""),
    scoringModel: "daily-access-v4",
    scoringModelVersion: value.scoringModelVersion ?? null,
    cellId,
    lat,
    lon,
    resolutionM: Number(value.resolutionM || 50),
    modes
  };
}

function normalizeLabel(value, fallback) {
  const text = String(value || "").trim().slice(0, SHORTLIST_LABEL_MAX_LENGTH);
  return text || fallback;
}

function normalizeItem(value, index) {
  if (!value || typeof value !== "object") return null;
  const snapshot = normalizeStoredSnapshot(value.snapshot);
  if (!snapshot) return null;
  const id = String(value.id || "").trim();
  if (!id) return null;
  const previous = normalizeStoredSnapshot(value.previousSnapshot);
  const allowedStatus = new Set(["current", "refreshing", "stale", "error"]);
  return {
    id,
    label: normalizeLabel(value.label, "Location " + (index + 1)),
    lat: snapshot.lat,
    lon: snapshot.lon,
    cellId: snapshot.cellId,
    savedAt: String(value.savedAt || ""),
    buildKey: String(value.buildKey || snapshot.buildKey || ""),
    snapshot,
    previousSnapshot: previous,
    status: allowedStatus.has(value.status) ? value.status : "current",
    lastError: value.lastError ? String(value.lastError) : null
  };
}

export function loadShortlist(storage) {
  try {
    const parsed = JSON.parse(storage.getItem(SHORTLIST_STORAGE_KEY) || "null");
    if (!parsed || parsed.version !== SHORTLIST_SCHEMA_VERSION || !Array.isArray(parsed.items)) return [];
    return parsed.items.map(normalizeItem).filter(Boolean).slice(0, SHORTLIST_MAX_ITEMS);
  } catch (error) {
    return [];
  }
}

export function saveShortlist(storage, items) {
  storage.setItem(SHORTLIST_STORAGE_KEY, JSON.stringify({
    version: SHORTLIST_SCHEMA_VERSION,
    items: items.slice(0, SHORTLIST_MAX_ITEMS)
  }));
}

export function createShortlistItem(payload, options = {}) {
  const snapshot = normalizeInspectSnapshot(payload, options.buildKey);
  if (!snapshot) return null;
  return {
    id: String(options.id || ""),
    label: normalizeLabel(options.label, "Location " + Number(options.position || 1)),
    lat: snapshot.lat,
    lon: snapshot.lon,
    cellId: snapshot.cellId,
    savedAt: String(options.now || new Date().toISOString()),
    buildKey: snapshot.buildKey,
    snapshot,
    previousSnapshot: null,
    status: "current",
    lastError: null
  };
}

export function upsertShortlistItem(items, item) {
  if (!item) return { items: items.slice(), index: -1, added: false, full: false };
  const existing = items.findIndex(function (entry) { return entry.cellId === item.cellId; });
  if (existing >= 0) {
    const next = items.slice();
    next[existing] = { ...item, id: next[existing].id, label: next[existing].label, savedAt: next[existing].savedAt };
    return { items: next, index: existing, added: false, full: false };
  }
  if (items.length >= SHORTLIST_MAX_ITEMS) {
    return { items: items.slice(), index: -1, added: false, full: true };
  }
  return { items: [...items, item], index: items.length, added: true, full: false };
}

export function renameShortlistItem(items, id, label) {
  return items.map(function (item, index) {
    return item.id === id ? { ...item, label: normalizeLabel(label, "Location " + (index + 1)) } : item;
  });
}

export function removeShortlistItem(items, id) {
  return items.filter(function (item) { return item.id !== id; });
}

export function moveShortlistItem(items, id, direction) {
  const index = items.findIndex(function (item) { return item.id === id; });
  const target = index + Number(direction);
  if (index < 0 || target < 0 || target >= items.length) return items.slice();
  const next = items.slice();
  [next[index], next[target]] = [next[target], next[index]];
  return next;
}

export function toggleComparisonSelection(selectedIds, id, checked) {
  const next = new Set(selectedIds);
  if (!checked) {
    next.delete(id);
    return { selected: next, blocked: false };
  }
  if (!next.has(id) && next.size >= 2) return { selected: next, blocked: true };
  next.add(id);
  return { selected: next, blocked: false };
}

export function refreshedShortlistItem(item, payload, buildKey) {
  const snapshot = normalizeInspectSnapshot(payload, buildKey);
  if (!snapshot) return { ...item, status: "error", lastError: "Invalid Daily Access v4 inspect response" };
  const buildChanged = item.snapshot.buildKey !== snapshot.buildKey;
  return {
    ...item,
    lat: snapshot.lat,
    lon: snapshot.lon,
    cellId: snapshot.cellId,
    buildKey: snapshot.buildKey,
    previousSnapshot: buildChanged ? item.snapshot : item.previousSnapshot,
    snapshot,
    status: "current",
    lastError: null
  };
}

export async function refreshShortlistItems(items, buildKey, fetcher, options = {}) {
  const next = items.map(function (item) {
    return item.buildKey === buildKey ? { ...item, status: "current", lastError: null } : { ...item, status: "refreshing", lastError: null };
  });
  const indices = next.map(function (item, index) { return item.status === "refreshing" ? index : -1; }).filter(function (index) { return index >= 0; });
  let cursor = 0;
  async function worker() {
    while (cursor < indices.length) {
      const index = indices[cursor++];
      const item = next[index];
      if (typeof options.onUpdate === "function") options.onUpdate(next.slice());
      try {
        const payload = await fetcher({ lat: item.lat, lon: item.lon });
        next[index] = refreshedShortlistItem(item, payload, buildKey);
      } catch (error) {
        next[index] = { ...item, status: "stale", lastError: String(error && error.message || error || "Refresh failed") };
      }
      if (typeof options.onUpdate === "function") options.onUpdate(next.slice());
    }
  }
  const concurrency = Math.max(1, Math.min(3, Number(options.concurrency || 3)));
  await Promise.all(Array.from({ length: Math.min(concurrency, indices.length) }, worker));
  return next;
}

export function totalDeltaFromPrevious(item, mode) {
  const current = item && item.snapshot && item.snapshot.modes[mode];
  const previous = item && item.previousSnapshot && item.previousSnapshot.modes[mode];
  if (!current || !previous || current.total === null || previous.total === null) return null;
  return current.total - previous.total;
}

export function comparisonRows(first, second, mode) {
  const a = first && first.modes && first.modes[mode];
  const b = second && second.modes && second.modes[mode];
  const rows = [{ key: "total", label: "Total", kind: "total", a: a && a.total, b: b && b.total }];
  COMPONENTS.forEach(function (key) {
    rows.push({ key, label: key[0].toUpperCase() + key.slice(1), kind: "positive", a: a && a.components[key], b: b && b.components[key] });
  });
  BONUSES.forEach(function (key) {
    rows.push({ key, label: key === "rail_station_access" ? "Rail / tram access" : "Green buffer", kind: "positive", a: a && a.bonuses[key], b: b && b.bonuses[key] });
  });
  PENALTIES.forEach(function (key) {
    const label = key.split("_").map(function (part) { return part[0].toUpperCase() + part.slice(1); }).join(" ");
    const av = a && a.penalties[key];
    const bv = b && b.penalties[key];
    rows.push({ key, label, kind: "penalty", a: av === null || av === undefined ? null : -Math.abs(av), b: bv === null || bv === undefined ? null : -Math.abs(bv) });
  });
  return rows.map(function (row) {
    const av = finiteOrNull(row.a);
    const bv = finiteOrNull(row.b);
    return { ...row, a: av, b: bv, delta: av === null || bv === null ? null : bv - av };
  });
}

export function shortlistExport(items, exportedAt = new Date().toISOString()) {
  return {
    schema: "livability-shortlist",
    version: SHORTLIST_SCHEMA_VERSION,
    exportedAt,
    items: items.map(function (item) {
      return {
        id: item.id,
        label: item.label,
        lat: item.lat,
        lon: item.lon,
        cellId: item.cellId,
        savedAt: item.savedAt,
        buildKey: item.buildKey,
        snapshot: item.snapshot,
        previousSnapshot: item.previousSnapshot
      };
    })
  };
}

