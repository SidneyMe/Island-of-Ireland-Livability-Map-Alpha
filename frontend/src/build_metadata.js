function shortFingerprint(value) {
  const text = String(value || "");
  return text ? text.slice(0, 10) : "unknown";
}

function dateLabel(value) {
  if (!value) return "unknown";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return String(value);
  return date.toISOString().slice(0, 10);
}

export function buildMetadataView(runtime = {}) {
  const provenance = runtime.data_provenance || {};
  const freshness = provenance.freshness || {};
  const model = provenance.model || {};
  const gtfs = provenance.gtfs || {};
  const osm = provenance.osm || {};
  const status = String(freshness.status || "incomplete");
  const modelName = String(model.name || runtime.scoring_model || "v3");
  const modelVersion = model.version === null || model.version === undefined
    ? ""
    : " · model " + String(model.version);
  return {
    status,
    statusLabel: status === "current" ? "Current" : status === "stale" ? "Stale" : "Incomplete",
    modelLabel: modelName + modelVersion,
    osmLabel: dateLabel(osm.imported_at) + " · " + shortFingerprint(osm.import_fingerprint),
    gtfsLabel: dateLabel(gtfs.analysis_date) + (Array.isArray(gtfs.sources) && gtfs.sources.length
      ? " · " + gtfs.sources.map(function (source) { return source.label || source.id; }).join(", ")
      : ""),
    updatedLabel: dateLabel(provenance.build && provenance.build.completed_at),
    buildLabel: shortFingerprint(provenance.build && provenance.build.key || runtime.build_key),
    warnings: Array.isArray(freshness.warnings) ? freshness.warnings.map(String) : []
  };
}

