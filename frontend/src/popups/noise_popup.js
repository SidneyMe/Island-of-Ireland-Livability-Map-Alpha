import { noiseMetricLabel, noiseSourceLabel } from "../noise_filters.js";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

export function noisePopupHtml(properties) {
  const kind = String(properties.kind || properties.source_type || "road");
  const metric = String(properties.metric || "Lden");
  const classLabel = String(properties.class || "unclassified");
  const sourceLabel = noiseSourceLabel(kind);
  const metricLabel = noiseMetricLabel(metric);
  const proxyScore = Number(properties.proxy_score);
  const calibratedBandMin = Number(properties.band_min ?? properties.calibrated_band_min);
  const details = [
    "<p>" + escapeHtml(metricLabel + " proxy score: " + (Number.isFinite(proxyScore) ? proxyScore.toFixed(0) : "unknown")) + "</p>",
    "<p>" + escapeHtml("Calibrated band min: " + (Number.isFinite(calibratedBandMin) ? calibratedBandMin.toFixed(0) : "unknown")) + "</p>",
    "<p>" + escapeHtml(sourceLabel + " - " + classLabel) + "</p>"
  ];
  if (properties.calibration_stat) {
    details.push("<p>Calibration stat: " + escapeHtml(String(properties.calibration_stat)) + "</p>");
  }
  if (properties.sample_count !== undefined && properties.sample_count !== null) {
    details.push("<p>Samples: " + escapeHtml(String(properties.sample_count)) + "</p>");
  }
  return (
    '<div class="popup-content">' +
      "<h3>Official-derived transport/industry noise overlay</h3>" +
      details.join("") +
      "<p>Road/rail use grid proxy; airport/industry use resolved official noise polygons. Not measured point noise.</p>" +
    "</div>"
  );
}
