import { noiseSourceLabel } from "../noise_filters.js";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

export function noisePopupHtml(properties) {
  const kind = String(properties.kind || properties.source_type || "road");
  const classLabel = String(properties.class || "unclassified");
  const sourceLabel = noiseSourceLabel(kind);
  const metricLabel = "Day-evening-night";
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
      "<h3>Official-derived road Lden proxy</h3>" +
      details.join("") +
      "<p>Approximate road Lden proxy from official-derived noise grid. Not measured point noise and not official contour geometry.</p>" +
    "</div>"
  );
}
