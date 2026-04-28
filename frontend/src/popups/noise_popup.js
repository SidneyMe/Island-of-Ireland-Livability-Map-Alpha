import { noiseSourceLabel } from "../noise_filters.js";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

export function noisePopupHtml(properties) {
  const sourceLabel = noiseSourceLabel(properties.source_type || "noise");
  const metricLabel = properties.metric === "Lnight" ? "Night" : "Day-evening-night";
  const round = properties.round || properties.round_number || "";
  const reportPeriod = properties.report_period || "";
  const jurisdiction = properties.jurisdiction === "roi"
    ? "Republic of Ireland"
    : properties.jurisdiction === "ni"
      ? "Northern Ireland"
      : properties.jurisdiction || "";
  const details = [
    "<p>" + escapeHtml(metricLabel + ": " + (properties.db_value || "unknown") + " dB") + "</p>",
    "<p>" + escapeHtml(sourceLabel + (jurisdiction ? " - " + jurisdiction : "")) + "</p>"
  ];
  if (round || reportPeriod) {
    details.push("<p>Round: " + escapeHtml([round, reportPeriod].filter(Boolean).join(" - ")) + "</p>");
  }
  if (properties.source_dataset) {
    details.push("<p>Source: " + escapeHtml(properties.source_dataset) + "</p>");
  }
  return (
    '<div class="popup-content">' +
      "<h3>Noise contour</h3>" +
      details.join("") +
    "</div>"
  );
}
