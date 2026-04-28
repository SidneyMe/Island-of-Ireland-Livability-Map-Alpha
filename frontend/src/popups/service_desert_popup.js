import { formatResolutionLabel } from "../grid_debug.js";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

export function serviceDesertPopupHtml(properties, fallbackResolutionM = 50) {
  return (
    '<div class="popup-content">' +
      "<h3>Service desert candidate</h3>" +
      "<p>Resolution: " + escapeHtml(formatResolutionLabel(properties.resolution_m || fallbackResolutionM)) + "</p>" +
      "<p>Baseline reachable GTFS stops: " + escapeHtml(String(properties.baseline_reachable_stop_count || 0)) + "</p>" +
      "<p>Reachable public departures (7d): " + escapeHtml(String(properties.reachable_public_departures_7d || 0)) + "</p>" +
    "</div>"
  );
}
