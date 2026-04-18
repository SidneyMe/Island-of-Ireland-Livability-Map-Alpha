function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function departureLineHtml(properties) {
  return (
    "<p>Public departures in activity window: " +
    escapeHtml(String(properties.public_departures_30d || 0)) +
    "</p>"
  );
}

export function transportRealityPopupHtml(properties) {
  return (
    '<div class="popup-content">' +
      "<h3>" + escapeHtml(properties.stop_name || properties.source_ref || "Transport stop") + "</h3>" +
      "<p>Status: " + escapeHtml(properties.reality_status || "unknown") + "</p>" +
      departureLineHtml(properties) +
    "</div>"
  );
}
