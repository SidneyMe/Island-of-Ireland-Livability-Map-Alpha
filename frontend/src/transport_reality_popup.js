import {
  TRANSPORT_MODE_ORDER,
  transportModeLabel,
  transportSubtierLabel
} from "./transport_filters.js";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function _normalizedRows(input) {
  const rows = Array.isArray(input) ? input.slice() : [input];
  return rows
    .filter(function (row) {
      return row && typeof row === "object";
    })
    .sort(function (left, right) {
      const leftKey = [
        String(left.stop_name || ""),
        String(left.feed_id || ""),
        String(left.stop_id || ""),
        String(left.source_ref || "")
      ].join("|");
      const rightKey = [
        String(right.stop_name || ""),
        String(right.feed_id || ""),
        String(right.stop_id || ""),
        String(right.source_ref || "")
      ].join("|");
      return leftKey.localeCompare(rightKey);
    });
}

function _routeModesList(properties) {
  if (Array.isArray(properties.route_modes)) {
    return properties.route_modes.map(function (value) {
      return String(value || "").trim();
    }).filter(Boolean);
  }
  return String(properties.route_modes || "")
    .split(",")
    .map(function (value) {
      return value.trim();
    })
    .filter(Boolean);
}

function _routeModesText(properties) {
  return _routeModesList(properties).join(", ");
}

function _modeTierText(properties) {
  const modes = new Set(_routeModesList(properties));
  for (const mode of TRANSPORT_MODE_ORDER) {
    if (modes.has(mode)) {
      return transportModeLabel(mode);
    }
  }
  return "";
}

function _tierText(properties) {
  if (properties.is_unscheduled_stop) {
    return "Unscheduled";
  }
  const modeTier = _modeTierText(properties);
  if (modeTier) {
    return modeTier;
  }
  return transportSubtierLabel(properties.bus_service_subtier);
}

function _rowHtml(properties) {
  const detailLines = [
    "<p><strong>Public transport tier:</strong> " + escapeHtml(_tierText(properties)) + "</p>",
    "<p>Scheduled snapshot departures in current activity window: " +
      escapeHtml(String(properties.public_departures_30d || 0)) +
      "</p>"
  ];
  const routeModes = _routeModesText(properties);
  if (routeModes) {
    detailLines.push("<p>Modes: " + escapeHtml(routeModes) + "</p>");
  }
  if (properties.has_exception_only_service) {
    detailLines.push("<p>Has calendar_dates-only bus service</p>");
  }
  if (properties.source_ref) {
    detailLines.push('<p class="popup-muted">' + escapeHtml(properties.source_ref) + "</p>");
  }
  return (
    '<section class="transport-popup-row">' +
      "<h3>" + escapeHtml(properties.stop_name || properties.source_ref || "Transport stop") + "</h3>" +
      detailLines.join("") +
    "</section>"
  );
}

export function transportRealityPopupHtml(input) {
  const rows = _normalizedRows(input);
  return (
    '<div class="popup-content popup-stack">' +
      rows.map(_rowHtml).join("") +
    "</div>"
  );
}
