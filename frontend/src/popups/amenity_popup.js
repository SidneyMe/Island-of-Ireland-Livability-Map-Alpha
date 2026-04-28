import { formatAmenityLabel } from "../amenity_filters.js";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

export function amenityPopupHtml(properties) {
  const title = properties.name || formatAmenityLabel(properties.category || "Amenity");
  const details = [];
  const categoryLabel = formatAmenityLabel(properties.category || "");
  const tierLabel = formatAmenityLabel(properties.tier || "");
  if (properties.name && categoryLabel) {
    const categoryText = tierLabel
      ? categoryLabel + " • " + tierLabel
      : categoryLabel;
    details.push("<p>" + escapeHtml(categoryText) + "</p>");
  } else if (tierLabel) {
    details.push("<p>" + escapeHtml(tierLabel) + "</p>");
  }
  if (properties.source_ref) {
    details.push("<p>" + escapeHtml(properties.source_ref) + "</p>");
  }
  if (properties.source) {
    details.push("<p>Source: " + escapeHtml(properties.source) + "</p>");
  }
  if (properties.conflict_class && properties.conflict_class !== "osm_only") {
    details.push("<p>Merge: " + escapeHtml(properties.conflict_class) + "</p>");
  }
  return (
    '<div class="popup-content">' +
      "<h3>" + escapeHtml(title) + "</h3>" +
      details.join("") +
    "</div>"
  );
}
