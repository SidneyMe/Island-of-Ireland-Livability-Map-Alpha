function _normalizedTierCounts(runtime) {
  const raw = runtime && runtime.amenity_tier_counts && typeof runtime.amenity_tier_counts === "object"
    ? runtime.amenity_tier_counts
    : {};
  const normalized = {};
  Object.keys(raw).forEach(function (category) {
    const tiers = raw[category];
    if (!tiers || typeof tiers !== "object" || Array.isArray(tiers)) {
      normalized[category] = {};
      return;
    }
    const bucket = {};
    Object.keys(tiers).forEach(function (tier) {
      if (!tier) return;
      const count = Number(tiers[tier]);
      if (!Number.isFinite(count) || count < 0) return;
      bucket[tier] = count;
    });
    normalized[category] = bucket;
  });
  return normalized;
}

function _selectedTierValues(enabledTiersByCategory, category) {
  if (enabledTiersByCategory instanceof Map) {
    return Array.from(enabledTiersByCategory.get(category) || []);
  }
  const raw = enabledTiersByCategory && typeof enabledTiersByCategory === "object"
    ? enabledTiersByCategory[category]
    : [];
  return Array.isArray(raw) ? raw.slice() : Array.from(raw || []);
}

function formatAmenityLabel(value) {
  const words = String(value || "")
    .trim()
    .split("_")
    .filter(Boolean)
    .map(function (word) {
      return String(word).toLowerCase();
    });
  if (!words.length) return "";
  words[0] = words[0].charAt(0).toUpperCase() + words[0].slice(1);
  return words.join(" ");
}

function tierOptionsForCategory(runtime, category) {
  return Object.keys(_normalizedTierCounts(runtime)[category] || {});
}

function defaultAmenityTierSelections(runtime) {
  const normalized = _normalizedTierCounts(runtime);
  const selections = {};
  Object.keys(normalized).forEach(function (category) {
    const tiers = Object.keys(normalized[category] || {});
    if (!tiers.length) return;
    selections[category] = tiers;
  });
  return selections;
}

function buildAmenityLayerFilter(runtime, enabledCategories, enabledTiersByCategory) {
  const categories = Array.from(enabledCategories || []);
  const clauses = [];

  categories.forEach(function (category) {
    const tiers = tierOptionsForCategory(runtime, category);
    if (!tiers.length) {
      clauses.push(["==", ["get", "category"], category]);
      return;
    }

    const selectedTiers = _selectedTierValues(enabledTiersByCategory, category);
    if (!selectedTiers.length) {
      return;
    }

    clauses.push([
      "all",
      ["==", ["get", "category"], category],
      ["in", ["coalesce", ["get", "tier"], ""], ["literal", selectedTiers]],
    ]);
  });

  if (!clauses.length) {
    return ["in", ["get", "category"], ["literal", []]];
  }
  if (clauses.length === 1) {
    return clauses[0];
  }
  return ["any", ...clauses];
}

export {
  buildAmenityLayerFilter,
  defaultAmenityTierSelections,
  formatAmenityLabel,
  tierOptionsForCategory,
};
