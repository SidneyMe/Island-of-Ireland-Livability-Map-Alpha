const EMPTY_FEATURE_COLLECTION = Object.freeze({ type: "FeatureCollection", features: [] });

export function parseRouteKeys(value) {
  if (Array.isArray(value)) {
    return Array.from(new Set(value.map(String).map(function (item) { return item.trim(); }).filter(Boolean)));
  }
  if (typeof value !== "string" || !value.trim()) return [];
  try {
    return parseRouteKeys(JSON.parse(value));
  } catch (_error) {
    return value.split(",").map(function (item) { return item.trim(); }).filter(Boolean);
  }
}

export function normalizeRouteIndex(payload) {
  if (!payload || !Array.isArray(payload.routes)) return [];
  return payload.routes.filter(function (route) {
    return route && route.route_key && route.asset_url;
  }).map(function (route) {
    return {
      ...route,
      route_key: String(route.route_key),
      mode: String(route.mode || "bus"),
      directions: Array.isArray(route.directions) ? route.directions : []
    };
  });
}

export function routeDisplayName(route) {
  const shortName = String(route && route.route_short_name || "").trim();
  const longName = String(route && route.route_long_name || "").trim();
  return [shortName, longName].filter(Boolean).join(" · ") || String(route && route.route_key || "Route");
}

export function searchRoutes(routes, query, modes, limit = 12) {
  const needle = String(query || "").trim().toLocaleLowerCase();
  const allowedModes = modes instanceof Set ? modes : new Set(modes || []);
  return (routes || []).filter(function (route) {
    if (allowedModes.size && !allowedModes.has(String(route.mode || ""))) return false;
    if (!needle) return true;
    const directionText = (route.directions || []).map(function (direction) {
      return [direction.origin_name, direction.destination_name, direction.headsign].join(" ");
    }).join(" ");
    return [route.route_short_name, route.route_long_name, route.route_key, directionText]
      .join(" ").toLocaleLowerCase().includes(needle);
  }).slice(0, Math.max(0, Number(limit) || 0));
}

export function overviewModeFilter(modes) {
  const values = Array.from(modes || []).map(String);
  return ["in", ["get", "mode"], ["literal", values]];
}

export function focusFeatureFilter(modes, patternId, featureKinds) {
  const filters = [
    ["in", ["get", "mode"], ["literal", Array.from(modes || []).map(String)]],
    ["in", ["get", "feature_kind"], ["literal", featureKinds]]
  ];
  if (patternId) filters.push(["==", ["get", "pattern_id"], String(patternId)]);
  return ["all", ...filters];
}

function patternFeatureKey(properties) {
  return [String(properties.route_key || ""), String(properties.pattern_id || "")].join("\u0000");
}

function exactPatternSignature(feature, stops) {
  const properties = feature.properties || {};
  return JSON.stringify([
    String(properties.route_key || ""),
    String(properties.mode || ""),
    String(properties.direction_id ?? ""),
    String(properties.headsign || "").trim(),
    String(properties.origin_name || "").trim(),
    String(properties.destination_name || "").trim(),
    String(properties.geometry_source || ""),
    feature.geometry || null,
    stops.map(function (stop) {
      const stopProperties = stop.properties || {};
      return [
        Number(stopProperties.stop_sequence),
        String(stopProperties.feed_id || ""),
        String(stopProperties.stop_id || ""),
        stop.geometry || null
      ];
    })
  ]);
}

export function dedupeExactRoutePatterns(featureCollection) {
  const features = featureCollection && Array.isArray(featureCollection.features)
    ? featureCollection.features
    : [];
  const stopsByPattern = new Map();
  features.forEach(function (feature) {
    const properties = feature && feature.properties || {};
    if (properties.feature_kind !== "stop" || !properties.pattern_id) return;
    const key = patternFeatureKey(properties);
    if (!stopsByPattern.has(key)) stopsByPattern.set(key, []);
    stopsByPattern.get(key).push(feature);
  });
  stopsByPattern.forEach(function (stops) {
    stops.sort(function (left, right) {
      return Number(left.properties.stop_sequence) - Number(right.properties.stop_sequence);
    });
  });

  const signatures = new Set();
  const duplicatePatternKeys = new Set();
  features.forEach(function (feature) {
    const properties = feature && feature.properties || {};
    if (properties.feature_kind !== "pattern" || !properties.pattern_id) return;
    const key = patternFeatureKey(properties);
    const signature = exactPatternSignature(feature, stopsByPattern.get(key) || []);
    if (signatures.has(signature)) duplicatePatternKeys.add(key);
    else signatures.add(signature);
  });

  return {
    type: "FeatureCollection",
    features: features.filter(function (feature) {
      const properties = feature && feature.properties || {};
      if (!properties.pattern_id) return true;
      return !duplicatePatternKeys.has(patternFeatureKey(properties));
    })
  };
}

export function mergeRouteFeatureCollections(payloads) {
  return dedupeExactRoutePatterns({
    type: "FeatureCollection",
    features: (payloads || []).flatMap(function (payload) {
      return payload && Array.isArray(payload.features) ? payload.features : [];
    })
  });
}

export function routeDirectionGroups(route, featureCollection) {
  const routeKey = String(route && route.route_key || "");
  const availablePatternIds = new Set((featureCollection && featureCollection.features || []).flatMap(function (feature) {
    const properties = feature && feature.properties || {};
    if (properties.feature_kind !== "pattern" || String(properties.route_key || "") !== routeKey) return [];
    return [String(properties.pattern_id)];
  }));
  const groups = new Map();
  (route && route.directions || []).forEach(function (direction) {
    const patternId = String(direction && direction.pattern_id || "");
    if (availablePatternIds.size && !availablePatternIds.has(patternId)) return;
    const label = String(direction && (direction.label || direction.destination_name || direction.headsign) || "Direction").trim();
    const groupKey = label.toLocaleLowerCase();
    if (!groups.has(groupKey)) groups.set(groupKey, { label: label, directions: [] });
    groups.get(groupKey).directions.push(direction);
  });
  return Array.from(groups.values());
}

export function emptyRouteFeatureCollection() {
  return { ...EMPTY_FEATURE_COLLECTION, features: [] };
}
