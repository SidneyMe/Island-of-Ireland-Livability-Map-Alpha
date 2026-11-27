export const CLICK_ACTIONS = {
  TRANSPORT: "transport",
  TRANSPORT_HUB: "transport_hub",
  TRANSPORT_ROUTE: "transport_route",
  AMENITY: "amenity",
  SERVICE_DESERT: "service_desert",
  NOISE: "noise",
  FINE_INSPECT: "fine_inspect",
  COARSE_GRID: "coarse_grid",
  NONE: "none"
};

function queryLayer(map, point, layerId) {
  if (!map || !layerId) return [];
  // Optional overlays (noise, deserts) are absent from some builds' styles.
  if (typeof map.getLayer === "function" && !map.getLayer(layerId)) return [];
  return map.queryRenderedFeatures(point, { layers: [layerId] });
}

export function resolveMapClickAction({
  map,
  point,
  fineSurfaceEnabled,
  gridVisible,
  activeGridLayerId
}) {
  const transportFeatures = queryLayer(map, point, "transport-reality-circle");
  if (transportFeatures.length > 0) {
    return { type: CLICK_ACTIONS.TRANSPORT, features: transportFeatures };
  }

  const transferFeatures = queryLayer(map, point, "transport-focus-transfers")
    .concat(queryLayer(map, point, "transport-transfer-hubs"));
  if (transferFeatures.length > 0) {
    return { type: CLICK_ACTIONS.TRANSPORT_HUB, features: transferFeatures };
  }

  const focusRouteFeatures = queryLayer(map, point, "transport-focus-line")
    .concat(queryLayer(map, point, "transport-focus-approximate"));
  if (focusRouteFeatures.length > 0) {
    return { type: CLICK_ACTIONS.TRANSPORT_ROUTE, features: focusRouteFeatures };
  }
  const routeFeatures = queryLayer(map, point, "transport-routes-line")
    .concat(queryLayer(map, point, "transport-routes-approximate"));
  if (routeFeatures.length > 0) {
    return { type: CLICK_ACTIONS.TRANSPORT_ROUTE, features: routeFeatures };
  }

  const amenityFeatures = queryLayer(map, point, "amenities-circle");
  if (amenityFeatures.length > 0) {
    return { type: CLICK_ACTIONS.AMENITY, features: amenityFeatures };
  }

  const desertFeatures = queryLayer(map, point, "service-deserts-fill");
  if (desertFeatures.length > 0) {
    return { type: CLICK_ACTIONS.SERVICE_DESERT, features: desertFeatures };
  }

  const noiseFeatures = queryLayer(map, point, "noise-proxy-fill");
  if (noiseFeatures.length > 0) {
    return { type: CLICK_ACTIONS.NOISE, features: noiseFeatures };
  }

  if (fineSurfaceEnabled) {
    return { type: CLICK_ACTIONS.FINE_INSPECT, features: [] };
  }

  const gridLayerId = gridVisible ? activeGridLayerId : null;
  const gridFeatures = queryLayer(map, point, gridLayerId);
  if (gridFeatures.length > 0) {
    return { type: CLICK_ACTIONS.COARSE_GRID, features: gridFeatures };
  }

  return { type: CLICK_ACTIONS.NONE, features: [] };
}
