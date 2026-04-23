export const CLICK_ACTIONS = {
  TRANSPORT: "transport",
  AMENITY: "amenity",
  SERVICE_DESERT: "service_desert",
  FINE_INSPECT: "fine_inspect",
  COARSE_GRID: "coarse_grid",
  NONE: "none"
};

function queryLayer(map, point, layerId) {
  if (!map || !layerId) return [];
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

  const amenityFeatures = queryLayer(map, point, "amenities-circle");
  if (amenityFeatures.length > 0) {
    return { type: CLICK_ACTIONS.AMENITY, features: amenityFeatures };
  }

  const desertFeatures = queryLayer(map, point, "service-deserts-fill");
  if (desertFeatures.length > 0) {
    return { type: CLICK_ACTIONS.SERVICE_DESERT, features: desertFeatures };
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
