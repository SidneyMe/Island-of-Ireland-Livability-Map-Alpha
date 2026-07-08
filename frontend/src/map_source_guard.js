function sourceExists(map, sourceId) {
  if (!map || typeof map.getSource !== "function") return false;
  try {
    return Boolean(map.getSource(sourceId));
  } catch (error) {
    return false;
  }
}

function sourceLoaded(map, sourceId, fallback = false) {
  if (!map || typeof map.isSourceLoaded !== "function") {
    return Boolean(fallback);
  }
  if (!sourceExists(map, sourceId)) {
    return false;
  }
  try {
    return Boolean(map.isSourceLoaded(sourceId));
  } catch (error) {
    return Boolean(fallback);
  }
}

export {
  sourceExists,
  sourceLoaded
};
