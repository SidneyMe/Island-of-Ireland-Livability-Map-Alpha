import assert from "node:assert/strict";
import { cellGeometryCenter, completeCellGeometry, convexHull } from "./cell_highlight.js";

function feature(cellId, ring) {
  return { type: "Feature", properties: { cell_id: cellId }, geometry: { type: "Polygon", coordinates: [ring] } };
}

{
  const fragments = [
    feature("1000:1:2", [[0, 0], [1, 0], [1, 2], [0, 2], [0, 0]]),
    feature("1000:1:2", [[1, 0], [3, 0], [3, 2], [1, 2], [1, 0]]),
    feature("another-cell", [[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]])
  ];
  const geometry = completeCellGeometry(fragments[0], fragments.slice(1));
  assert.equal(geometry.type, "Polygon");
  assert.deepEqual(new Set(geometry.coordinates[0].map(JSON.stringify)), new Set([
    JSON.stringify([0, 0]), JSON.stringify([3, 0]), JSON.stringify([3, 2]), JSON.stringify([0, 2])
  ]));
}

assert.deepEqual(convexHull([[0, 0], [1, 0], [2, 0], [2, 1], [0, 1]]), [[0, 0], [2, 0], [2, 1], [0, 1]]);
assert.deepEqual(cellGeometryCenter({
  type: "Polygon",
  coordinates: [[[-8, 51], [-6, 51], [-6, 54], [-8, 54], [-8, 51]]]
}), [-7, 52.5]);
assert.equal(cellGeometryCenter(null), null);

console.log("frontend complete cell highlight checks passed");
