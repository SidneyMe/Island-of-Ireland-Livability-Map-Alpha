// Navigation is independent of the map runtime so help and tabs remain usable
// while the dataset is warming up or unavailable.
export function initializeWorkspace(root = document) {
  root.querySelectorAll('[role="tablist"]').forEach(function (list) {
    const tabs = Array.from(list.querySelectorAll('[role="tab"]'));
    function activate(tab, focus = false) {
      tabs.forEach(function (item) {
        const selected = item === tab;
        item.setAttribute("aria-selected", String(selected));
        item.tabIndex = selected ? 0 : -1;
        const panel = root.getElementById(item.getAttribute("aria-controls"));
        if (panel) panel.hidden = !selected;
      });
      if (focus) tab.focus();
    }
    tabs.forEach(function (tab, index) {
      tab.addEventListener("click", function () { activate(tab); });
      tab.addEventListener("keydown", function (event) {
        let next;
        if (event.key === "ArrowRight") next = (index + 1) % tabs.length;
        if (event.key === "ArrowLeft") next = (index + tabs.length - 1) % tabs.length;
        if (event.key === "Home") next = 0;
        if (event.key === "End") next = tabs.length - 1;
        if (next === undefined) return;
        event.preventDefault();
        activate(tabs[next], true);
      });
    });
  });

  const layers = root.getElementById("workspace-layers");
  const count = root.getElementById("active-layer-count");
  const summary = root.getElementById("active-layer-summary");
  function updateLayerCount() {
    if (!layers) return;
    const active = layers.querySelectorAll(
      '.amenity-control-card > .toggle-row input[type="checkbox"]:checked:not(:disabled), ' +
      '#transit-controls > .toggle-row input[type="checkbox"]:checked:not(:disabled), ' +
      '#landuse-controls > .toggle-row input[type="checkbox"]:checked:not(:disabled), ' +
      '#noise-controls > .toggle-row input[type="checkbox"]:checked:not(:disabled)'
    ).length;
    if (count) { count.textContent = String(active); count.hidden = active === 0; }
    if (summary) summary.textContent = active ? active + (active === 1 ? " overlay on" : " overlays on") : "No overlays on";
  }
  if (layers) layers.addEventListener("change", updateLayerCount);
  updateLayerCount();
}
