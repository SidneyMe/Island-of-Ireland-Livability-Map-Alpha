import assert from "node:assert/strict";

import { transportRealityPopupHtml } from "./transport_reality_popup.js";

{
  const html = transportRealityPopupHtml({
    stop_name: "Main Street",
    reality_status: "active_confirmed",
    public_departures_30d: 21
  });

  assert.match(html, /Public departures in activity window: 21/);
}

{
  const html = transportRealityPopupHtml({
    stop_name: "Drogheda Train Station",
    reality_status: "inactive_confirmed",
    public_departures_30d: 0
  });

  assert.match(html, /Public departures in activity window: 0/);
}

{
  const html = transportRealityPopupHtml({
    source_ref: "gtfs/nta/S1",
    reality_status: "school_only_confirmed",
    public_departures_30d: 0
  });

  assert.match(html, /gtfs\/nta\/S1/);
}

{
  const html = transportRealityPopupHtml({
    stop_name: "Dromcolliher",
    reality_status: "active_confirmed",
    public_departures_30d: 14
  });

  assert.match(html, /Public departures in activity window: 14/);
}

console.log("transport reality popup checks passed");
