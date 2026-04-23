import assert from "node:assert/strict";

import { transportRealityPopupHtml } from "./transport_reality_popup.js";

{
  const html = transportRealityPopupHtml({
    stop_name: "Main Street",
    bus_service_subtier: "mon_sun",
    public_departures_30d: 21,
    route_modes: "bus"
  });

  assert.match(html, /Weekly service:<\/strong> Whole week/);
  assert.match(html, /Scheduled snapshot departures in current activity window: 21/);
  assert.match(html, /Modes: bus/);
}

{
  const html = transportRealityPopupHtml({
    source_ref: "gtfs\/nta\/S1",
    is_unscheduled_stop: true,
    public_departures_30d: 0
  });

  assert.match(html, /Unscheduled/);
  assert.match(html, /gtfs\/nta\/S1/);
}

{
  const html = transportRealityPopupHtml([
    {
      stop_name: "Rosehall",
      bus_service_subtier: "mon_sat",
      public_departures_30d: 10
    },
    {
      stop_name: "Rosehall Rail",
      bus_service_subtier: null,
      has_exception_only_service: true,
      public_departures_30d: 0
    }
  ]);

  assert.match(html, /Rosehall/);
  assert.match(html, /Mon-Sat/);
  assert.match(html, /Rosehall Rail/);
  assert.match(html, /Has calendar_dates-only bus service/);
}

console.log("transport reality popup checks passed");
