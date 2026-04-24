import assert from "node:assert/strict";

import { transportRealityPopupHtml } from "./transport_reality_popup.js";

{
  const html = transportRealityPopupHtml({
    stop_name: "Main Street",
    bus_service_subtier: "mon_sun",
    public_departures_30d: 21,
    weekday_morning_peak_deps: 4,
    weekday_evening_peak_deps: 5,
    friday_evening_deps: 6,
    transport_score_units: 3,
    bus_daytime_deps: 28,
    bus_daytime_headway_min: 30,
    bus_frequency_tier: "moderate",
    route_modes: "bus"
  });

  assert.match(html, /Public transport tier:<\/strong> Moderate \(16-30 min\)/);
  assert.match(html, /Transport score units: 3 \/ 5/);
  assert.match(html, /Bus frequency: Moderate \(16-30 min\)/);
  assert.match(html, /Weekday daytime bus headway: 30 min; departures: 28/);
  assert.match(html, /Weekday commute departures: 4 morning \/ 5 evening/);
  assert.match(html, /Friday evening departures: 6 \(Friday 16:00 through Saturday 02:00 am\)/);
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
  const html = transportRealityPopupHtml({
    stop_name: "Heuston",
    bus_service_subtier: null,
    public_departures_30d: 6150,
    route_modes: "tram",
    source_ref: "gtfs/nta/8220GA00387"
  });

  assert.match(html, /Public transport tier:<\/strong> Tram/);
  assert.doesNotMatch(html, /No recent bus tier/);
  assert.match(html, /Modes: tram/);
}

{
  const html = transportRealityPopupHtml({
    stop_name: "Dublin Heuston",
    bus_service_subtier: null,
    public_departures_30d: 3196,
    route_modes: "rail",
    source_ref: "gtfs/nta/8220IR0132"
  });

  assert.match(html, /Public transport tier:<\/strong> Rail/);
  assert.doesNotMatch(html, /No recent bus tier/);
  assert.match(html, /Modes: rail/);
}

{
  const html = transportRealityPopupHtml({
    stop_name: "Mixed interchange",
    bus_service_subtier: "mon_sun",
    bus_frequency_tier: "frequent",
    public_departures_30d: 120,
    route_modes: "bus,rail,tram"
  });

  assert.match(html, /Public transport tier:<\/strong> Tram/);
  assert.doesNotMatch(html, /Public transport tier:<\/strong> Whole week/);
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
