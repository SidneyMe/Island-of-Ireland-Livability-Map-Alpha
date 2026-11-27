"""Dependency-free named v4 channels, safe to import in Windows bake workers."""
CATEGORIES = ("shops", "transport", "healthcare", "parks")
PENALTIES = ("railway_proximity", "road_proximity", "industrial_context", "over_concentration", "pitch_noise")
BONUSES = ("rail_station_access", "green_buffer")
SCORE_CHANNEL_SCHEMA = {"version": 1, "components": list(CATEGORIES), "penalties": list(PENALTIES), "bonuses": list(BONUSES)}
