# Replay Map Names

`replay_map_names.json` maps replay-file map names (from filenames) to the actual in-game map names.

- **Keys**: Replay map name, lowercase (e.g. `"sea hope"`, `"gold harbor"`).
- **Values**: Actual map name as shown in-game (e.g. `"Sea Of Fortune"`, `"Haven"`).

Display format: **Actual Name (Replay Name)** when a mapping exists (e.g. `Sea Of Fortune (Sea Hope)`). If no mapping is found, only the replay name is shown.

Add new entries to `replay_map_names.json` as needed; the bot loads it at startup.
