## Updating the project

---

### Updating the bot

After pulling, you may need to re-sync the command tree in order for changes to be reflected in Discord. 
This can be done with the `--sync` flag:

```shell
cd bot
python run.py --sync
```


### Updating resources

1. Shrimpy relies on unpacked game files at each game update. Install the WoWS Unpacker utility
available [here](https://forum.worldofwarships.eu/topic/113847-all-wows-unpack-tool-unpack-game-client-resources/).

2. Run the end-to-end update pipeline from the repo root:

```shell
python scripts/update_resources.py --wows-root "C:\Games\World_of_Warships"
```

Notes:
- Use the venv Python if the bot uses it:
  `.\.venv\Scripts\python.exe scripts/update_resources.py --wows-root "C:\Games\World_of_Warships"`
- The script clones/updates `minimap_renderer` and `FilteredGameParams2Json` into `tools/`.
- It unpacks `GameParams.data`, `texts`, and `ships_silhouettes`, generates renderer data
  (including `abilities.json`), and regenerates `ships.json`.
- If the renderer package path cannot be auto-detected, pass `--renderer-target`.

3. Compare ship changes:

```shell
python scripts/ships/compare.py
```

4. Update `bot/assets/public/guess.toml` as appropriate.
