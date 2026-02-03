## Shrimpy

Shrimpy is a Discord bot with World of Warships utilities.

For a list of features, see the [commands reference](https://github.com/padtrack/track/wiki/Commands) in the wiki.

---

### Installation

**Quick summary:** Python 3.10+, Redis, FFmpeg. Clone → create and activate a **virtual environment** (`.venv`) → `pip install -r requirements.txt` → copy `secrets_template.ini` to `secrets.ini` → run `python bot/utils/db.py` → start Redis → run bot and worker.

For a **step-by-step guide** (including troubleshooting), see **[docs/INSTALL.md](docs/INSTALL.md)**.

| Step | Command / action |
|------|------------------|
| 1 | Python 3.10+; create virtual env: `python -m venv .venv` then activate (e.g. `.\.venv\Scripts\Activate.ps1` on Windows) |
| 2 | Clone repo, `cd track` |
| 3 | `pip install -U pip` then `pip install -r requirements.txt` |
| 4 | Copy `secrets_template.ini` → `secrets.ini` and fill in Discord token, Redis, WG app id |
| 5 | `python bot/utils/db.py` (from repo root) |
| 6 | Install and start [Redis](https://redis.io/docs/getting-started/); install [FFmpeg](https://ffmpeg.org/) |
| 7 | Run bot: `python -m bot.run` (and worker: `python -m bot.worker -q single dual`) or use `.\scripts\dev.ps1 start` |

For updating the bot between game updates, see [docs/UPDATING.md](docs/UPDATING.md).

---

### Usage

The bot can be launched with `bot/run.py`. The full usage is:

```
python run.py [--sync | --no-sync]
```

The optional sync flag will cause the bot to sync the command tree on startup. 
Only use this flag when necessary to avoid being rate-limited.

Render workers can be launched with `bot/worker.py`. The full usage is:

```
python worker.py -q {single, dual} [{single,dual} ...]
```

Which queues the worker should listen to can be specified with the respective option.

---

### Development Commands (Windows PowerShell)

A utility script `scripts/dev.ps1` provides convenient commands for development:

```powershell
.\scripts\dev.ps1 <action>
```

| Action | Description |
|--------|-------------|
| `start` | Start Redis, Bot, and Worker (idempotent: stops any existing process first) |
| `start-sync` | Start with command tree sync (use after adding or changing slash command options so Discord shows them) |
| `stop` | Stop Bot and Worker (idempotent; works across sessions) |
| `restart` | Stop all, ensure Redis, flush queues, and start fresh |
| `status` | Show status of all services (PID-based; works from any terminal) |
| `logs` | Stream bot logs from `generated/shrimpy-bot.log` (Ctrl+C to stop) |
| `worker-logs` | Stream worker logs from `generated/shrimpy-worker.log` (Ctrl+C to stop) |
| `flush` | Flush all Redis queues |
| `help` | Show help message |

Start/stop use PID files in `generated/` so they work reliably when run via `powershell -File` or from any terminal.

**Examples:**
```powershell
# Start everything (Redis + Bot + Worker)
.\scripts\dev.ps1 start

# Clean restart (stop, flush queues, start fresh)
.\scripts\dev.ps1 restart

# Watch live worker logs for render processing
.\scripts\dev.ps1 worker-logs

# Check status of all services
.\scripts\dev.ps1 status
```

**Environment Variables:**
- `REDIS_PORT` - Redis port (default: `6380`)
- `SYNC_GUILD_ID` - Guild ID for command sync (optional)

---

### Manual Commands Reference

If you prefer running commands manually:

**Start Redis (Docker):**
```powershell
docker start redis-shrimpy
# Or create new: docker run -d --name redis-shrimpy -p 6380:6379 redis:latest redis-server --requirepass shrimpy
```

**Start Bot:**
```powershell
$env:REDIS_PORT="6380"; python bot/run.py
```

**Start Worker:**
```powershell
$env:REDIS_PORT="6380"; python bot/worker.py -q single dual
```

**Sync Commands to a Specific Guild:**
```powershell
$env:SYNC_GUILD_ID="YOUR_GUILD_ID"; $env:REDIS_PORT="6380"; python bot/run.py --sync
```

**Flush Redis Queues:**
```powershell
docker exec redis-shrimpy redis-cli -a shrimpy FLUSHALL
```

**Check Service Status:**
```powershell
.\scripts\dev.ps1 status
# Or for Redis only: docker ps --filter "name=redis-shrimpy"
```

---

### License

This project is licensed under the GNU AGPLv3 License.

If you run a modified version of this bot for other users (e.g. as a hosted Discord bot),
the AGPLv3 generally requires that those users be able to obtain the Corresponding Source
for the version you are running.

This repository originated from `padtrack/track` (`https://github.com/padtrack/track`) and includes modifications. The app is now named Shrimpy.

---

### Credits and Links

- [@alpha#9432](https://github.com/0alpha) - Thank you for your invaluable insight and help with the OAuth 2.0 server!
- [@TenguBlade#3158](https://www.reddit.com/user/TenguBlade/) - Thank you for your help with the guess similarity groups!
- @dmc#3518 - Thank you for your help with the builds!
- The Minimap Renderer's repository is available [here](https://github.com/WoWs-Builder-Team/minimap_renderer).
