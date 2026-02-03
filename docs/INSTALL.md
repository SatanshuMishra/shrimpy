# Installation and setup

This guide gets Shrimpy running on a new machine with minimal manual steps.

---

## Prerequisites

- **Python 3.10+**
- **Redis** (for queues; e.g. Docker or local install)
- **FFmpeg** (for render worker; must be on `PATH`)
- **Git** (for cloning and for `minimap_renderer` dependency)

---

## 1. Clone and enter the repo

```powershell
git clone https://github.com/SatanshuMishra/shrimpy.git
cd track
```

(Or your fork / repo URL.)

---

## 2. Virtual environment (`.venv`)

Use a virtual environment so dependencies stay isolated. From the repo root:

**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Linux/macOS:**
```bash
python3.10 -m venv .venv
source .venv/bin/activate
```

Ensure `.venv` is activated (your prompt usually shows `(.venv)`) before running `pip` or the bot.

---

## 3. Install dependencies

```powershell
pip install -U pip
pip install -r requirements.txt
```

This installs all runtime dependencies, including `minimap_renderer` (and its `replay_parser` dependency) from Git. If you see errors about `replay_parser`, ensure you are using Python 3.10+ and retry; the renderer package pulls it in.

---

## 4. Secrets and configuration

### 4.1 Copy secrets template

```powershell
copy secrets_template.ini secrets.ini
```

Edit `secrets.ini` and fill in (at least for `[testing]`):

- `discord_token` – from [Discord Developer Portal](https://discord.com/developers/applications) (Bot token)
- `redis_password` – password for your Redis instance
- `redis_host` – usually `localhost`
- `wg_application_id` – Wargaming API application ID (for stats, etc.)
- `twitter_bearer_token` – optional; only if you use the cat extension

Use the `[production]` section when running in production.

---

## 5. Config defaults (optional)

For a new deployment you may want to change built-in defaults in `config.py` (bottom of the file):

- `CREATED` – timestamp when the bot “started” (for stats)
- `DISCORD_OWNER_IDS` – set of Discord user IDs for owner-only commands
- `CHANNELS_FAILED_RENDERS` – channel ID where failed renders are uploaded

You can also set `REDIS_PORT`, `REDIS_HOST`, `REDIS_PASSWORD`, `DISCORD_OWNER_IDS`, `CHANNELS_FAILED_RENDERS`, and `CREATED` via **environment variables** (e.g. in your shell or process manager) before starting the bot; `config.py` reads them when set.

---

## 6. Database setup

Create the SQLite DB and tables (from repo root):

```powershell
python bot/utils/db.py
```

---

## 7. Redis

Start Redis (required for the bot and worker). Example with Docker:

```powershell
docker run -d --name redis-shrimpy -p 6379:6379 redis:latest redis-server --requirepass YOUR_PASSWORD
```

Set `REDIS_PASSWORD` in `secrets.ini` to match. Use `REDIS_PORT=6379` unless you map a different port (set `REDIS_PORT` in the environment when starting the bot if needed).

---

## 8. FFmpeg (for render worker)

Install [FFmpeg](https://ffmpeg.org/) and ensure `ffmpeg` is on your `PATH`. The render worker uses it to produce videos.

---

## 9. Run the bot and worker

**Option A – dev script (Windows PowerShell)**

```powershell
.\scripts\dev.ps1 start
```

Use `.\scripts\dev.ps1 start-sync` once after adding or changing slash commands.

**Option B – manual**

```powershell
$env:REDIS_PORT="6379"
python -m bot.run
```

In a second terminal (with the same venv and `REDIS_PORT`):

```powershell
$env:REDIS_PORT="6379"
python -m bot.worker -q single dual
```

---

## 10. Sync slash commands (once)

After changing or adding slash commands:

```powershell
$env:SYNC_GUILD_ID="YOUR_GUILD_ID"
$env:REDIS_PORT="6379"
python -m bot.run --sync
```

Omit `SYNC_GUILD_ID` for global sync (slower, rate-limited).

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| `ModuleNotFoundError: No module named 'renderer'` | Run `pip install -r requirements.txt` again; ensure the `git+https://...minimap_renderer` line is installed. |
| `ModuleNotFoundError: No module named 'replay_parser'` | Usually installed with `minimap_renderer`. Use Python 3.10+ and reinstall requirements. |
| Redis connection refused | Start Redis (e.g. `docker start redis-shrimpy`) and check `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` in `secrets.ini` and in the environment when starting the bot. |
| Bot runs but render fails | Ensure FFmpeg is on `PATH` and the worker is running (`.\scripts\dev.ps1 status`). |
| `secrets.ini` not found | Copy `secrets_template.ini` to `secrets.ini` in the repo root and fill in values. |

---

## Summary checklist

- [ ] Python 3.10+ installed  
- [ ] Clone repo, `cd track`  
- [ ] Create virtual environment (`.venv`) and activate it  
- [ ] `pip install -r requirements.txt`  
- [ ] Copy `secrets_template.ini` → `secrets.ini` and fill in secrets  
- [ ] Run database setup (`python bot/utils/db.py` from repo root)  
- [ ] Start Redis  
- [ ] Install FFmpeg and ensure it’s on `PATH`  
- [ ] Start bot and worker (`.\scripts\dev.ps1 start` or manually)  
- [ ] (Once) Sync commands with `--sync` and optionally `SYNC_GUILD_ID`  

For updating game data after a WoWS patch, see [UPDATING.md](UPDATING.md).
