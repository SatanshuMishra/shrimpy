## Track

Track is a Discord bot with World of Warships utilities.

For a list of features, see the [commands reference](https://github.com/padtrack/track/wiki/Commands) in the wiki.

---

### Installation


1. Get Python 3.10 or higher

A virtual environment can be created with `python3.10 -m venv venv`.

2. Clone the repository

```
git clone https://github.com/padtrack/track.git
```

3. Install dependencies

```
cd track
pip install -U -r requirements.txt
```

4. Set up the database

```
python bot/utils/db.py
```

5. Create a `secrets.ini` file from `secrets_template.ini`

For more information about creating a Discord applications, see [this article](https://discordpy.readthedocs.io/en/stable/discord.html).

6. Install Redis

For more information, see [this article](https://redis.io/docs/getting-started/).

7. Install FFmpeg

For more information, see [this website](https://ffmpeg.org/).

8. Configure the project in `config.py`

Most of these can be left unchanged, but it is highly advised to change the values at the bottom.

9. You're set! For information about updating the bot between game updates, see [here](docs/UPDATING.md).

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
| `logs` | Stream bot logs from `generated/track-bot.log` (Ctrl+C to stop) |
| `worker-logs` | Stream worker logs from `generated/track-worker.log` (Ctrl+C to stop) |
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
docker start redis-track
# Or create new: docker run -d --name redis-track -p 6380:6379 redis:latest redis-server --requirepass track
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
docker exec redis-track redis-cli -a track FLUSHALL
```

**Check Service Status:**
```powershell
.\scripts\dev.ps1 status
# Or for Redis only: docker ps --filter "name=redis-track"
```

---

### License

This project is licensed under the GNU AGPLv3 License.

If you run a modified version of this bot for other users (e.g. as a hosted Discord bot),
the AGPLv3 generally requires that those users be able to obtain the Corresponding Source
for the version you are running.

This repository originated from `padtrack/track` (`https://github.com/padtrack/track`) and includes modifications.

---

### Credits and Links

- [@alpha#9432](https://github.com/0alpha) - Thank you for your invaluable insight and help with the OAuth 2.0 server!
- [@TenguBlade#3158](https://www.reddit.com/user/TenguBlade/) - Thank you for your help with the guess similarity groups!
- @dmc#3518 - Thank you for your help with the builds!
- The Minimap Renderer's repository is available [here](https://github.com/WoWs-Builder-Team/minimap_renderer).
