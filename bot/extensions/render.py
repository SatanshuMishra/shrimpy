import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import io
import json
import logging
import shutil
import ssl
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
import zoneinfo
from zoneinfo import ZoneInfo
from typing import Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")

# Thread pool for running sync Redis/RQ calls off the event loop
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="redis_rq")


async def _run_sync(func: Callable[..., T], *args, **kwargs) -> T:
    """Run a sync function in the thread pool to avoid blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor, functools.partial(func, *args, **kwargs)
    )

import discord
import redis
import rq
import rq.job
import rq.worker
from rq import Retry
from discord import app_commands, ui
from discord.ext import commands

from bot import tasks
from bot.shrimpy import Shrimpy
from bot.utils import db, errors, functions
from bot.utils.replay_stats import BattleStats, aggregate_win_rate, format_win_rate_line
from config import cfg

logger = logging.getLogger("shrimpy")
_url = f"redis://:{cfg.redis.password}@{cfg.redis.host}:{cfg.redis.port}/"
_redis: redis.Redis = redis.from_url(_url)
_async_redis: redis.asyncio.Redis = redis.asyncio.from_url(_url)

UNKNOWN_JOB_STATUS_RETRY = 5
URL_MAX_LENGTH = 512
BATCH_MAX_REPLAYS = 10  # Discord message attachment limit

# SECURITY: HTTP timeout configuration to prevent hanging requests (DoS mitigation)
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)

# SECURITY: Maximum file size for replay downloads (50 MB) to prevent DoS/memory exhaustion
MAX_REPLAY_DOWNLOAD_BYTES = 50 * 1024 * 1024

# SECURITY: Maximum replay attachment size (50 MB) - replays are typically < 5 MB
MAX_REPLAY_ATTACHMENT_BYTES = 50 * 1024 * 1024

# SECURITY: Maximum video output size (25 MB) to prevent Redis/memory exhaustion
# This matches Discord's max message total; individual files can be 8 MB
MAX_VIDEO_OUTPUT_BYTES = 25 * 1024 * 1024

# SECURITY: Allowed domains for tournament replay URLs (SSRF mitigation)
# Only these domains are permitted for automatic URL fetching from messages
ALLOWED_REPLAY_DOMAINS = frozenset({
    "wows-tournaments.com",
    "api.wows-tournaments.com",
    "cdn.wows-tournaments.com",
    "replays.wows-numbers.com",
    "replayswows.com",
})

# SECURITY: Allowed domains for callback URLs (SSRF/exfiltration mitigation)
ALLOWED_CALLBACK_DOMAINS = frozenset({
    "wows-tournaments.com",
    "api.wows-tournaments.com",
})


def _is_url_allowed(url: str, allowed_domains: frozenset) -> bool:
    """
    SECURITY: Validate that a URL's domain is in the allowlist.
    Prevents SSRF attacks by blocking requests to internal IPs, cloud metadata, etc.
    """
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname
        if not host:
            return False
        # Check if the hostname matches or is a subdomain of any allowed domain
        host_lower = host.lower()
        for domain in allowed_domains:
            if host_lower == domain or host_lower.endswith(f".{domain}"):
                return True
        return False
    except Exception:
        return False
BATCH_PENDING_PREFIX = "renderbatch_pending:"
BATCH_PENDING_TTL = 60  # seconds to reply with files before prompt shows timeout
BATCH_POST_RESULT_ATTEMPTS = 5
BATCH_POST_RESULT_RETRY_DELAY_BASE = 1  # base seconds for exponential backoff
FAILED_TO_POST_MSG = "Could not post result (network error)."
# Discord: 8 MB per file; 25 MB total per message (use 8 MB to stay safe for all clients)
DISCORD_MAX_FILE_BYTES = 8 * 1024 * 1024
DISCORD_MAX_MESSAGE_BYTES = 25 * 1024 * 1024
DISCORD_EPHEMERAL_FLAG = 64
# Minimum interval between non-terminal status embed edits (seconds) to avoid rate limits
STATUS_EMBED_EDIT_MIN_INTERVAL = 1.5
# Fallback: poll RQ job status every N seconds if pubsub never fires (avoids blocking forever)
BATCH_POLL_STATUS_FALLBACK_SECONDS = 15
# Min/max time to wait for one batch job (from batch start). Per-job timeout is scaled by batch size
# so later jobs (e.g. 8–10) have enough wall time; otherwise all tasks share one 30-min window.
# Industry standard: allocate 10 min per job (renders average 3–6 min, with buffer for slow replays).
BATCH_POLL_MAX_WAIT_MIN_SECONDS = 30 * 60        # 30 min minimum
BATCH_POLL_MAX_WAIT_PER_JOB_SECONDS = 10 * 60    # 10 min per job (generous buffer)
BATCH_POLL_MAX_WAIT_CAP_SECONDS = 150 * 60       # 2.5 hours max

# Batch job reliability: RQ retry configuration
# Industry standard: 3 retries to handle transient failures.
# Note: Windows workers don't support scheduled delays (no SIGALRM), so use immediate retries.
# The recovery loop provides additional safety net after all retries are exhausted.
BATCH_JOB_RETRY_MAX = 3

# Batch job TTL: how long a job can stay queued before RQ discards it.
# Must be long enough for all jobs in the batch to START (not finish).
# Formula: (queue_depth + batch_size) * per_job_execution_time + buffer
BATCH_JOB_TTL_PER_JOB_SECONDS = 10 * 60  # 10 min per job in queue
BATCH_JOB_TTL_MIN_SECONDS = 60 * 60      # 1 hour minimum

# Batch job result TTL: how long finished results stay in Redis.
# Must be long enough for recovery to fetch results after all poll tasks exit.
# Industry standard: keep results for entire batch duration + buffer.
BATCH_JOB_RESULT_TTL_SECONDS = 90 * 60   # 90 minutes (longer than max batch time)
BATCH_JOB_FAILURE_TTL_SECONDS = 90 * 60  # 90 minutes

# Recovery: how long to wait for still-running jobs before giving up.
# After poll tasks exit, recovery loops and waits for jobs that are "started" in RQ.
BATCH_RECOVERY_MAX_WAIT_SECONDS = 30 * 60   # 30 min max wait in recovery loop
BATCH_RECOVERY_POLL_INTERVAL_SECONDS = 10   # Check every 10s for job completion

# Shared aiohttp session for webhook/ephemeral requests (lazy init, reused)
_http_session: Optional[aiohttp.ClientSession] = None


async def _get_http_session() -> aiohttp.ClientSession:
    """Get or create the shared aiohttp session for webhook requests."""
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=HTTP_TIMEOUT)
    return _http_session

# Exceptions that indicate transient network/SSL errors and warrant retry
_NETWORK_RETRY_EXC = (OSError, ssl.SSLError, discord.HTTPException, aiohttp.ClientError)


def _pack_into_buckets(
    items: list[tuple[int, str, bytes]],
    max_total_bytes: int,
) -> list[list[tuple[int, str, bytes]]]:
    """
    Pack (result_index, out_name, data) items into as few buckets as possible,
    each bucket's total size <= max_total_bytes. First-fit.
    """
    buckets: list[list[tuple[int, str, bytes]]] = []
    current: list[tuple[int, str, bytes]] = []
    current_size = 0
    for item in items:
        i, out_name, data = item
        size = len(data)
        if current_size + size > max_total_bytes and current:
            buckets.append(current)
            current = []
            current_size = 0
        current.append(item)
        current_size += size
    if current:
        buckets.append(current)
    return buckets


async def _send_with_retry(
    max_attempts: int,
    backoff_base: float,
    send_fn: Callable[[], Awaitable[T]],
    log_context: str = "",
) -> Optional[T]:
    """
    Run an async send operation with exponential backoff on network/SSL/HTTP errors.
    Returns the result of send_fn() or None if all attempts failed.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return await send_fn()
        except _NETWORK_RETRY_EXC as e:
            last_exc = e
            logger.warning(
                "%s attempt %d/%d failed: %s",
                log_context,
                attempt + 1,
                max_attempts,
                e,
            )
        except Exception as e:
            logger.warning(
                "%s attempt %d/%d unexpected error: %s",
                log_context,
                attempt + 1,
                max_attempts,
                e,
            )
            last_exc = e
        if attempt < max_attempts - 1:
            delay = backoff_base * (2**attempt)
            await asyncio.sleep(delay)
    if last_exc:
        logger.debug("All retries exhausted for %s: %s", log_context, last_exc)
    return None

# Replay filename format: YYYYMMDD_HHMMSS_ShipIndex_MapCode_MapName.wowsreplay
# ShipIndex: Code-Ship-Name (first hyphen separates code; ship name has hyphens for spaces)
# MapName: underscore-separated parts, map code first then map name parts

# Replay map name -> actual map name (lowercase keys for lookup); add new entries to replay_map_names.json
_REPLAY_MAP_NAMES_PATH = Path(__file__).parent / "replay_map_names.json"
_REPLAY_MAP_NAMES: dict[str, str] = {}
if _REPLAY_MAP_NAMES_PATH.exists():
    try:
        _REPLAY_MAP_NAMES = json.loads(_REPLAY_MAP_NAMES_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not load replay map names: %s", e)


def _format_map_display(replay_map_name: str) -> str:
    """
    Return display string for map: "Actual Name (Replay Name)" when a mapping exists,
    otherwise the replay map name as-is.
    """
    if not replay_map_name:
        return replay_map_name
    key = replay_map_name.lower().strip()
    actual = _REPLAY_MAP_NAMES.get(key)
    if actual:
        return f"{actual} ({replay_map_name})"
    return replay_map_name


def _format_full_date(date_str: str) -> str:
    """
    Format date string YYYY-MM-DD as "Wednesday, January 15th, 2026".
    Returns date_str unchanged if parsing fails.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day = dt.day
        suffix = (
            "th"
            if 11 <= day % 100 <= 13
            else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        )
        return f"{dt:%A, %B} {day}{suffix}, {dt:%Y}"
    except (ValueError, TypeError):
        return date_str


def _parse_replay_filename(filename: str) -> Optional[dict]:
    """
    Parse a .wowsreplay filename into date, time, ship_name, map_name.
    Returns None if parsing fails.
    """
    if not filename or not filename.lower().endswith(".wowsreplay"):
        return None
    base = filename[:-10].strip()  # drop .wowsreplay
    parts = base.split("_")
    if len(parts) < 5:
        return None
    date_str = parts[0]
    time_str = parts[1]
    ship_index = parts[2]
    # map_code = parts[3], map name = parts[4:] joined
    if len(date_str) != 8 or not date_str.isdigit():
        return None
    if len(time_str) != 6 or not time_str.isdigit():
        return None
    try:
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        time_formatted = f"{time_str[:2]}:{time_str[2:4]}"
        # Full time with seconds for Discord dynamic timestamp (short-time only).
        time_full = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
    except (IndexError, TypeError):
        return None
    # ShipIndex: first hyphen separates code from ship name; ship name uses hyphens for spaces
    idx = ship_index.find("-")
    if idx < 0:
        return None
    ship_name = ship_index[idx + 1 :].replace("-", " ").strip().rstrip(".")
    if not ship_name:
        return None
    map_name_parts = parts[4:]
    map_name = " ".join(map_name_parts).replace("_", " ").strip().title() if map_name_parts else parts[3]
    map_name = map_name.rstrip(".").strip()  # strip trailing period (e.g. from extension bleed)
    return {
        "date": date_formatted,
        "time": time_formatted,
        "time_full": time_full,
        "ship_name": ship_name,
        "map_name": map_name,
    }


def _replay_sort_key(filename: str) -> tuple:
    """
    Return a sort key (date, time) for chronological ordering (earliest first).
    Unparseable filenames sort to the end (use a high sentinel).
    """
    parsed = _parse_replay_filename(filename)
    if not parsed:
        return ("9999-99-99", "99:99")
    return (parsed["date"], parsed["time"])


def format_replay_display(filename: str) -> str:
    """
    Return a neat one-line display for the replay (parsed): Date Time · Ship · Map.
    Falls back to full filename if parse fails.
    """
    parsed = _parse_replay_filename(filename)
    if not parsed:
        return filename
    map_display = _format_map_display(parsed["map_name"])
    return (
        f"{parsed['date']} {parsed['time']} · {parsed['ship_name']} · {map_display}"
    )


def _replay_datetime_to_unix(date_str: str, time_str: str, tz_name: str) -> Optional[int]:
    """
    Interpret date_str (YYYY-MM-DD) and time_str (HH:MM:SS or HH:MM) in the given IANA timezone,
    return Unix timestamp (seconds since epoch, UTC).
    Uses full datetime from game (including seconds). Falls back to UTC if tz_name invalid.
    Returns None only if date/time parsing fails.
    """
    dt_naive = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt_naive = datetime.strptime(f"{date_str} {time_str}", fmt)
            break
        except (ValueError, TypeError):
            continue
    if dt_naive is None:
        return None
    for tz_try in (tz_name or "UTC", "UTC"):
        try:
            tz = ZoneInfo(tz_try)
            return int(dt_naive.replace(tzinfo=tz).timestamp())
        except Exception:
            continue
    # Last resort: interpret as UTC so we always return a timestamp (Discord short-time).
    return int(dt_naive.replace(tzinfo=timezone.utc).timestamp())


def _format_unix_time(unix_ts: int, tz_name: str) -> Optional[str]:
    """
    Convert a Unix timestamp to a short time string in tz_name.
    Returns None only if conversion fails.
    """
    try:
        tz = ZoneInfo(tz_name)
        tz_label = tz_name
    except Exception:
        tz = timezone.utc
        tz_label = "UTC"
    try:
        dt = datetime.fromtimestamp(unix_ts, tz=tz)
    except (OSError, OverflowError, ValueError):
        return None
    abbr = dt.strftime("%Z") or tz_label
    return f"{dt:%H:%M} {abbr}".strip()


def _format_unix_datetime(unix_ts: int, tz_name: str) -> Optional[str]:
    """
    Convert a Unix timestamp to a date+time string in tz_name.
    Returns None only if conversion fails.
    """
    try:
        tz = ZoneInfo(tz_name)
        tz_label = tz_name
    except Exception:
        tz = timezone.utc
        tz_label = "UTC"
    try:
        dt = datetime.fromtimestamp(unix_ts, tz=tz)
    except (OSError, OverflowError, ValueError):
        return None
    abbr = dt.strftime("%Z") or tz_label
    return f"{dt:%Y-%m-%d} {dt:%H:%M} {abbr}".strip()


def _infer_timezone_from_upload(filename: str, upload_timestamp: float) -> str:
    """
    Infer the uploader's timezone from the first replay filename and when they uploaded.
    The replay time in the filename is in the uploader's local time; the message was sent
    at upload_timestamp (UTC). The replay moment must be *before* the upload (you can't
    upload before the game happened). So we only consider TZ where replay_ts <= upload_ts,
    and pick the one where replay is closest to but not after upload — i.e. smallest
    (upload_ts - replay_ts). That way we get "when they played" in their TZ; Discord
    then shows that moment in each viewer's local time.
    """
    parsed = _parse_replay_filename(filename)
    if not parsed:
        return "UTC"
    time_for_ts = parsed.get("time_full") or parsed["time"]
    dt_naive = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt_naive = datetime.strptime(
                f"{parsed['date']} {time_for_ts}", fmt
            )
            break
        except (ValueError, TypeError):
            continue
    if dt_naive is None:
        return "UTC"
    best_tz = "UTC"
    best_gap = float("inf")  # upload_ts - replay_ts (only when replay_ts <= upload_ts)
    # Fallback when replay is "same local day" but already next day in UTC (e.g. Hawaii evening).
    best_future_gap = float("inf")  # replay_ts - upload_ts when 0 < replay_ts - upload_ts <= 14h
    best_future_tz = "UTC"
    try:
        for tz_name in zoneinfo.available_timezones():
            try:
                tz = ZoneInfo(tz_name)
                ts = int(dt_naive.replace(tzinfo=tz).timestamp())
                if ts <= upload_timestamp:
                    gap = upload_timestamp - ts
                    if gap < best_gap:
                        best_gap = gap
                        best_tz = tz_name
                elif 0 < ts - upload_timestamp <= 14 * 3600:
                    fgap = ts - upload_timestamp
                    if fgap < best_future_gap:
                        best_future_gap = fgap
                        best_future_tz = tz_name
            except Exception:
                continue
    except Exception:
        pass
    if best_gap != float("inf"):
        return best_tz
    if best_future_gap != float("inf"):
        return best_future_tz
    return best_tz


def format_replay_summary_line(
    filename: str,
    tz_override: Optional[str] = None,
    unix_ts: Optional[int] = None,
) -> str:
    """
    One-line for include_summary: Discord dynamic timestamp <t:unix:t> so each viewer
    sees the time in their local timezone. When unix_ts is None we derive it from
    filename + tz_override (or UTC). Map uses _format_map_display.
    """
    parsed = _parse_replay_filename(filename)
    if not parsed:
        return format_replay_display(filename)
    tz_name = tz_override or "UTC"
    if unix_ts is None:
        time_for_ts = parsed.get("time_full") or parsed["time"]
        unix_ts = _replay_datetime_to_unix(parsed["date"], time_for_ts, tz_name)
        logger.debug(
            "Summary line from filename: tz=%s unix_ts=%s file=%s",
            tz_name,
            unix_ts,
            filename[:50],
        )
    else:
        logger.debug(
            "Summary line from metadata: unix_ts=%s file=%s",
            unix_ts,
            filename[:50],
        )
    map_display = _format_map_display(parsed["map_name"])
    if unix_ts is not None:
        return f"<t:{unix_ts}:t> · {map_display}"
    return format_replay_display(filename)


def _format_summary_overall_stats(batch_battle_stats: Optional[list]) -> str:
    """Build '**Overall Statistics**:\\n\\nWin-Rate: 60%' block for include_summary. Only titles bold."""
    if not batch_battle_stats:
        return ""
    wins, total, rate = aggregate_win_rate(batch_battle_stats)
    if total == 0 or rate is None:
        return ""
    return f"**Overall Statistics**:\n\nWin-Rate: {rate:.0f}%"


WOWS_TOURNAMENTS_CHANNELS = [
    1095389506200420483,  # website-replay-api
    1096583194284916886,  # sekrit-bot-stuff
    1234992264661569606,  # sekrit-bot-stuff (WM)
]
WOWS_TOURNAMENTS_USERS = [
    197139421290561536,  # Stewie
    212466672450142208,  # Trackpad
    439350471015268356,  # Test Bot
    1004877222760415292,  # Live Bot
]


def track_task_request(f):
    async def wrapped(self, *args, **kwargs):
        if self._interaction:
            await _async_redis.set(
                f"task_request_{self._interaction.user.id}", "", ex=600
            )
        try:
            return await f(self, *args, **kwargs)
        except Exception as e:
            logger.error("Render tracking failed", exc_info=e)
        finally:
            if self._interaction:
                await _async_redis.delete(f"task_request_{self._interaction.user.id}")

    return wrapped


class BuildsButton(ui.Button):
    def __init__(self, builds: list[dict], **kwargs):
        super().__init__(label="View Builds", **kwargs)

        self.fp = io.StringIO()
        for build in builds:
            if clan := build["clan"]:
                self.fp.write(f"[{clan}] ")
            self.fp.write(build["name"] + " (" + build["ship"] + ")\n" + build["build_url"] + "&ref=shrimpy" + "\n\n")
        self.fp.seek(0)

    async def callback(self, interaction: discord.Interaction) -> None:
        # noinspection PyTypeChecker
        file = discord.File(self.fp, filename="builds.txt")
        await functions.reply(interaction, file=file, ephemeral=True)
        self.fp.seek(0)


class ChatButton(ui.Button):
    def __init__(self, chat: str, **kwargs):
        super().__init__(label="Export Chat", **kwargs)

        self.fp = io.StringIO(chat)
        self.fp.seek(0)

    async def callback(self, interaction: discord.Interaction) -> None:
        # noinspection PyTypeChecker
        file = discord.File(self.fp, filename="chat.txt")
        await functions.reply(interaction, file=file, ephemeral=True)
        self.fp.seek(0)


class RenderView(ui.View):
    def __init__(self, builds: list[dict], chat: str, **kwargs):
        super().__init__(**kwargs)

        for build in builds:
            if build["relation"] == -1 and build["build_url"]:
                url = build["build_url"] + "&ref=shrimpy"
                if len(url) <= URL_MAX_LENGTH:
                    self.add_item(ui.Button(label="Player Build", url=url))
                break

        self.builds_button = BuildsButton(builds)
        self.add_item(self.builds_button)
        self.chat_button = ChatButton(chat)
        self.add_item(self.chat_button)
        self.message: Optional[discord.Message] = None

    async def on_timeout(self) -> None:
        self.builds_button.disabled = True
        self.chat_button.disabled = True
        await self.message.edit(view=self)


class Render:
    DEFAULT_FPS = 30
    DEFAULT_QUALITY = 7
    MAX_WAIT_TIME = 180
    FINISHED_TTL = 600
    COOLDOWN = 1
    QUEUE_SIZE = 10

    # Same rectangle family: filled vs hollow (Discord can't color one character green)
    PROGRESS_FOREGROUND = "▰"  # filled square
    PROGRESS_BACKGROUND = "▱"  # hollow square (same shape, outline)

    QUEUE: rq.Queue

    def __init__(self, bot: Shrimpy, interaction: Optional[discord.Interaction]):
        self._bot = bot
        self._interaction = interaction
        self._job: Optional[rq.job.Job] = None

    @property
    def job_position(self) -> int:
        """Sync property - use async_job_position in async code."""
        pos = self._job.get_position()
        return pos + 1 if pos else 1

    async def async_job_position(self) -> int:
        """Get job position without blocking the event loop."""
        pos = await _run_sync(self._job.get_position)
        return pos + 1 if pos else 1

    @property
    def job_ttl(self) -> int:
        return max(self.QUEUE.count, 1) * self.MAX_WAIT_TIME

    async def _reupload(
        self, task_status: Optional[str], exc_info: Optional[str]
    ) -> None:
        raise NotImplementedError()

    async def _check(self) -> bool:
        # Run sync RQ calls in executor to avoid blocking event loop
        worker_count = await _run_sync(
            rq.worker.Worker.count, connection=_redis, queue=self.QUEUE
        )
        queue_count = await _run_sync(lambda: self.QUEUE.count)
        cooldown = await _async_redis.ttl(f"cooldown_{self._interaction.user.id}")

        try:
            assert (
                worker_count != 0
            ), f"{self._interaction.user.mention} No running workers detected."
            assert (
                queue_count <= self.QUEUE_SIZE
            ), f"{self._interaction.user.mention} Queue full. Please try again later."
            assert (
                cooldown <= 0
            ), f"{self._interaction.user.mention} You're on render cooldown until <t:{int(time.time()) + cooldown}:R>."
            assert not await _async_redis.exists(
                f"task_request_{self._interaction.user.id}"
            ), f"{self._interaction.user.mention} You have an ongoing/queued render. Please try again later."
            return True
        except AssertionError as e:
            await functions.reply(self._interaction, str(e), ephemeral=True)
            return False

    async def message(self, **kwargs) -> discord.Message:
        return await functions.reply(self._interaction, **kwargs)

    async def on_success(self, data: bytes, message: discord.Message) -> None:
        return

    @track_task_request
    async def poll_result(
        self, input_name: str, filename: Optional[str] = None
    ) -> None:
        initial_position = await self.async_job_position()
        summary_tz = "UTC"
        try:
            user = await db.User.get_or_create(id=self._interaction.user.id)
            user_tz = getattr(user, "replay_timezone", None)
            if user_tz:
                summary_tz = user_tz
        except Exception as e:
            logger.debug(
                "Could not load user timezone for %s: %s",
                self._interaction.user.id,
                e,
            )
        embed = RenderWaitingEmbed(
            input_name,
            initial_position,
            filename=filename,
            summary_tz=summary_tz,
        )
        message = await self.message(embed=embed)
        last_position: Optional[int] = None
        last_progress: Optional[float] = None
        last_edit_time: float = time.time()

        psub = _async_redis.pubsub()
        psub.ignore_subscribe_messages = True
        await psub.psubscribe(f"*{self._job.id}")

        async for response in psub.listen():
            if response["type"] != "pmessage":
                continue

            # Run sync RQ call in executor to avoid blocking event loop
            status = await _run_sync(self._job.get_status, refresh=True)
            match status:
                case "queued":
                    position = await self.async_job_position()
                    if position == last_position:
                        continue
                    # Debounce: skip edit if too soon (unless first update)
                    now = time.time()
                    if now - last_edit_time < STATUS_EMBED_EDIT_MIN_INTERVAL:
                        continue

                    embed = RenderWaitingEmbed(
                        input_name,
                        position,
                        filename=filename,
                        summary_tz=summary_tz,
                    )
                    message = await message.edit(embed=embed)
                    last_position = position
                    last_edit_time = now
                case "started":
                    meta = await _run_sync(self._job.get_meta, refresh=True)
                    progress = meta.get("progress", 0.0)
                    task_status = meta.get("status", None)
                    if progress == last_progress:
                        continue
                    # Debounce: skip edit if too soon
                    now = time.time()
                    if now - last_edit_time < STATUS_EMBED_EDIT_MIN_INTERVAL:
                        continue

                    embed = RenderStartedEmbed(
                        input_name,
                        progress,
                        task_status=task_status,
                        filename=filename,
                        summary_tz=summary_tz,
                    )
                    message = await message.edit(embed=embed)
                    last_progress = progress
                    last_edit_time = now
                case "finished":
                    if not self._job.result:
                        continue

                    if isinstance(self._job.result, tuple):
                        # Unpack 5 or 6 elements (6th is battle_stats, unused in single render)
                        data, result_fn, time_taken, builds_str, chat = self._job.result[:5]
                        replay_ts = (
                            self._job.result[6] if len(self._job.result) >= 7 else None
                        )
                        # When user set timezone, use filename+tz so the moment is correct.
                        summary_unix_ts = (
                            None
                            if (summary_tz and summary_tz != "UTC")
                            else replay_ts
                        )
                        # battle_stats = self._job.result[5] if len(self._job.result) >= 6 else None

                        try:
                            file = discord.File(io.BytesIO(data), f"{result_fn}.mp4")
                            if builds_str:
                                view = RenderView(json.loads(builds_str), chat)
                                sent_message = await self.message(
                                    content=None,
                                    file=file,
                                    view=view,
                                )
                                view.message = sent_message
                            else:
                                sent_message = await self.message(
                                    content=None, file=file
                                )
                                await self.on_success(data, sent_message)
                            embed = RenderSuccessEmbed(
                                input_name,
                                sent_message,
                                time_taken,
                                filename=filename,  # original replay filename for summary
                                summary_tz=summary_tz,
                                unix_ts=summary_unix_ts,
                            )
                        except discord.HTTPException:
                            embed = RenderFailureEmbed(
                                input_name,
                                "Rendered file too large (>8 MB). Consider reducing quality.",
                                filename=filename,
                                summary_tz=summary_tz,
                                unix_ts=summary_unix_ts,
                            )
                    elif isinstance(self._job.result, errors.RenderError):
                        embed = RenderFailureEmbed(
                            input_name,
                            self._job.result.message,
                            filename=filename,
                            summary_tz=summary_tz,
                        )
                    else:
                        logger.error(f"Unhandled job result {self._job.result}")
                        embed = RenderFailureEmbed(
                            input_name,
                            "An unhandled error occurred.",
                            filename=filename,
                            summary_tz=summary_tz,
                        )

                    await message.edit(embed=embed)
                    break
                case "failed":
                    meta = await _run_sync(self._job.get_meta, refresh=True)
                    timeout = meta.get("timeout", None)
                    if timeout:
                        logger.warning("Render job timed out")
                        embed = RenderFailureEmbed(
                            input_name,
                            "Job timed out.",
                            filename=filename,
                            summary_tz=summary_tz,
                        )
                    else:
                        # fetch again to update exc_info (in executor)
                        job = await _run_sync(
                            rq.job.Job.fetch, self._job.id, connection=_redis
                        )
                        task_status = self._job.meta.get("status", None)
                        logger.error(
                            f'Render job failed with status "{task_status}"\n{job.exc_info}'
                        )
                        if "StopIteration" in job.exc_info:
                            err_message = "An unhandled error occurred (likely incomplete replay)."
                        else:
                            err_message = "An unhandled error occurred."

                        embed = RenderFailureEmbed(
                            input_name,
                            err_message,
                            filename=filename,
                            summary_tz=summary_tz,
                        )
                        await self._reupload(task_status, job.exc_info)

                    await message.edit(embed=embed)
                    break
                case _base:
                    if status is None:
                        await asyncio.sleep(UNKNOWN_JOB_STATUS_RETRY)
                        status = await _run_sync(self._job.get_status, refresh=True)
                        if status is not None:
                            continue

                    logger.warning(f"Unknown job status {status}")
                    embed = RenderFailureEmbed(
                        input_name,
                        "Render job expired.",
                        filename=filename,
                        summary_tz=summary_tz,
                    )
                    await message.edit(embed=embed)
                    break

        await psub.unsubscribe()
        await psub.close()


class RenderSingle(Render):
    QUEUE = rq.Queue("single", connection=_redis)

    def __init__(
        self,
        bot: Shrimpy,
        interaction: discord.Interaction,
        attachment: discord.Attachment,
    ):
        super().__init__(bot, interaction)

        self._attachment = attachment

    async def _reupload(
        self, task_status: Optional[str], exc_info: Optional[str]
    ) -> None:
        try:
            with (
                io.BytesIO() as fp,
                io.StringIO(f"Task Status: {task_status}\n\n{exc_info}\n") as report,
            ):
                await self._attachment.save(fp)

                channel = await self._bot.fetch_channel(cfg.channels.failed_renders)
                # noinspection PyTypeChecker
                await channel.send(
                    files=[
                        discord.File(report, filename="report.txt"),
                        discord.File(fp, filename=self._attachment.filename),
                    ]
                )
        except (discord.HTTPException, discord.NotFound):
            logger.error(
                f"Failed to reupload render with interaction ID {self._interaction.id}"
            )

    async def start(self, *args) -> None:
        if not await self._check():
            return

        # SECURITY: Validate attachment size to prevent DoS/memory exhaustion
        if self._attachment.size > MAX_REPLAY_ATTACHMENT_BYTES:
            await functions.reply(
                self._interaction,
                f"Replay file too large ({self._attachment.size / 1024 / 1024:.1f} MB). "
                f"Maximum allowed: {MAX_REPLAY_ATTACHMENT_BYTES / 1024 / 1024:.0f} MB.",
                ephemeral=True,
            )
            return

        await self._interaction.response.defer()

        with io.BytesIO() as fp:
            await self._attachment.save(fp)
            fp.seek(0)

            arguments = [self._interaction.user.id, self.COOLDOWN, fp.read()]
            arguments.extend(args)
            self._job = self.QUEUE.enqueue(
                tasks.render_single,
                args=arguments,
                failure_ttl=self.FINISHED_TTL,
                result_ttl=self.FINISHED_TTL,
                ttl=self.job_ttl,
            )

        display_name = format_replay_display(self._attachment.filename)
        self._bot.loop.create_task(
            self.poll_result(display_name, self._attachment.filename)
        )


class RenderDual(Render):
    QUEUE = rq.Queue("dual", connection=_redis)

    def __init__(
        self,
        bot: Shrimpy,
        interaction: discord.Interaction,
        attachment1: discord.Attachment,
        attachment2: discord.Attachment,
    ):
        super().__init__(bot, interaction)

        self._attachment1 = attachment1
        self._attachment2 = attachment2

    async def _reupload(
        self, task_status: Optional[str], exc_info: Optional[str]
    ) -> None:
        try:
            with (
                io.BytesIO() as fp1,
                io.BytesIO() as fp2,
                io.StringIO(f"Task Status: {task_status}\n\n{exc_info}\n") as report,
            ):
                await self._attachment1.save(fp1)
                await self._attachment2.save(fp2)

                channel = await self._bot.fetch_channel(cfg.channels.failed_renders)
                # noinspection PyTypeChecker
                await channel.send(
                    files=[
                        discord.File(report, filename="report.txt"),
                        discord.File(fp1, filename=self._attachment1.filename),
                        discord.File(fp2, filename=self._attachment2.filename),
                    ],
                )
        except (discord.HTTPException, discord.NotFound):
            logger.error(
                f"Failed to reupload render with interaction ID {self._interaction.id}"
            )

    async def start(self, *args) -> None:
        if not await self._check():
            return

        # SECURITY: Validate attachment sizes to prevent DoS/memory exhaustion
        total_size = self._attachment1.size + self._attachment2.size
        if total_size > MAX_REPLAY_ATTACHMENT_BYTES * 2:
            await functions.reply(
                self._interaction,
                f"Combined replay files too large ({total_size / 1024 / 1024:.1f} MB). "
                f"Maximum allowed: {MAX_REPLAY_ATTACHMENT_BYTES * 2 / 1024 / 1024:.0f} MB.",
                ephemeral=True,
            )
            return

        await self._interaction.response.defer()

        with io.BytesIO() as fp1, io.BytesIO() as fp2:
            await self._attachment1.save(fp1)
            fp1.seek(0)
            await self._attachment2.save(fp2)
            fp2.seek(0)

            arguments = [
                self._interaction.user.id,
                self.COOLDOWN,
                fp1.read(),
                fp2.read(),
            ]
            arguments.extend(args)
            self._job = self.QUEUE.enqueue(
                tasks.render_dual,
                args=arguments,
                failure_ttl=self.FINISHED_TTL,
                result_ttl=self.FINISHED_TTL,
                ttl=self.job_ttl,
            )

        self._bot.loop.create_task(self.poll_result(f"{args[2]} vs. {args[3]}"))


class RenderWT(Render):
    QUEUE = rq.Queue("dual", connection=_redis)

    def __init__(
        self,
        bot: Shrimpy,
        data1: bytes,
        data2: bytes,
        output_channel: int,
        callback_url: Optional[str],
    ):
        super().__init__(bot, None)

        self._data1 = data1
        self._data2 = data2
        self.output_channel = output_channel
        self.callback_url = callback_url

    async def _reupload(
        self, task_status: Optional[str], exc_info: Optional[str]
    ) -> None:
        try:
            with (
                io.StringIO(f"Task Status: {task_status}\n\n{exc_info}\n") as report,
            ):
                channel = await self._bot.fetch_channel(cfg.channels.failed_renders)
                # noinspection PyTypeChecker
                await channel.send(
                    files=[
                        discord.File(report, filename="report.txt"),
                    ],
                )
        except (discord.HTTPException, discord.NotFound):
            logger.error(
                f"Failed to reupload wows-tournaments render with interaction ID {self._interaction.id}"
            )

    async def _check(self) -> bool:
        return True

    async def message(self, **kwargs) -> discord.Message:
        channel = await self._bot.fetch_channel(self.output_channel)
        return await channel.send(**kwargs)

    async def on_success(self, data: bytes, message: discord.Message) -> None:
        if self.callback_url:
            # SECURITY: Validate callback URL is in the allowlist (SSRF mitigation)
            if not _is_url_allowed(self.callback_url, ALLOWED_CALLBACK_DOMAINS):
                logger.warning(
                    "Blocked callback to non-allowlisted URL: %s", self.callback_url
                )
                return
            session = await _get_http_session()
            await session.post(
                f"{self.callback_url}&messageId={message.id}",
            )

    async def start(self, name_a: str, name_b: str) -> None:
        if not await self._check():
            return

        arguments = [
            0,
            self.COOLDOWN,
            self._data1,
            self._data2,
        ]
        arguments.extend((20, 9, name_a, name_b, False))
        self._job = self.QUEUE.enqueue(
            tasks.render_dual,
            args=arguments,
            failure_ttl=self.FINISHED_TTL,
            result_ttl=self.FINISHED_TTL,
            ttl=self.job_ttl,
        )

        self._bot.loop.create_task(self.poll_result(f"{name_a} vs. {name_b}"))


def _format_render_summary(
    filename: Optional[str],
    tz_override: Optional[str] = None,
    unix_ts: Optional[int] = None,
) -> Optional[str]:
    """Build '**Date Time:** <t:unix:R>' summary so Discord shows time in viewer's local TZ."""
    parsed = _parse_replay_filename(filename) if filename else None
    if not parsed:
        return None
    tz_name = tz_override or "UTC"
    if unix_ts is None:
        time_for_ts = parsed.get("time_full") or parsed["time"]
        unix_ts = _replay_datetime_to_unix(parsed["date"], time_for_ts, tz_name)
    date_time = f"<t:{unix_ts}:R>" if unix_ts is not None else f"{parsed['date']} {parsed['time']}"
    map_display = _format_map_display(parsed["map_name"])
    return (
        f"**Date Time:** {date_time}\n"
        f"**Ship:** {parsed['ship_name']}\n"
        f"**Map:** {map_display}"
    )


# Progress bar spans full footer width; ~30 blocks matches typical Discord embed width
BATCH_PROGRESS_BAR_WIDTH = 30


def _batch_progress_footer(
    done: int, total: int, progress_fraction: Optional[float] = None
) -> str:
    """
    Build footer text for batch processing embed: full-width progress bar + 'done/total replays'.
    Bar reflects actual progress: if progress_fraction is set, use it (0.0–1.0); else use done/total.
    """
    width = BATCH_PROGRESS_BAR_WIDTH
    if total <= 0:
        frac = 0.0
    elif progress_fraction is not None:
        frac = max(0.0, min(1.0, progress_fraction))
    else:
        frac = done / total
    filled = min(width, round(frac * width))
    empty = width - filled
    bar = (
        f"{Render.PROGRESS_FOREGROUND * filled}"
        f"{Render.PROGRESS_BACKGROUND * empty}"
    )
    return f"{bar} {done}/{total} replays"


class RenderEmbed(discord.Embed):
    TITLE_STARTING = "Render Process Starting"
    TITLE_PROCESSING = "Render Processing"
    TITLE_SUMMARY = "Render Summary"
    # Batch mode: three dialogs, same colors as single-render
    BATCH_TITLE_PROMPT = "Provide Render Files 👀"
    BATCH_TITLE_PROCESSING = "Processing Renders"
    BATCH_TITLE_TIMEOUT = "Command Times Out"
    BATCH_COLOR_STARTING = 0x097DE3  # same as single-render starting
    BATCH_COLOR_PROCESSING = 0xE30952  # same as single-render in-progress
    BATCH_COLOR_COMPLETE = 0x32A885  # same as single-render complete

    def __init__(
        self,
        color: int,
        title: str,
        input_name: str,
        filename: Optional[str] = None,
        summary_tz: Optional[str] = None,
        unix_ts: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(title=title, color=color)

        summary = _format_render_summary(filename, summary_tz, unix_ts)
        if summary:
            self.add_field(name="Render Summary", value=summary, inline=False)
        else:
            self.add_field(name="Input", value=input_name)
        self.process_kwargs(**kwargs)

    def process_kwargs(self, **kwargs):
        if status := kwargs.pop("status", None):
            self.add_field(name="Status", value=status, inline=False)

        if position := kwargs.pop("position", None):
            self.add_field(name="Position", value=position, inline=False)

        if progress := kwargs.pop("progress", None):
            bar = (
                f"{round(progress * 10) * Render.PROGRESS_FOREGROUND}"
                f"{round((1 - progress) * 10) * Render.PROGRESS_BACKGROUND}"
            )
            self.add_field(name="Progress", value=bar, inline=False)

        if time_taken := kwargs.pop("time_taken", None):
            self.add_field(name="Time Taken", value=time_taken, inline=False)

        if result := kwargs.pop("result", None):
            self.add_field(name="Result", value=result, inline=False)


class RenderWaitingEmbed(RenderEmbed):
    COLOR = 0x097DE3  # starting left bar: #097de3

    def __init__(
        self,
        input_name: str,
        position: int,
        filename: Optional[str] = None,
        summary_tz: Optional[str] = None,
        unix_ts: Optional[int] = None,
    ):
        super().__init__(
            self.COLOR,
            RenderEmbed.TITLE_STARTING,
            input_name,
            filename=filename,
            summary_tz=summary_tz,
            unix_ts=unix_ts,
            position=position,
        )


class RenderStartedEmbed(RenderEmbed):
    COLOR = 0xE30952  # in-progress left bar: #e30952

    def __init__(
        self,
        input_name: str,
        progress: float,
        task_status: Optional[str] = None,
        filename: Optional[str] = None,
        summary_tz: Optional[str] = None,
        unix_ts: Optional[int] = None,
    ):
        super().__init__(
            self.COLOR,
            RenderEmbed.TITLE_PROCESSING,
            input_name,
            filename=filename,
            summary_tz=summary_tz,
            unix_ts=unix_ts,
            status=task_status.title() if task_status else "Started",
            progress=progress,
        )


class RenderSuccessEmbed(RenderEmbed):
    COLOR = 0x32A885  # complete left bar: #32a885
    TEMPLATE = "File link: [{0}]({1})\nMessage link: [Message]({2})"

    def __init__(
        self,
        input_name: str,
        sent_message: discord.Message,
        time_taken: str,
        filename: Optional[str] = None,
        summary_tz: Optional[str] = None,
        unix_ts: Optional[int] = None,
    ):
        sent_attachment = sent_message.attachments[0]
        result_msg = self.TEMPLATE.format(
            sent_attachment.filename, sent_attachment.url, sent_message.jump_url
        )

        super().__init__(
            self.COLOR,
            RenderEmbed.TITLE_SUMMARY,
            input_name,
            filename=filename,
            summary_tz=summary_tz,
            unix_ts=unix_ts,
            result=result_msg,
            time_taken=time_taken,
        )


class RenderFailureEmbed(RenderEmbed):
    COLOR = 0x32A885  # complete (error) left bar: #32a885

    def __init__(
        self,
        input_name: str,
        err_message,
        filename: Optional[str] = None,
        summary_tz: Optional[str] = None,
        unix_ts: Optional[int] = None,
    ):
        super().__init__(
            self.COLOR,
            RenderEmbed.TITLE_SUMMARY,
            input_name,
            filename=filename,
            summary_tz=summary_tz,
            unix_ts=unix_ts,
            status="Error",
            result=err_message,
        )


async def _send_ephemeral_followup(
    application_id: str, token: str, content: str
) -> bool:
    """Send an ephemeral followup message for the given interaction. Returns True if sent."""
    url = f"https://discord.com/api/v10/webhooks/{application_id}/{token}"
    payload = {"content": content, "flags": DISCORD_EPHEMERAL_FLAG}
    try:
        session = await _get_http_session()
        async with session.post(url, json=payload) as resp:
            return resp.status in (200, 204)
    except Exception as e:
        logger.warning("Failed to send ephemeral followup: %s", e)
        return False


async def _edit_batch_prompt_to_timeout(
    bot: Shrimpy,
    channel: discord.abc.Messageable,
    message_id: int,
) -> None:
    """After BATCH_PENDING_TTL, if the prompt message still shows the prompt, edit it to timeout."""
    await asyncio.sleep(BATCH_PENDING_TTL)
    try:
        fresh = await channel.fetch_message(message_id)
        if not fresh.embeds:
            return
        title = fresh.embeds[0].title
        if title != RenderEmbed.BATCH_TITLE_PROMPT:
            return  # user already replied; message was edited to Processing Renders
        timeout_embed = discord.Embed(
            title=RenderEmbed.BATCH_TITLE_TIMEOUT,
            color=0x747F8D,
            description=(
                "This command has timed out. Use the command again to try again."
            ),
        )
        await fresh.edit(embed=timeout_embed)
    except (discord.NotFound, discord.Forbidden) as e:
        logger.debug("Could not edit batch prompt to timeout: %s", e)


async def _batch_check(
    channel: discord.abc.Messageable,
    user_id: int,
    user_mention: str,
    count: int,
) -> bool:
    """Validate batch can run; send error to channel and return False if not."""
    # Run sync RQ/Redis calls in executor to avoid blocking event loop
    worker_count = await _run_sync(
        rq.worker.Worker.count, connection=_redis, queue=RenderSingle.QUEUE
    )
    cooldown = await _async_redis.ttl(f"cooldown_{user_id}")
    task_request_exists = await _async_redis.exists(f"task_request_{user_id}")
    queue_count = await _run_sync(lambda: RenderSingle.QUEUE.count)
    try:
        assert worker_count != 0, f"{user_mention} No running workers detected."
        assert queue_count + count <= RenderSingle.QUEUE_SIZE, (
            f"{user_mention} Queue full. Please try again later."
        )
        assert cooldown <= 0, (
            f"{user_mention} You're on render cooldown until "
            f"<t:{int(time.time()) + cooldown}:R>."
        )
        assert not task_request_exists, (
            f"{user_mention} You have an ongoing/queued render. Please try again later."
        )
        return True
    except AssertionError as e:
        await channel.send(str(e))
        return False


def _batch_aggregate_progress(job_ids: list, total: int) -> float:
    """
    Compute batch progress fraction (0.0–1.0) from RQ job statuses and current job progress.
    Each job contributes 1.0 if finished/failed, or meta['progress'] if started, else 0.
    """
    if total <= 0:
        return 0.0
    progress_sum = 0.0
    for jid in job_ids:
        try:
            job = rq.job.Job.fetch(jid, connection=_redis)
        except Exception:
            continue
        status = job.get_status(refresh=True)
        if status in ("finished", "failed"):
            progress_sum += 1.0
        elif status == "started":
            progress_sum += float(job.get_meta(refresh=True).get("progress", 0.0))
    return progress_sum / total


async def _batch_progress_updater(
    status_message: discord.Message,
    job_ids: list,
    results_list: list,
    total: int,
    lock: asyncio.Lock,
) -> None:
    """
    Periodically update the batch status embed footer with actual progress (completed jobs + current job progress).
    Run until cancelled by the coordinator when all poll tasks complete.
    """
    try:
        while True:
            await asyncio.sleep(STATUS_EMBED_EDIT_MIN_INTERVAL)
            async with lock:
                any_still_queued = any(st == "Queued" for _, st, _ in results_list)
                if not any_still_queued:
                    continue
                desc_lines = []
                for i, (fn, st, link) in enumerate(results_list):
                    display = format_replay_display(fn)
                    line = f"{i + 1}. **{display}**: {st}"
                    if st == "Done" and link:
                        line += f" — [Link]({link})"
                    elif st == "Failed" and link:
                        line += f" — {link}"
                    desc_lines.append(line)
                done_count = sum(1 for _, st, _ in results_list if st != "Queued")
            progress_frac = await _run_sync(
                _batch_aggregate_progress, job_ids, total
            )
            embed = discord.Embed(
                title=RenderEmbed.BATCH_TITLE_PROCESSING,
                color=RenderEmbed.BATCH_COLOR_PROCESSING,
                description="\n".join(desc_lines),
            )
            embed.set_footer(
                text=_batch_progress_footer(
                    done_count, total, progress_fraction=progress_frac
                )
            )
            try:
                await status_message.edit(embed=embed)
            except Exception as e:
                logger.warning("Could not update batch progress embed: %s", e)
    except asyncio.CancelledError:
        pass


async def _poll_single_batch_job(
    bot: Shrimpy,
    channel: discord.abc.Messageable,
    status_message: discord.Message,
    job_id: str,
    job_ids: list,
    filename: str,
    replay_bytes: bytes,
    job_index: int,
    total: int,
    results_list: list,
    lock: asyncio.Lock,
    single_message: bool = False,
    rendered_files: Optional[list] = None,
    batch_battle_stats: Optional[list] = None,
    batch_replay_timestamps: Optional[list] = None,
) -> None:
    """
    Poll ONE batch job until it finishes/fails.
    Updates results_list[job_index] and the status embed.
    Also stores BattleStats in batch_battle_stats[job_index] if provided.
    Does NOT handle combined send or cleanup (coordinator does that).
    """
    # Run sync RQ call in executor to avoid blocking event loop
    job = await _run_sync(rq.job.Job.fetch, job_id, connection=_redis)
    psub = _async_redis.pubsub()
    psub.ignore_subscribe_messages = True
    await psub.psubscribe(f"*{job_id}")
    listener = psub.listen()
    started_at = time.time()
    # Timeout scales with batch size so later jobs (e.g. 8–10) have enough wall time.
    wait_cap = min(
        BATCH_POLL_MAX_WAIT_CAP_SECONDS,
        max(
            BATCH_POLL_MAX_WAIT_MIN_SECONDS,
            total * BATCH_POLL_MAX_WAIT_PER_JOB_SECONDS,
        ),
    )

    try:
        while True:
            # Wait for pubsub message or fallback timeout; then always check RQ status.
            # This avoids blocking forever if Redis never publishes (e.g. missed event, worker crash).
            try:
                response = await asyncio.wait_for(
                    listener.__anext__(),
                    timeout=BATCH_POLL_STATUS_FALLBACK_SECONDS,
                )
            except asyncio.TimeoutError:
                response = None
            if response is not None and response.get("type") != "pmessage":
                continue

            status = await _run_sync(job.get_status, refresh=True)
            if status not in ("finished", "failed"):
                if (time.time() - started_at) >= wait_cap:
                    async with lock:
                        results_list[job_index] = (
                            filename,
                            "Failed",
                            "Job did not complete in time.",
                        )
                        any_still_queued = any(st == "Queued" for _, st, _ in results_list)
                        desc_lines = [
                            f"{i + 1}. **{format_replay_display(fn)}**: {st}"
                            + (f" — [Link]({link})" if st == "Done" and link else "")
                            + (f" — {link}" if st == "Failed" and link else "")
                            for i, (fn, st, link) in enumerate(results_list)
                        ]
                        title = RenderEmbed.BATCH_TITLE_PROCESSING if any_still_queued else RenderEmbed.TITLE_SUMMARY
                        color = RenderEmbed.BATCH_COLOR_PROCESSING if any_still_queued else RenderEmbed.BATCH_COLOR_COMPLETE
                        embed = discord.Embed(title=title, color=color, description="\n".join(desc_lines))
                        if any_still_queued:
                            done_count = sum(1 for _, st, _ in results_list if st != "Queued")
                            progress_frac = await _run_sync(_batch_aggregate_progress, job_ids, total)
                            embed.set_footer(text=_batch_progress_footer(done_count, total, progress_fraction=progress_frac))
                        try:
                            await status_message.edit(embed=embed)
                        except Exception as e:
                            logger.warning("Could not update batch status embed: %s", e)
                    break
                continue

            async with lock:
                if status == "finished" and job.result:
                    if isinstance(job.result, tuple):
                        # Unpack 5+ elements (6th is battle_stats, 7th is replay timestamp)
                        data, out_name, time_taken, builds_str, chat = job.result[:5]
                        battle_stats = job.result[5] if len(job.result) >= 6 else None
                        replay_ts = job.result[6] if len(job.result) >= 7 else None
                        if batch_battle_stats is not None:
                            batch_battle_stats[job_index] = battle_stats
                        if batch_replay_timestamps is not None:
                            batch_replay_timestamps[job_index] = replay_ts
                        if rendered_files is not None:
                            rendered_files[job_index] = (out_name, data)
                        if single_message:
                            # Coordinator will send combined; just mark done
                            results_list[job_index] = (filename, "Done", None)
                        else:
                            # Send individually with robust retries
                            if len(data) > DISCORD_MAX_FILE_BYTES:
                                results_list[job_index] = (
                                    filename,
                                    "Failed",
                                    "File too large (>8 MB).",
                                )
                            else:

                                async def _send_one():
                                    if builds_str:
                                        view = RenderView(
                                            json.loads(builds_str), chat
                                        )
                                        sent = await channel.send(
                                            content=None,
                                            file=discord.File(
                                                io.BytesIO(data), f"{out_name}.mp4"
                                            ),
                                            view=view,
                                        )
                                        view.message = sent
                                        return sent
                                    return await channel.send(
                                        content=None,
                                        file=discord.File(
                                            io.BytesIO(data), f"{out_name}.mp4"
                                        ),
                                    )

                                sent = await _send_with_retry(
                                    BATCH_POST_RESULT_ATTEMPTS,
                                    BATCH_POST_RESULT_RETRY_DELAY_BASE,
                                    _send_one,
                                    f"Batch job {job_id} send",
                                )
                                if sent is not None:
                                    results_list[job_index] = (
                                        filename,
                                        "Done",
                                        sent.jump_url,
                                    )
                                else:
                                    results_list[job_index] = (
                                        filename,
                                        "Failed",
                                        FAILED_TO_POST_MSG,
                                    )
                    elif isinstance(job.result, errors.RenderError):
                        results_list[job_index] = (
                            filename,
                            "Failed",
                            job.result.message,
                        )
                    else:
                        results_list[job_index] = (
                            filename,
                            "Failed",
                            "Unknown result.",
                        )
                elif status == "failed":
                    meta = await _run_sync(job.get_meta, refresh=True)
                    timeout = meta.get("timeout", False)
                    err = "Job timed out." if timeout else "An unhandled error occurred."
                    results_list[job_index] = (filename, "Failed", err)
                    try:
                        ch = await bot.fetch_channel(cfg.channels.failed_renders)
                        await ch.send(
                            files=[
                                discord.File(
                                    io.BytesIO(replay_bytes), filename=filename
                                ),
                            ]
                        )
                    except Exception:
                        pass
                else:
                    results_list[job_index] = (
                        filename,
                        "Failed",
                        "Unknown result.",
                    )

                # Update status embed
                any_still_queued = any(st == "Queued" for _, st, _ in results_list)
                desc_lines = []
                for i, (fn, st, link) in enumerate(results_list):
                    display = format_replay_display(fn)
                    line = f"{i + 1}. **{display}**: {st}"
                    if st == "Done" and link:
                        line += f" — [Link]({link})"
                    elif st == "Failed" and link:
                        line += f" — {link}"
                    desc_lines.append(line)
                if any_still_queued:
                    title = RenderEmbed.BATCH_TITLE_PROCESSING
                    color = RenderEmbed.BATCH_COLOR_PROCESSING
                else:
                    title = RenderEmbed.TITLE_SUMMARY
                    color = RenderEmbed.BATCH_COLOR_COMPLETE
                embed = discord.Embed(
                    title=title,
                    color=color,
                    description="\n".join(desc_lines),
                )
                if title == RenderEmbed.BATCH_TITLE_PROCESSING:
                    done_count = sum(
                        1 for _, st, _ in results_list if st != "Queued"
                    )
                    progress_frac = await _run_sync(
                        _batch_aggregate_progress, job_ids, total
                    )
                    embed.set_footer(
                        text=_batch_progress_footer(
                            done_count, total, progress_fraction=progress_frac
                        )
                    )
                try:
                    await status_message.edit(embed=embed)
                except Exception as e:
                    logger.warning("Could not update batch status embed: %s", e)
            break
    finally:
        await psub.unsubscribe()
        await psub.close()


async def _recover_finished_batch_jobs(
    bot: Shrimpy,
    channel: discord.abc.Messageable,
    job_ids: list,
    filenames: list,
    bytes_list: list,
    results_list: list,
    rendered_files: Optional[list],
    batch_battle_stats: Optional[list],
    batch_replay_timestamps: Optional[list],
    single_message: bool,
) -> None:
    """
    Industry-standard recovery loop: wait for and recover jobs that are still running or
    have finished after poll tasks exited. Loops until all jobs are done/failed or max wait.

    This handles:
    - Jobs that finished after their poll task timed out
    - Jobs that are still running (waits for them)
    - Jobs that were retried by RQ and have since completed

    Best practice: poll with exponential backoff, max wait time, log recoveries.
    """
    recovery_start = time.time()
    recovered_count = 0

    while True:
        # Check if we've exceeded max recovery time
        if (time.time() - recovery_start) >= BATCH_RECOVERY_MAX_WAIT_SECONDS:
            logger.warning(
                "Batch recovery max wait (%s s) exceeded; %s jobs recovered, proceeding with available results",
                BATCH_RECOVERY_MAX_WAIT_SECONDS,
                recovered_count,
            )
            break

        # Check which jobs still need recovery
        jobs_needing_recovery = []
        jobs_still_running = []
        for i in range(len(job_ids)):
            fn, st, link = results_list[i]
            if st == "Done":
                continue
            # Recoverable: Queued, or Failed with specific timeout message
            if st == "Queued" or (st == "Failed" and link == "Job did not complete in time."):
                jobs_needing_recovery.append(i)

        if not jobs_needing_recovery:
            # All jobs are done or permanently failed
            break

        # Check RQ status for each job needing recovery
        for i in jobs_needing_recovery:
            fn = results_list[i][0]
            try:
                job = await _run_sync(
                    rq.job.Job.fetch, job_ids[i], connection=_redis
                )
            except Exception as e:
                logger.debug("Recovery: could not fetch job %s: %s", job_ids[i], e)
                continue

            status = await _run_sync(job.get_status, refresh=True)

            if status == "started" or status == "queued" or status == "scheduled":
                # Job is still running or waiting; we'll wait and check again
                jobs_still_running.append(i)
                continue

            if status == "finished" and job.result and isinstance(job.result, tuple):
                # Job completed! Recover the result.
                data, out_name, time_taken, builds_str, chat = job.result[:5]
                battle_stats = job.result[5] if len(job.result) >= 6 else None
                replay_ts = job.result[6] if len(job.result) >= 7 else None

                if rendered_files is not None and i < len(rendered_files):
                    rendered_files[i] = (out_name, data)
                if batch_battle_stats is not None and i < len(batch_battle_stats):
                    batch_battle_stats[i] = battle_stats
                if batch_replay_timestamps is not None and i < len(batch_replay_timestamps):
                    batch_replay_timestamps[i] = replay_ts

                if single_message:
                    results_list[i] = (fn, "Done", None)
                else:
                    if len(data) > DISCORD_MAX_FILE_BYTES:
                        results_list[i] = (fn, "Failed", "File too large (>8 MB).")
                    else:
                        async def _send_one(out=out_name, d=data, b=builds_str, c=chat):
                            if b:
                                view = RenderView(json.loads(b), c)
                                sent = await channel.send(
                                    content=None,
                                    file=discord.File(io.BytesIO(d), f"{out}.mp4"),
                                    view=view,
                                )
                                view.message = sent
                                return sent
                            return await channel.send(
                                content=None,
                                file=discord.File(io.BytesIO(d), f"{out}.mp4"),
                            )

                        sent = await _send_with_retry(
                            BATCH_POST_RESULT_ATTEMPTS,
                            BATCH_POST_RESULT_RETRY_DELAY_BASE,
                            _send_one,
                            f"Batch job {job_ids[i]} recovery send",
                        )
                        if sent is not None:
                            results_list[i] = (fn, "Done", sent.jump_url)
                        else:
                            results_list[i] = (fn, "Failed", FAILED_TO_POST_MSG)

                recovered_count += 1
                logger.info(
                    "Recovered batch job %s (index %s) from RQ after poll task had exited",
                    job_ids[i],
                    i,
                )

            elif status == "failed":
                # Job failed permanently; mark as failed (no retry left)
                meta = await _run_sync(job.get_meta, refresh=True)
                timeout = meta.get("timeout", False)
                err = "Job timed out." if timeout else "Render failed after retries."
                results_list[i] = (fn, "Failed", err)
                logger.warning(
                    "Batch job %s (index %s) failed permanently in RQ: %s",
                    job_ids[i],
                    i,
                    err,
                )

        # If there are still jobs running in RQ, wait before next poll
        if jobs_still_running:
            logger.debug(
                "Recovery: %s jobs still running in RQ, waiting %s s before next check",
                len(jobs_still_running),
                BATCH_RECOVERY_POLL_INTERVAL_SECONDS,
            )
            await asyncio.sleep(BATCH_RECOVERY_POLL_INTERVAL_SECONDS)
        else:
            # No jobs still running; we've processed everything we can
            break

    if recovered_count > 0:
        logger.info("Batch recovery complete: %s jobs recovered", recovered_count)


async def _batch_coordinator(
    bot: Shrimpy,
    channel: discord.abc.Messageable,
    status_message: discord.Message,
    job_ids: list,
    filenames: list,
    bytes_list: list,
    user_id: int,
    results_list: list,
    single_message: bool,
    include_summary: bool,
    interaction_token: Optional[str],
    application_id: Optional[str],
    files_message_id: int,
    rendered_files: Optional[list],
    batch_battle_stats: Optional[list] = None,
    batch_replay_timestamps: Optional[list] = None,
    upload_timestamp: Optional[float] = None,
) -> None:
    """
    Coordinator: spawn poll tasks, wait for ALL to complete, then send and cleanup.
    Cleanup (task_request, cooldown, temp dir) ALWAYS runs in finally so the bot
    stays usable even when delivery fails.
    Also collects battle stats for win rate calculation when include_summary is True.
    When include_summary and upload_timestamp are set, timezone is inferred so the
    uploader sees their local time and other viewers see the same moment in their TZ.
    """
    lock = asyncio.Lock()
    total = len(job_ids)
    batch_temp_dir: Optional[Path] = None
    # Resolve timezone for summary display: user-set > UTC.
    summary_tz = None
    summary_tz_from_user_setting = False
    if include_summary:
        try:
            user = await db.User.get_or_create(id=user_id)
            user_tz = getattr(user, "replay_timezone", None)
            if user_tz:
                summary_tz = user_tz
                summary_tz_from_user_setting = True
        except Exception as e:
            logger.debug("Could not load user timezone for %s: %s", user_id, e)
        if summary_tz is None:
            summary_tz = "UTC"
        logger.info(
            "Batch summary timezone: %s (user_set=%s, user_id=%s)",
            summary_tz,
            summary_tz_from_user_setting,
            user_id,
        )

    try:
        # Create poll tasks for each job
        poll_tasks = [
            asyncio.create_task(
                _poll_single_batch_job(
                    bot,
                    channel,
                    status_message,
                    job_id,
                    job_ids,
                    filename,
                    replay_bytes,
                    i,
                    total,
                    results_list,
                    lock,
                    single_message,
                    rendered_files,
                    batch_battle_stats,
                    batch_replay_timestamps,
                )
            )
            for i, (job_id, filename, replay_bytes) in enumerate(
                zip(job_ids, filenames, bytes_list)
            )
        ]

        # Progress updater: periodically refresh footer with actual progress (full-width bar)
        progress_task = asyncio.create_task(
            _batch_progress_updater(
                status_message, job_ids, results_list, total, lock
            )
        )

        # Wait for ALL poll tasks to complete (exceptions absorbed)
        await asyncio.gather(*poll_tasks, return_exceptions=True)
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

        # Recover any jobs that finished in RQ after our poll task timed out (e.g. jobs 7–10
        # queued behind 1–6). Otherwise we'd only have 6 in rendered_files and send one message.
        await _recover_finished_batch_jobs(
            bot,
            channel,
            job_ids,
            filenames,
            bytes_list,
            results_list,
            rendered_files,
            batch_battle_stats,
            batch_replay_timestamps,
            single_message,
        )

        # Sync results_list from rendered_files (don't rely on expired RQ job results)
        for i, (fn, st, link) in enumerate(results_list):
            if st == "Queued":
                if rendered_files is not None and i < len(rendered_files) and rendered_files[i] is not None:
                    results_list[i] = (fn, "Done", None)
                else:
                    results_list[i] = (fn, "Failed", "Missing render data.")

        # Durable storage: persist rendered files to disk so retries don't depend on RQ
        entries_for_send: list[tuple[str, bytes]] = []
        if rendered_files:
            for entry in rendered_files:
                if entry is not None:
                    entries_for_send.append(entry)
            if entries_for_send:
                batch_temp_dir = Path(tempfile.mkdtemp(prefix="batch_renders_"))
                for out_name, data in entries_for_send:
                    (batch_temp_dir / f"{out_name}.mp4").write_bytes(data)

        sent_combined = None
        used_fallback = False
        # Why we used individual sends: "size" = pre-flight limit, "network" = combined send failed
        fallback_reason: Optional[str] = None
        num_bucket_messages: Optional[int] = None

        if single_message and entries_for_send:
            # Pre-flight: skip combined send if over Discord limits
            total_size = sum(len(d) for _, d in entries_for_send)
            max_file = max(len(d) for _, d in entries_for_send)
            use_combined = (
                total_size <= DISCORD_MAX_MESSAGE_BYTES
                and max_file <= DISCORD_MAX_FILE_BYTES
            )
            if not use_combined:
                logger.info(
                    "Batch pre-flight: total=%s max_file=%s; using individual sends",
                    total_size,
                    max_file,
                )
                used_fallback = True
                fallback_reason = "size"

            if use_combined and batch_temp_dir is not None:
                # Build files from durable paths (fresh handle per retry)
                def _make_combined_files():
                    return [
                        discord.File(
                            batch_temp_dir / f"{out_name}.mp4",
                            filename=f"{out_name}.mp4",
                        )
                        for out_name, _ in entries_for_send
                    ]

                sent_combined = await _send_with_retry(
                    BATCH_POST_RESULT_ATTEMPTS,
                    BATCH_POST_RESULT_RETRY_DELAY_BASE,
                    lambda: channel.send(content=None, files=_make_combined_files()),
                    "Combined batch send",
                )

            if sent_combined is None and entries_for_send and batch_temp_dir is not None:
                # Pack into as few messages as possible (bin packing); first message gets H1
                if not used_fallback:
                    fallback_reason = "network"
                used_fallback = True

                packable: list[tuple[int, str, bytes]] = []
                for i in range(total):
                    if rendered_files is None or i >= len(rendered_files) or rendered_files[i] is None:
                        continue
                    fn, _ = results_list[i][0], results_list[i][1]
                    out_name, data = rendered_files[i]
                    if len(data) > DISCORD_MAX_FILE_BYTES:
                        results_list[i] = (fn, "Failed", "File too large (>8 MB).")
                        continue
                    packable.append((i, out_name, data))

                buckets = _pack_into_buckets(
                    packable, DISCORD_MAX_MESSAGE_BYTES
                ) if packable else []
                num_bucket_messages = len(buckets)
                first_parsed = (
                    _parse_replay_filename(results_list[0][0])
                    if results_list else None
                )
                date_title = (
                    _format_full_date(first_parsed["date"])
                    if first_parsed else "Renders"
                )
                any_bucket_failed = False
                if include_summary and batch_replay_timestamps is not None:
                    meta_count = sum(1 for x in batch_replay_timestamps if x is not None)
                    logger.info(
                        "Batch summary: metadata_timestamps=%s/%s (rest use filename+tz=%s)",
                        meta_count,
                        len(batch_replay_timestamps),
                        summary_tz or "UTC",
                    )
                # For include_summary: continuous numbering across buckets (second message starts at 7 if first had 6)
                summary_start_by_bucket = []
                if buckets:
                    acc = 0
                    for b in buckets:
                        summary_start_by_bucket.append(acc)
                        acc += len(b)
                for bucket_idx, bucket in enumerate(buckets):
                    if include_summary:
                        start_num = summary_start_by_bucket[bucket_idx]
                        # When user set timezone, use filename+tz only (ignore metadata ts).
                        # When user set timezone, use filename+tz so the moment is correct; else use metadata.
                        lines = [
                            f"{start_num + idx + 1}. {format_replay_summary_line(results_list[i][0], summary_tz, batch_replay_timestamps[i] if batch_replay_timestamps and not summary_tz_from_user_setting else None)}"
                            for idx, (i, _, _) in enumerate(bucket)
                        ]
                        header = ""
                        if bucket_idx == 0:
                            header = f"# Renders for {date_title}\n\n"
                            overall = _format_summary_overall_stats(batch_battle_stats)
                            if overall:
                                header += overall + "\n\n"
                            header += "**Renders (in Chronological Order)**:\n"
                        bucket_content = header + "\n".join(lines)
                    else:
                        bucket_content = None

                    def _make_bucket_files(b=bucket, d=batch_temp_dir):
                        return [
                            discord.File(d / f"{on}.mp4", filename=f"{on}.mp4")
                            for _, on, _ in b
                        ]

                    sent = await _send_with_retry(
                        BATCH_POST_RESULT_ATTEMPTS,
                        BATCH_POST_RESULT_RETRY_DELAY_BASE,
                        lambda: channel.send(
                            content=bucket_content,
                            files=_make_bucket_files(),
                        ),
                        f"Batch bucket {bucket_idx + 1}/{len(buckets)}",
                    )
                    if sent is not None:
                        for (i, _, _) in bucket:
                            fn = results_list[i][0]
                            results_list[i] = (fn, "Done", sent.jump_url)
                    else:
                        any_bucket_failed = True
                        for (i, out_name, data) in bucket:
                            fn = results_list[i][0]
                            sent_one = await _send_with_retry(
                                BATCH_POST_RESULT_ATTEMPTS,
                                BATCH_POST_RESULT_RETRY_DELAY_BASE,
                                lambda i_=i, o=out_name, d=data: channel.send(
                                    content=None,
                                    file=discord.File(io.BytesIO(d), f"{o}.mp4"),
                                ),
                                f"Individual send {fn!r}",
                            )
                            if sent_one is not None:
                                results_list[i] = (fn, "Done", sent_one.jump_url)
                            else:
                                results_list[i] = (fn, "Failed", FAILED_TO_POST_MSG)

            if sent_combined is not None:
                for i in range(total):
                    fn, st, link = results_list[i]
                    if st == "Done":
                        results_list[i] = (fn, "Done", sent_combined.jump_url)

                if include_summary:
                    if batch_replay_timestamps is not None:
                        meta_count = sum(1 for x in batch_replay_timestamps if x is not None)
                        logger.info(
                            "Batch summary (single msg): metadata_timestamps=%s/%s (rest use filename+tz=%s)",
                            meta_count,
                            len(batch_replay_timestamps),
                            summary_tz or "UTC",
                        )
                    first_parsed = (
                        _parse_replay_filename(results_list[0][0])
                        if results_list
                        else None
                    )
                    date_title = (
                        _format_full_date(first_parsed["date"])
                        if first_parsed
                        else "Renders"
                    )
                    overall = _format_summary_overall_stats(batch_battle_stats)
                    # When user set timezone, use filename+tz only (ignore metadata ts).
                    # When user set timezone, use filename+tz so the moment is correct; else use metadata.
                    summary_lines = [
                        f"{i + 1}. {format_replay_summary_line(fn, summary_tz, batch_replay_timestamps[i] if batch_replay_timestamps and not summary_tz_from_user_setting else None)}"
                        for i, (fn, st, link) in enumerate(results_list)
                    ]
                    content = f"# Renders for {date_title}\n\n"
                    if overall:
                        content += overall + "\n\n"
                    content += "**Renders (in Chronological Order)**:\n"
                    content += "\n".join(summary_lines)
                    await _send_with_retry(
                        BATCH_POST_RESULT_ATTEMPTS,
                        BATCH_POST_RESULT_RETRY_DELAY_BASE,
                        lambda: sent_combined.edit(content=content),
                        "Batch summary edit",
                    )

        # Update final embed
        desc_lines = []
        for i, (fn, st, link) in enumerate(results_list):
            display = format_replay_display(fn)
            line = f"{i + 1}. **{display}**: {st}"
            if st == "Done" and link:
                line += f" — [Link]({link})"
            elif st == "Failed" and link:
                line += f" — {link}"
            desc_lines.append(line)
        final_desc = "\n".join(desc_lines)
        # Add win rate to final embed when include_summary is True
        if include_summary and batch_battle_stats:
            win_rate_line = format_win_rate_line(batch_battle_stats)
            if win_rate_line:
                final_desc += f"\n\n{win_rate_line}"
        embed = discord.Embed(
            title=RenderEmbed.TITLE_SUMMARY,
            color=RenderEmbed.BATCH_COLOR_COMPLETE,
            description=final_desc,
        )
        try:
            await status_message.edit(embed=embed)
        except Exception as e:
            logger.warning("Could not update final batch embed: %s", e)

        # Delete user's file-list message
        if files_message_id is not None:
            try:
                files_msg = await channel.fetch_message(files_message_id)
                await files_msg.delete()
            except (discord.NotFound, discord.Forbidden) as e:
                logger.debug(
                    "Could not delete batch file-list message %s: %s",
                    files_message_id,
                    e,
                )
            except Exception as e:
                logger.warning("Error deleting file-list message: %s", e)

        # Notify about failed-to-post
        failed_to_post = [
            fn for fn, st, link in results_list if st == "Failed" and link == FAILED_TO_POST_MSG
        ]
        if failed_to_post:
            user_mention = f"<@{user_id}>"
            lines = []
            for fn in failed_to_post:
                display = format_replay_display(fn)
                lines.append(f"• **{display}**\n  `{fn}`")
            content = (
                f"{user_mention} The following replay(s) could not be "
                f"posted after {BATCH_POST_RESULT_ATTEMPTS} attempts (network error):\n"
                + "\n\n".join(lines)
            )
            sent_ephemeral = False
            if interaction_token and application_id:
                sent_ephemeral = await _send_ephemeral_followup(
                    application_id, interaction_token, content
                )
            if not sent_ephemeral:
                try:
                    await channel.send(content)
                except Exception as e:
                    logger.warning("Could not notify user of failed replays: %s", e)

        if include_summary and interaction_token and application_id:
            if not single_message:
                await _send_ephemeral_followup(
                    application_id,
                    interaction_token,
                    'No summary was included because "Attach all in one message" was not enabled. Enable that option to include a render summary in the message.',
                )
            elif used_fallback:
                if fallback_reason == "size":
                    if num_bucket_messages is not None and num_bucket_messages > 1:
                        await _send_ephemeral_followup(
                            application_id,
                            interaction_token,
                            f"Renders were split into {num_bucket_messages} messages due to Discord size limits (8 MB per file, 25 MB total). Summary is included in each message.",
                        )
                    elif num_bucket_messages == 0:
                        await _send_ephemeral_followup(
                            application_id,
                            interaction_token,
                            "All renders exceeded 8 MB per file and could not be attached.",
                        )
                    else:
                        await _send_ephemeral_followup(
                            application_id,
                            interaction_token,
                            "Some files were omitted (over 8 MB). Remaining renders are in as few messages as possible with summary in each.",
                        )
                else:
                    if num_bucket_messages is not None and num_bucket_messages > 1:
                        await _send_ephemeral_followup(
                            application_id,
                            interaction_token,
                            f"Combined upload failed due to network issues. Renders were sent in {num_bucket_messages} messages; summary is included in each.",
                        )
                    else:
                        await _send_ephemeral_followup(
                            application_id,
                            interaction_token,
                            "Combined upload failed due to network issues. Renders were sent individually, so no summary was attached.",
                        )
    except Exception as e:
        logger.exception("Batch coordinator error (cleanup will still run): %s", e)
    finally:
        # GUARANTEED CLEANUP: always clear queue state so the bot stays usable
        try:
            await _async_redis.delete(f"task_request_{user_id}")
            await _async_redis.set(f"cooldown_{user_id}", "", ex=RenderSingle.COOLDOWN)
        except Exception as e:
            logger.warning("Batch cleanup Redis error: %s", e)
        if batch_temp_dir is not None and batch_temp_dir.exists():
            try:
                shutil.rmtree(batch_temp_dir, ignore_errors=True)
            except Exception as e:
                logger.debug("Batch temp dir cleanup: %s", e)


class RenderCog(commands.Cog):
    def __init__(self, bot: Shrimpy):
        self.bot: Shrimpy = bot

    @app_commands.command(
        name="render",
        description="Generates a minimap timelapse and more from a replay file.",
        extras={"category": "wows"},
    )
    @app_commands.describe(
        replay="A .wowsreplay file.",
        fps="Can be a value from 15 to 30, and defaults to 20.",
        quality="Can be a value from 1-9, and defaults to 7. Higher values may require Nitro boosts.",
        logs="Shows additional statistics, and defaults to on.",
        anon='Anonymizes player names in the format "Player X", and defaults to off. Ignored when logs is off.',
        chat="Shows chat, and defaults to on. Ignored when logs is off.",
        team_tracers="Colors tracers by their relation to the replay creator, and defaults to off.",
    )
    async def render(
        self,
        interaction: discord.Interaction,
        replay: discord.Attachment,
        fps: app_commands.Range[int, 15, 30] = 20,
        quality: app_commands.Range[int, 1, 9] = 7,
        logs: bool = True,
        anon: bool = False,
        chat: bool = True,
        team_tracers: bool = False,
    ):
        render = RenderSingle(self.bot, interaction, replay)
        await render.start(fps, quality, logs, anon, chat, team_tracers)

    @app_commands.command(
        name="dualrender",
        description="Merges two replay files from opposing teams into a minimap timelapse.",
        extras={"category": "wows"},
    )
    @app_commands.describe(
        replay_a='The replay to use as the "green" team.',
        replay_b='The replay to use as the "red" team.',
        name_a='The name to use for the "green" team.',
        name_b='The name to use for the "red" team.',
        fps="Can be a value from 15 to 30, and defaults to 20.",
        quality="Can be a value from 1-9, and defaults to 7. Higher values may require Nitro boosts.",
        team_tracers="Colors tracers by their relation to the replay creator, and defaults to off.",
    )
    async def dual_render(
        self,
        interaction: discord.Interaction,
        replay_a: discord.Attachment,
        replay_b: discord.Attachment,
        name_a: app_commands.Range[str, 1, 12] = "Alpha",
        name_b: app_commands.Range[str, 1, 12] = "Bravo",
        fps: app_commands.Range[int, 15, 30] = 20,
        quality: app_commands.Range[int, 1, 9] = 7,
        team_tracers: bool = False,
    ):
        render = RenderDual(self.bot, interaction, replay_a, replay_b)
        await render.start(fps, quality, name_a, name_b, team_tracers)

    @app_commands.command(
        name="renderbatch",
        description="Queue multiple .wowsreplay files. Reply to the bot's message with your files.",
        extras={"category": "wows"},
    )
    @app_commands.describe(
        fps="Can be a value from 15 to 30, and defaults to 20.",
        quality="Can be a value from 1-9, and defaults to 7. Higher values may require Nitro boosts.",
        logs="Shows additional statistics, and defaults to on.",
        anon='Anonymizes player names in the format "Player X", and defaults to off. Ignored when logs is off.',
        chat="Shows chat, and defaults to on. Ignored when logs is off.",
        team_tracers="Colors tracers by their relation to the replay creator, and defaults to off.",
        single_message="Attach all renders in a single Discord message (up to 10 files). Defaults to off.",
        include_summary="Include a numbered summary of all renders in the message text. Only applies when single_message is enabled.",
    )
    async def render_batch(
        self,
        interaction: discord.Interaction,
        fps: app_commands.Range[int, 15, 30] = 20,
        quality: app_commands.Range[int, 1, 9] = 7,
        logs: bool = True,
        anon: bool = False,
        chat: bool = True,
        team_tracers: bool = False,
        single_message: bool = False,
        include_summary: bool = False,
    ):
        await interaction.response.defer()
        embed = discord.Embed(
            title=RenderEmbed.BATCH_TITLE_PROMPT,
            color=RenderEmbed.BATCH_COLOR_STARTING,
            description=(
                "**Reply to this message** with your replay file(s) (up to "
                f"**{BATCH_MAX_REPLAYS}** .wowsreplay files)."
            ),
        )
        msg = await interaction.followup.send(embed=embed, wait=True)
        payload = {
            "user_id": interaction.user.id,
            "channel_id": interaction.channel_id,
            "fps": fps,
            "quality": quality,
            "logs": logs,
            "anon": anon,
            "chat": chat,
            "team_tracers": team_tracers,
            "single_message": single_message,
            "include_summary": include_summary,
            "interaction_token": interaction.token,
            "application_id": str(interaction.application_id),
        }
        key = f"{BATCH_PENDING_PREFIX}{msg.id}"
        await _async_redis.set(key, json.dumps(payload), ex=BATCH_PENDING_TTL)
        self.bot.loop.create_task(
            _edit_batch_prompt_to_timeout(self.bot, msg.channel, msg.id)
        )

    async def _try_handle_render_batch_reply(
        self, message: discord.Message
    ) -> bool:
        """Handle a reply that may contain batch replay attachments. Return True if handled."""
        if not message.reference or message.author.bot:
            return False
        ref_id = message.reference.message_id
        key = f"{BATCH_PENDING_PREFIX}{ref_id}"
        raw = await _async_redis.get(key)
        if not raw:
            return False
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await _async_redis.delete(key)
            return False
        if message.author.id != payload["user_id"]:
            return False

        replays = [
            a
            for a in message.attachments
            if a.filename.lower().endswith(".wowsreplay")
        ][:BATCH_MAX_REPLAYS]
        if not replays:
            await message.channel.send(
                f"{message.author.mention} No .wowsreplay files found in your message."
            )
            await _async_redis.delete(key)
            return True
        await _async_redis.delete(key)

        ok = await _batch_check(
            message.channel,
            payload["user_id"],
            message.author.mention,
            len(replays),
        )
        if not ok:
            return True

        await _async_redis.set(
            f"task_request_{payload['user_id']}", "", ex=600
        )
        bytes_list = []
        for att in replays:
            with io.BytesIO() as fp:
                await att.save(fp)
                fp.seek(0)
                bytes_list.append(fp.read())
        filenames = [a.filename for a in replays]

        # Sort by play time (earliest to most recent) so batch order is always chronological
        pairs = sorted(
            zip(filenames, bytes_list),
            key=lambda p: _replay_sort_key(p[0]),
        )
        filenames = [p[0] for p in pairs]
        bytes_list = [p[1] for p in pairs]

        queue = RenderSingle.QUEUE
        # Industry standard: TTL must allow ALL jobs to START (not just finish).
        # Formula: (current queue depth + batch size) * time per job, with minimum floor.
        batch_size = len(bytes_list)
        job_ttl = max(
            BATCH_JOB_TTL_MIN_SECONDS,
            (queue.count + batch_size) * BATCH_JOB_TTL_PER_JOB_SECONDS,
        )
        # Industry standard: retry for transient failures.
        # Immediate retries (no interval) work on Windows without scheduler.
        retry_config = Retry(max=BATCH_JOB_RETRY_MAX)
        job_ids = []
        for data in bytes_list:
            job = queue.enqueue(
                tasks.render_single,
                args=[
                    0,
                    0,
                    data,
                    payload["fps"],
                    payload["quality"],
                    payload["logs"],
                    payload["anon"],
                    payload["chat"],
                    payload["team_tracers"],
                ],
                failure_ttl=BATCH_JOB_FAILURE_TTL_SECONDS,
                result_ttl=BATCH_JOB_RESULT_TTL_SECONDS,
                ttl=job_ttl,
                retry=retry_config,
            )
            job_ids.append(job.id)

        results_list = [(fn, "Queued", None) for fn in filenames]
        single_message = payload.get("single_message", False)
        include_summary = payload.get("include_summary", False)
        rendered_files = [None] * len(replays)
        batch_battle_stats: list[Optional[BattleStats]] = [None] * len(replays)
        batch_replay_timestamps: list[Optional[int]] = [None] * len(replays)
        desc_lines = [
            f"{i + 1}. **{format_replay_display(fn)}**: Queued"
            for i, fn in enumerate(filenames)
        ]
        embed = discord.Embed(
            title=RenderEmbed.BATCH_TITLE_PROCESSING,
            color=RenderEmbed.BATCH_COLOR_PROCESSING,
            description=f"Batch queued: **{len(replays)}** replays.\n\n" + "\n".join(desc_lines),
        )
        embed.set_footer(text=_batch_progress_footer(0, len(replays)))
        prompt_message = await message.channel.fetch_message(ref_id)
        await prompt_message.edit(embed=embed)

        interaction_token = payload.get("interaction_token")
        application_id = payload.get("application_id")

        # Use coordinator pattern: ONE task handles all jobs + final send/cleanup
        # Pass upload time so we infer uploader timezone and show their local time in summary.
        self.bot.loop.create_task(
            _batch_coordinator(
                self.bot,
                message.channel,
                prompt_message,
                job_ids,
                filenames,
                bytes_list,
                payload["user_id"],
                results_list,
                single_message,
                include_summary,
                interaction_token,
                application_id,
                message.id,
                rendered_files,
                batch_battle_stats,
                batch_replay_timestamps,
                upload_timestamp=message.created_at.timestamp(),
            )
        )
        return True

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if message.reference:
            if await self._try_handle_render_batch_reply(message):
                return
        if (
            message.channel.id not in WOWS_TOURNAMENTS_CHANNELS
            or message.author.id not in WOWS_TOURNAMENTS_USERS
        ):
            return

        await message.add_reaction("✅")

        try:
            data = json.loads(message.content[1:-1])
        except json.JSONDecodeError:
            return

        files = []

        # SECURITY: Validate all replay URLs and callback URL before processing
        callback_url = data.get("callbackUrl", "")
        if callback_url and not _is_url_allowed(callback_url, ALLOWED_CALLBACK_DOMAINS):
            logger.warning(
                "Tournament render blocked: callback URL not allowed: %s", callback_url
            )
            await message.add_reaction("❌")
            return

        for replay in data.get("replays", []):
            replay_url = replay.get("replay", "")
            if not _is_url_allowed(replay_url, ALLOWED_REPLAY_DOMAINS):
                logger.warning(
                    "Tournament render blocked: replay URL not allowed: %s", replay_url
                )
                await message.add_reaction("❌")
                return

        async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
            for replay in data["replays"]:
                async with session.get(replay["replay"]) as response:
                    # SECURITY: Enforce size limit to prevent DoS/memory exhaustion
                    content_length = response.headers.get("Content-Length")
                    if content_length and int(content_length) > MAX_REPLAY_DOWNLOAD_BYTES:
                        logger.warning(
                            "Tournament replay too large: %s bytes", content_length
                        )
                        await message.add_reaction("❌")
                        return
                    # Read with size limit (streaming)
                    data_bytes = await response.content.read(MAX_REPLAY_DOWNLOAD_BYTES + 1)
                    if len(data_bytes) > MAX_REPLAY_DOWNLOAD_BYTES:
                        logger.warning("Tournament replay exceeded size limit during read")
                        await message.add_reaction("❌")
                        return
                    files.append(data_bytes)

        render = RenderWT(
            self.bot,
            files[0],
            files[1],
            data["targetChannelId"],
            f"{data['callbackUrl']}",
        )
        await render.start(*[replay["tag"].upper() for replay in data["replays"]])


async def setup(bot: Shrimpy):
    await bot.add_cog(RenderCog(bot))
    await _async_redis.config_set("notify-keyspace-events", "KEA")
