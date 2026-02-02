import aiohttp
import asyncio
import io
import json
import logging
import shutil
import ssl
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")

import discord
import redis
import rq
import rq.job
import rq.worker
from discord import app_commands, ui
from discord.ext import commands

from bot import tasks
from bot.track import Track
from bot.utils import errors, functions
from config import cfg

logger = logging.getLogger("track")
_url = f"redis://:{cfg.redis.password}@localhost:{cfg.redis.port}/"
_redis: redis.Redis = redis.from_url(_url)
_async_redis: redis.asyncio.Redis = redis.asyncio.from_url(_url)

UNKNOWN_JOB_STATUS_RETRY = 5
URL_MAX_LENGTH = 512
BATCH_MAX_REPLAYS = 10  # Discord message attachment limit
BATCH_PENDING_PREFIX = "renderbatch_pending:"
BATCH_PENDING_TTL = 60  # seconds to reply with files before prompt shows timeout
BATCH_POST_RESULT_ATTEMPTS = 5
BATCH_POST_RESULT_RETRY_DELAY_BASE = 1  # base seconds for exponential backoff
FAILED_TO_POST_MSG = "Could not post result (network error)."
# Discord: 8 MB per file; 25 MB total per message (use 8 MB to stay safe for all clients)
DISCORD_MAX_FILE_BYTES = 8 * 1024 * 1024
DISCORD_MAX_MESSAGE_BYTES = 25 * 1024 * 1024
DISCORD_EPHEMERAL_FLAG = 64

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
            self.fp.write(build["name"] + " (" + build["ship"] + ")\n" + build["build_url"] + "&ref=track" + "\n\n")
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
                url = build["build_url"] + "&ref=track"
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

    def __init__(self, bot: Track, interaction: Optional[discord.Interaction]):
        self._bot = bot
        self._interaction = interaction
        self._job: Optional[rq.job.Job] = None

    @property
    def job_position(self) -> int:
        pos = self._job.get_position()
        return pos + 1 if pos else 1

    @property
    def job_ttl(self) -> int:
        return max(self.QUEUE.count, 1) * self.MAX_WAIT_TIME

    async def _reupload(
        self, task_status: Optional[str], exc_info: Optional[str]
    ) -> None:
        raise NotImplementedError()

    async def _check(self) -> bool:
        worker_count = rq.worker.Worker.count(connection=_redis, queue=self.QUEUE)
        cooldown = await _async_redis.ttl(f"cooldown_{self._interaction.user.id}")

        try:
            assert (
                worker_count != 0
            ), f"{self._interaction.user.mention} No running workers detected."
            assert (
                self.QUEUE.count <= self.QUEUE_SIZE
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
        embed = RenderWaitingEmbed(input_name, self.job_position, filename=filename)
        message = await self.message(embed=embed)
        last_position: Optional[int] = None
        last_progress: Optional[float] = None

        psub = _async_redis.pubsub()
        psub.ignore_subscribe_messages = True
        await psub.psubscribe(f"*{self._job.id}")

        async for response in psub.listen():
            if response["type"] != "pmessage":
                continue

            status = self._job.get_status(refresh=True)
            match status:
                case "queued":
                    position = self.job_position
                    if position == last_position:
                        continue

                    embed = RenderWaitingEmbed(
                        input_name, position, filename=filename
                    )
                    message = await message.edit(embed=embed)
                    last_position = position
                case "started":
                    progress = self._job.get_meta(refresh=True).get("progress", 0.0)
                    if progress == last_progress:
                        continue

                    embed = RenderStartedEmbed(
                        input_name, self._job, progress, filename=filename
                    )
                    message = await message.edit(embed=embed)
                    last_progress = progress
                case "finished":
                    if not self._job.result:
                        continue

                    if isinstance(self._job.result, tuple):
                        data, result_fn, time_taken, builds_str, chat = self._job.result

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
                            )
                        except discord.HTTPException:
                            embed = RenderFailureEmbed(
                                input_name,
                                "Rendered file too large (>8 MB). Consider reducing quality.",
                                filename=filename,
                            )
                    elif isinstance(self._job.result, errors.RenderError):
                        embed = RenderFailureEmbed(
                            input_name,
                            self._job.result.message,
                            filename=filename,
                        )
                    else:
                        logger.error(f"Unhandled job result {self._job.result}")
                        embed = RenderFailureEmbed(
                            input_name,
                            "An unhandled error occurred.",
                            filename=filename,
                        )

                    await message.edit(embed=embed)
                    break
                case "failed":
                    timeout = self._job.get_meta(refresh=True).get("timeout", None)
                    if timeout:
                        logger.warning("Render job timed out")
                        embed = RenderFailureEmbed(
                            input_name, "Job timed out.", filename=filename
                        )
                    else:
                        # fetch again to update exc_info
                        job = rq.job.Job.fetch(self._job.id, connection=_redis)
                        task_status = self._job.meta.get("status", None)
                        logger.error(
                            f'Render job failed with status "{task_status}"\n{job.exc_info}'
                        )
                        if "StopIteration" in job.exc_info:
                            err_message = "An unhandled error occurred (likely incomplete replay)."
                        else:
                            err_message = "An unhandled error occurred."

                        embed = RenderFailureEmbed(
                            input_name, err_message, filename=filename
                        )
                        await self._reupload(task_status, job.exc_info)

                    await message.edit(embed=embed)
                    break
                case _base:
                    if status is None:
                        await asyncio.sleep(UNKNOWN_JOB_STATUS_RETRY)
                        status = self._job.get_status(refresh=True)
                        if status is not None:
                            continue

                    logger.warning(f"Unknown job status {status}")
                    embed = RenderFailureEmbed(
                        input_name, "Render job expired.", filename=filename
                    )
                    await message.edit(embed=embed)
                    break

        await psub.unsubscribe()
        await psub.close()


class RenderSingle(Render):
    QUEUE = rq.Queue("single", connection=_redis)

    def __init__(
        self,
        bot: Track,
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
        bot: Track,
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
        bot: Track,
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
            async with aiohttp.ClientSession() as session:
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


def _format_render_summary(filename: Optional[str]) -> Optional[str]:
    """Build '**Date Time:** ... from parsed filename, or None. Labels bolded."""
    parsed = _parse_replay_filename(filename) if filename else None
    if not parsed:
        return None
    map_display = _format_map_display(parsed["map_name"])
    return (
        f"**Date Time:** {parsed['date']} {parsed['time']}\n"
        f"**Ship:** {parsed['ship_name']}\n"
        f"**Map:** {map_display}"
    )


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
        **kwargs,
    ):
        super().__init__(title=title, color=color)

        summary = _format_render_summary(filename)
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
        self, input_name: str, position: int, filename: Optional[str] = None
    ):
        super().__init__(
            self.COLOR,
            RenderEmbed.TITLE_STARTING,
            input_name,
            filename=filename,
            position=position,
        )


class RenderStartedEmbed(RenderEmbed):
    COLOR = 0xE30952  # in-progress left bar: #e30952

    def __init__(
        self,
        input_name: str,
        job: rq.job.Job,
        progress: float,
        filename: Optional[str] = None,
    ):
        if task_status := job.get_meta(refresh=True).get("status", None):
            super().__init__(
                self.COLOR,
                RenderEmbed.TITLE_PROCESSING,
                input_name,
                filename=filename,
                status=task_status.title(),
                progress=progress,
            )
        else:
            super().__init__(
                self.COLOR,
                RenderEmbed.TITLE_PROCESSING,
                input_name,
                filename=filename,
                status="Started",
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
            result=result_msg,
            time_taken=time_taken,
        )


class RenderFailureEmbed(RenderEmbed):
    COLOR = 0x32A885  # complete (error) left bar: #32a885

    def __init__(
        self, input_name: str, err_message, filename: Optional[str] = None
    ):
        super().__init__(
            self.COLOR,
            RenderEmbed.TITLE_SUMMARY,
            input_name,
            filename=filename,
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
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                return resp.status in (200, 204)
    except Exception as e:
        logger.warning("Failed to send ephemeral followup: %s", e)
        return False


async def _edit_batch_prompt_to_timeout(
    bot: Track,
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
    worker_count = rq.worker.Worker.count(connection=_redis, queue=RenderSingle.QUEUE)
    try:
        cooldown = _redis.ttl(f"cooldown_{user_id}")
        task_request_exists = _redis.exists(f"task_request_{user_id}")
        queue_count = RenderSingle.QUEUE.count
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


async def _poll_single_batch_job(
    bot: Track,
    channel: discord.abc.Messageable,
    status_message: discord.Message,
    job_id: str,
    filename: str,
    replay_bytes: bytes,
    job_index: int,
    total: int,
    results_list: list,
    lock: asyncio.Lock,
    single_message: bool = False,
    rendered_files: Optional[list] = None,
) -> None:
    """
    Poll ONE batch job until it finishes/fails.
    Updates results_list[job_index] and the status embed.
    Does NOT handle combined send or cleanup (coordinator does that).
    """
    job = rq.job.Job.fetch(job_id, connection=_redis)
    psub = _async_redis.pubsub()
    psub.ignore_subscribe_messages = True
    await psub.psubscribe(f"*{job_id}")

    try:
        async for response in psub.listen():
            if response["type"] != "pmessage":
                continue

            status = job.get_status(refresh=True)
            if status not in ("finished", "failed"):
                continue

            async with lock:
                if status == "finished" and job.result:
                    if isinstance(job.result, tuple):
                        data, out_name, time_taken, builds_str, chat = job.result
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
                    timeout = job.get_meta(refresh=True).get("timeout", False)
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
                try:
                    await status_message.edit(embed=embed)
                except Exception as e:
                    logger.warning("Could not update batch status embed: %s", e)
            break
    finally:
        await psub.unsubscribe()
        await psub.close()


async def _batch_coordinator(
    bot: Track,
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
) -> None:
    """
    Coordinator: spawn poll tasks, wait for ALL to complete, then send and cleanup.
    Cleanup (task_request, cooldown, temp dir) ALWAYS runs in finally so the bot
    stays usable even when delivery fails.
    """
    lock = asyncio.Lock()
    total = len(job_ids)
    batch_temp_dir: Optional[Path] = None

    try:
        # Create poll tasks for each job
        poll_tasks = [
            asyncio.create_task(
                _poll_single_batch_job(
                    bot,
                    channel,
                    status_message,
                    job_id,
                    filename,
                    replay_bytes,
                    i,
                    total,
                    results_list,
                    lock,
                    single_message,
                    rendered_files,
                )
            )
            for i, (job_id, filename, replay_bytes) in enumerate(
                zip(job_ids, filenames, bytes_list)
            )
        ]

        # Wait for ALL poll tasks to complete (exceptions absorbed)
        await asyncio.gather(*poll_tasks, return_exceptions=True)

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
                for bucket_idx, bucket in enumerate(buckets):
                    if include_summary:
                        lines = [
                            f"{idx + 1}. **{format_replay_display(results_list[i][0])}**"
                            for idx, (i, _, _) in enumerate(bucket)
                        ]
                        bucket_content = (
                            (f"# Renders for {date_title}\n\n" if bucket_idx == 0 else "")
                            + "\n".join(lines)
                        )
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
                    summary_lines = []
                    for i, (fn, st, link) in enumerate(results_list):
                        display = format_replay_display(fn)
                        line = f"{i + 1}. **{display}**: {st}"
                        if st == "Done" and link:
                            line += f" — [Link]({link})"
                        elif st == "Failed" and link:
                            line += f" — {link}"
                        summary_lines.append(line)
                    content = (
                        f"# Renders for {date_title}\n\n" + "\n".join(summary_lines)
                    )
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
        embed = discord.Embed(
            title=RenderEmbed.TITLE_SUMMARY,
            color=RenderEmbed.BATCH_COLOR_COMPLETE,
            description="\n".join(desc_lines),
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
    def __init__(self, bot: Track):
        self.bot: Track = bot

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
        job_ttl = max(queue.count + len(bytes_list), 1) * RenderSingle.MAX_WAIT_TIME
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
                failure_ttl=RenderSingle.FINISHED_TTL,
                result_ttl=RenderSingle.FINISHED_TTL,
                ttl=job_ttl,
            )
            job_ids.append(job.id)

        results_list = [(fn, "Queued", None) for fn in filenames]
        single_message = payload.get("single_message", False)
        include_summary = payload.get("include_summary", False)
        rendered_files = [None] * len(replays)
        desc_lines = [
            f"{i + 1}. **{format_replay_display(fn)}**: Queued"
            for i, fn in enumerate(filenames)
        ]
        embed = discord.Embed(
            title=RenderEmbed.BATCH_TITLE_PROCESSING,
            color=RenderEmbed.BATCH_COLOR_PROCESSING,
            description=f"Batch queued: **{len(replays)}** replays.\n\n" + "\n".join(desc_lines),
        )
        prompt_message = await message.channel.fetch_message(ref_id)
        await prompt_message.edit(embed=embed)

        interaction_token = payload.get("interaction_token")
        application_id = payload.get("application_id")

        # Use coordinator pattern: ONE task handles all jobs + final send/cleanup
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

        async with aiohttp.ClientSession() as session:
            for replay in data["replays"]:
                async with session.get(replay["replay"]) as response:
                    files.append(await response.read())

        render = RenderWT(
            self.bot,
            files[0],
            files[1],
            data["targetChannelId"],
            f"{data['callbackUrl']}",
        )
        await render.start(*[replay["tag"].upper() for replay in data["replays"]])


async def setup(bot: Track):
    await bot.add_cog(RenderCog(bot))
    await _async_redis.config_set("notify-keyspace-events", "KEA")
