from typing import Union
import collections
import datetime
import json
import os

from discord.ext import commands, tasks
from discord import app_commands
import discord

from bot.shrimpy import Shrimpy
from bot.utils.logs import logger
from config import cfg


# SECURITY: Changed from pickle to JSON to avoid deserialization vulnerabilities
# pickle.load() on untrusted data can lead to arbitrary code execution
STATS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../assets/private/stats.json"
)
# Legacy path for migration
LEGACY_STATS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../assets/private/stats.pickle"
)
CREATED = datetime.datetime.fromtimestamp(cfg.created, datetime.timezone.utc)


class Core(commands.Cog):
    def __init__(self, bot: Shrimpy):
        self.bot: Shrimpy = bot

        self.session = collections.Counter()

        # Load persistent stats from JSON (or migrate from legacy pickle)
        self.persistent = self._load_persistent_stats()

        self.save_stats.start()

    def _load_persistent_stats(self) -> collections.Counter:
        """
        Load persistent stats from JSON file.
        Migrates from legacy pickle format if needed.
        """
        # Try loading from JSON first
        try:
            with open(STATS_PATH, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                return collections.Counter(data)
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to load stats.json: {e}")

        # Try migrating from legacy pickle (one-time migration)
        try:
            import pickle
            with open(LEGACY_STATS_PATH, "rb") as fp:
                legacy_data = pickle.load(fp)
                logger.info("Migrated stats from pickle to JSON format")
                return collections.Counter(legacy_data)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Failed to load legacy stats.pickle: {e}")

        return collections.Counter()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Logged in as {self.bot.user}")

    @commands.Cog.listener()
    async def on_disconnect(self):
        logger.info("Disconnected")

    @commands.Cog.listener()
    async def on_resumed(self):
        logger.info("Reconnected")

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error):
        if isinstance(error, commands.CommandNotFound):
            return
        if isinstance(error, commands.NotOwner):
            logger.warning("Ignored non-owner user")
            return
        else:
            logger.error(
                f"Ignoring exception in command {ctx.command.name}", exc_info=error
            )

    @tasks.loop(minutes=1)
    async def save_stats(self):
        if self.bot.stopping:
            return

        # SECURITY: Save as JSON instead of pickle to avoid deserialization risks
        with open(STATS_PATH, "w", encoding="utf-8") as fp:
            json.dump(dict(self.persistent), fp, indent=2)

    @commands.Cog.listener()
    async def on_app_command_completion(
        self,
        interaction: discord.Interaction,
        command: Union[app_commands.Command, app_commands.ContextMenu],
    ):
        self.session["commands"] += 1
        self.session[command.name] += 1
        self.persistent["commands"] += 1
        self.persistent[command.name] += 1

    @app_commands.command(
        name="status",
        description="View information about the bot.",
        extras={"category": "general"},
    )
    async def status(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            f"Online since: {discord.utils.format_dt(self.bot.online_since, style='R')}\n"
            f"Servers: `{len(self.bot.guilds)}`\n"
            f"Commands (session): `{self.session['commands']}`\n"
            f"Commands (since {discord.utils.format_dt(CREATED, 'd')}): `{self.persistent['commands']}`\n\n"
            "Popular commands:\n"
            + "\n".join(
                f"- {name}: `{count}`"
                for name, count in self.persistent.most_common(5)[1:]
            ),
            ephemeral=True,
        )


async def setup(bot: Shrimpy):
    await bot.add_cog(Core(bot))
