import concurrent.futures
import re

import bs4
from discord.ext import commands, tasks
from discord import app_commands
import discord
import requests

import api
from bot.shrimpy import Shrimpy
from bot.utils.logs import logger


PATTERN = re.compile(r'\[{"title":".+","date":(\d+)},{"title":".+","date":(\d+)}]')


executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


def scrape():
    try:
        result = {}

        for region, url in api.URLS.items():
            route = url + "/en/news"

            response = requests.get(
                route,
                params={"category": "game-updates", "pjax": "1", "multi": "true"},
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

            if response.status_code != 200:
                logger.error(
                    f"Error code {response.status_code} while fetching articles"
                )
                return

            soup = bs4.BeautifulSoup(response.text, "html.parser")
            article = soup.find("article")
            if not article:
                logger.warning(
                    "Updates: no <article> found for %s; page structure may have changed",
                    region,
                )
                continue
            link = article.find("vue-news-link")
            if not link or not link.get("link"):
                logger.warning(
                    "Updates: no vue-news-link or link for %s; page structure may have changed",
                    region,
                )
                continue
            article_url = url + link.get("link")

            response = requests.get(article_url, params={"pjax": "1"})

            if response.status_code != 200:
                logger.error(
                    f"Error code {response.status_code} while fetching update article"
                )
                return

            soup = bs4.BeautifulSoup(response.text, "html.parser")
            local_time = soup.find("vue-local-time")
            if not local_time or not local_time.get("time-list"):
                logger.warning(
                    "Updates: no vue-local-time or time-list for %s; page structure may have changed",
                    region,
                )
                continue
            times = local_time.get("time-list")
            match = re.match(PATTERN, times)

            if not match:
                logger.warning(
                    "Updates: failed to extract times with regex for %s",
                    region,
                )
                continue

            time_from = int(match.group(1)) // 1000
            time_until = int(match.group(2)) // 1000

            result[region] = article_url, time_from, time_until

        return result
    except Exception as e:
        logger.warning("Unhandled exception while loading Updates", exc_info=e)
        return


class UpdateCog(commands.Cog):
    def __init__(self, bot: Shrimpy):
        self.bot = bot

        self.update_data = {}
        self.task_scrape.start()

    @tasks.loop(hours=1)
    async def task_scrape(self):
        logger.info("Loading Updates...")
        result = await self.bot.loop.run_in_executor(executor, scrape)

        if not result:
            logger.warning("Failed to load Updates")
            #
            # if not self.update_data:
            #     import sys
            #
            #     sys.exit(1)
        else:
            self.update_data = result
            logger.info("Updates loaded")

    @app_commands.command(
        name="update",
        description="Fetches latest WoWS update details.",
        extras={"category": "wows"},
    )
    async def update(self, interaction: discord.Interaction):
        if not self.update_data:
            await interaction.response.send_message(
                "Update information currently unavailable. Please try again later.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "**Maintenance Times**\n"
                + "\n".join(
                    f"{region.upper()}\n"
                    f"<{url}>\n"
                    f"From <t:{time_from}:F> to <t:{time_until}:F>\n"
                    f"From <t:{time_from}:R> to <t:{time_until}:R>\n"
                    for region, (url, time_from, time_until) in self.update_data.items()
                )
            )


async def setup(bot: Shrimpy):
    await bot.add_cog(UpdateCog(bot))
