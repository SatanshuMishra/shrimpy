from typing import Optional

from discord.ext import commands
from discord import app_commands
import discord

from bot.utils import assets


from bot.shrimpy import Shrimpy


class FunCog(commands.Cog):
    def __init__(self, bot: Shrimpy):
        self.bot: Shrimpy = bot

    @app_commands.command(
        name="aah", description="Monday is coming", extras={"category": "fun"}
    )
    @app_commands.describe(hd="extra pixels?")
    async def aah(self, interaction: discord.Interaction, hd: Optional[bool] = False):
        if hd:
            await interaction.response.send_message(assets.get("HDAAH"))
        else:
            await interaction.response.send_message(assets.get("AAH"))

    @app_commands.command(name="pog", description="poggers", extras={"category": "fun"})
    async def pog(self, interaction: discord.Interaction):
        await interaction.response.send_message(assets.get("POGGERS"))


async def setup(bot: Shrimpy):
    await bot.add_cog(FunCog(bot))
