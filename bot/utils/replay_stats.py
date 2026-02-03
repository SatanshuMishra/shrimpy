"""
Modular battle statistics extraction from World of Warships replays.

This module provides a clean abstraction for extracting battle metadata:
- Win/loss result
- Map name
- Team composition (ships per side)
- Players per team counts

The implementation is decoupled from the renderer and can be reused by:
- Batch render summaries (win rate calculation)
- Future database storage for long-term statistics
- Other bot commands requiring replay metadata

Industry best practice: Single source of truth for battle stats extraction,
with clear data contracts (dataclasses) that are serializable for storage.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VehicleEntry:
    """A single vehicle/player in the battle."""

    ship_id: int
    ship_params_id: int
    player_name: str
    clan_tag: str
    team_id: int
    relation: int  # 0 = friendly (same team as recorder), 1 = enemy, -1 = unknown
    is_bot: bool


@dataclass
class BattleStats:
    """
    Extracted statistics for a single battle/replay.

    All fields are Optional to handle incomplete replays gracefully.
    The `win` field is from the perspective of the player who recorded the replay.
    """

    # Battle result
    win: Optional[bool] = None  # True = recorder's team won, False = lost, None = unknown

    # Map information
    map_name: Optional[str] = None
    map_display_name: Optional[str] = None  # Human-readable map name if available

    # Team composition
    players_per_team: Optional[int] = None
    friendly_count: int = 0
    enemy_count: int = 0

    # Full team composition (for future detailed analysis)
    friendly_team: list[VehicleEntry] = field(default_factory=list)
    enemy_team: list[VehicleEntry] = field(default_factory=list)

    # Battle metadata
    battle_type: Optional[int] = None
    game_version: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to dict for JSON/DB storage."""
        return {
            "win": self.win,
            "map_name": self.map_name,
            "map_display_name": self.map_display_name,
            "players_per_team": self.players_per_team,
            "friendly_count": self.friendly_count,
            "enemy_count": self.enemy_count,
            "friendly_team": [
                {
                    "ship_id": v.ship_id,
                    "ship_params_id": v.ship_params_id,
                    "player_name": v.player_name,
                    "clan_tag": v.clan_tag,
                    "team_id": v.team_id,
                    "relation": v.relation,
                    "is_bot": v.is_bot,
                }
                for v in self.friendly_team
            ],
            "enemy_team": [
                {
                    "ship_id": v.ship_id,
                    "ship_params_id": v.ship_params_id,
                    "player_name": v.player_name,
                    "clan_tag": v.clan_tag,
                    "team_id": v.team_id,
                    "relation": v.relation,
                    "is_bot": v.is_bot,
                }
                for v in self.enemy_team
            ],
            "battle_type": self.battle_type,
            "game_version": self.game_version,
        }


def extract_battle_stats_from_replay_info(replay_info: dict) -> Optional[BattleStats]:
    """
    Extract battle statistics from parsed replay info.

    This version accepts the already-parsed replay_info dict (from ReplayParser.get_info()),
    avoiding double-parsing when the worker already has this data.

    Args:
        replay_info: Dict from ReplayParser.get_info() with 'open', 'hidden', 'error' keys.

    Returns:
        BattleStats if extraction succeeded, None on parse failure.
    """
    try:
        hidden = replay_info.get("hidden")
        if not hidden:
            logger.debug("No hidden data in replay_info")
            return None

        replay_data = hidden.get("replay_data")
        if not replay_data:
            logger.debug("No replay_data in hidden")
            return None

        # Extract basic metadata
        stats = BattleStats(
            map_name=getattr(replay_data, "game_map", None),
            battle_type=getattr(replay_data, "game_battle_type", None),
            game_version=getattr(replay_data, "game_version", None),
        )

        # Get player info and owner info
        player_info = getattr(replay_data, "player_info", {})
        owner_id = getattr(replay_data, "owner_id", None)

        if not player_info:
            logger.debug("No player_info in replay_data")
            return stats

        # Find the recording player's team
        owner_team_id = None
        if owner_id is not None and owner_id in player_info:
            owner_team_id = player_info[owner_id].team_id

        # Build team composition
        friendly_team = []
        enemy_team = []

        for pid, pinfo in player_info.items():
            entry = VehicleEntry(
                ship_id=pinfo.ship_id,
                ship_params_id=pinfo.ship_params_id,
                player_name=pinfo.name,
                clan_tag=pinfo.clan_tag,
                team_id=pinfo.team_id,
                relation=pinfo.relation,
                is_bot=pinfo.is_bot,
            )

            # relation: 0 = friendly (same team as recorder), 1 = enemy
            if pinfo.relation == 0:
                friendly_team.append(entry)
            elif pinfo.relation == 1:
                enemy_team.append(entry)

        stats.friendly_team = friendly_team
        stats.enemy_team = enemy_team
        stats.friendly_count = len(friendly_team)
        stats.enemy_count = len(enemy_team)

        # Calculate players per team (assume symmetric teams)
        if friendly_team:
            stats.players_per_team = len(friendly_team)

        # Determine win/loss from game_result
        game_result = getattr(replay_data, "game_result", None)
        if game_result is not None:
            winning_team_id = game_result.team_id
            # -1 means no result (incomplete replay, still in progress, etc.)
            if winning_team_id != -1 and owner_team_id is not None:
                stats.win = winning_team_id == owner_team_id

        return stats

    except Exception as e:
        logger.warning("Failed to extract battle stats: %s", e)
        return None


def extract_battle_stats(replay_bytes: bytes) -> Optional[BattleStats]:
    """
    Extract battle statistics from raw replay file bytes.

    This is a convenience wrapper that parses the replay first.
    Use extract_battle_stats_from_replay_info() if you already have parsed data.

    Args:
        replay_bytes: Raw .wowsreplay file contents.

    Returns:
        BattleStats if extraction succeeded, None on parse failure.
    """
    try:
        import io
        from replay_parser import ReplayParser

        with io.BytesIO(replay_bytes) as fp:
            replay_info = ReplayParser(fp, strict=False).get_info()
            return extract_battle_stats_from_replay_info(replay_info)
    except Exception as e:
        logger.warning("Failed to parse replay for stats extraction: %s", e)
        return None


def aggregate_win_rate(
    stats_list: list[Optional[BattleStats]],
) -> tuple[int, int, Optional[float]]:
    """
    Calculate aggregate win rate from a list of battle stats.

    Args:
        stats_list: List of BattleStats (or None for failed parses).

    Returns:
        Tuple of (wins, total_with_result, win_rate_percent).
        win_rate_percent is None if total_with_result == 0.
    """
    wins = 0
    total_with_result = 0

    for stats in stats_list:
        if stats is not None and stats.win is not None:
            total_with_result += 1
            if stats.win:
                wins += 1

    if total_with_result == 0:
        return (0, 0, None)

    rate = (wins / total_with_result) * 100
    return (wins, total_with_result, rate)


def format_win_rate_line(
    stats_list: list[Optional[BattleStats]],
) -> Optional[str]:
    """
    Format a user-friendly win rate summary line for Discord.

    Args:
        stats_list: List of BattleStats from a batch.

    Returns:
        Formatted string like "**Win Rate:** 7/10 (70%)" or None if no results.
    """
    wins, total, rate = aggregate_win_rate(stats_list)

    if total == 0:
        return None

    return f"**Win Rate:** {wins}/{total} ({rate:.0f}%)"
