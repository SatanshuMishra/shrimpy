import os

import environ

SECRETS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "secrets.ini")
ENVIRONMENT = os.environ.get("ENVIRONMENT", default="testing")
ini_secrets = environ.secrets.INISecrets.from_path(SECRETS_PATH, ENVIRONMENT)


@environ.config(prefix="")
class ShrimpyConfig:
    created = environ.var(converter=int)

    @environ.config(prefix="DISCORD")
    class Discord:
        owner_ids = environ.var(converter=set)
        default_prefix = environ.var(")")
        token = ini_secrets.secret(name="discord_token")

    discord = environ.group(Discord)

    @environ.config(prefix="REDIS")
    class Redis:
        port = environ.var(6379, converter=int)
        password = environ.var(
            name="REDIS_PASSWORD", default=ini_secrets.secret(name="redis_password")
        )
        host = environ.var(
            name="REDIS_HOST", default=ini_secrets.secret(name="redis_host")
        )

    redis = environ.group(Redis)

    @environ.config(prefix="CHANNELS")
    class ChannelIDs:
        failed_renders = environ.var(converter=int)

    channels = environ.group(ChannelIDs)

    @environ.config(prefix="WG")
    class Wargaming:
        app_id = ini_secrets.secret(name="wg_application_id")

    wg = environ.group(Wargaming)

    @environ.config(prefix="TWITTER")
    class Twitter:
        token = ini_secrets.secret(name="twitter_bearer_token", default=None)

    twitter = environ.group(Twitter)

    @environ.config(prefix="RENDER")
    class Render:
        # Timezone in which the replay filename's date/time is recorded (usually YOUR local time).
        # Set to your IANA timezone (e.g. "America/New_York") so batch summary times match when you played.
        # Default "UTC" treats filename time as UTC, so Discord will show the wrong local time unless replays are in UTC.
        replay_timezone = environ.var("UTC")

    render = environ.group(Render)

    @environ.config(prefix="WORKER")
    class Worker:
        # Number of parallel render workers (min 1, max 4). More workers = faster batch processing.
        # Each worker processes one job at a time; N workers = N parallel renders.
        count = environ.var(1, converter=int)

    worker = environ.group(Worker)


def _parse_owner_ids(value: str):
    return {int(x.strip()) for x in value.split(",") if x.strip()} if value else None


def _build_environ():
    # Worker count: min 1, max 4 (clamped)
    raw_worker_count_str = os.environ.get("WORKER_COUNT", "")
    if not raw_worker_count_str:
        try:
            secret_val = ini_secrets.secret(name="worker_count", default=None)
            if secret_val and isinstance(secret_val, str):
                raw_worker_count_str = secret_val
            else:
                raw_worker_count_str = "1"
        except Exception:
            raw_worker_count_str = "1"
    try:
        raw_worker_count = int(raw_worker_count_str)
    except (ValueError, TypeError):
        raw_worker_count = 1
    worker_count = max(1, min(4, raw_worker_count))
    env = {
        "CREATED": int(os.environ.get("CREATED", 1663989263)),
        "DISCORD_OWNER_IDS": _parse_owner_ids(os.environ.get("DISCORD_OWNER_IDS", ""))
        or {212466672450142208, 113104128783159296},
        "CHANNELS_FAILED_RENDERS": int(
            os.environ.get("CHANNELS_FAILED_RENDERS", 1010834704804614184)
        ),
        "REDIS_PORT": int(os.environ.get("REDIS_PORT", 6379)),
        "RENDER_REPLAY_TIMEZONE": os.environ.get("RENDER_REPLAY_TIMEZONE")
        or ini_secrets.secret(name="render_replay_timezone", default="UTC")
        or "UTC",
        "WORKER_COUNT": worker_count,
    }
    if "REDIS_PASSWORD" in os.environ:
        env["REDIS_PASSWORD"] = os.environ["REDIS_PASSWORD"]
    if "REDIS_HOST" in os.environ:
        env["REDIS_HOST"] = os.environ["REDIS_HOST"]
    if "RENDER_REPLAY_TIMEZONE" in os.environ:
        env["RENDER_REPLAY_TIMEZONE"] = os.environ["RENDER_REPLAY_TIMEZONE"]
    return env


cfg: ShrimpyConfig = ShrimpyConfig.from_environ(environ=_build_environ())
