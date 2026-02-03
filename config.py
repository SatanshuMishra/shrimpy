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


def _parse_owner_ids(value: str):
    return {int(x.strip()) for x in value.split(",") if x.strip()} if value else None


def _build_environ():
    env = {
        "CREATED": int(os.environ.get("CREATED", 1663989263)),
        "DISCORD_OWNER_IDS": _parse_owner_ids(os.environ.get("DISCORD_OWNER_IDS", ""))
        or {212466672450142208, 113104128783159296},
        "CHANNELS_FAILED_RENDERS": int(
            os.environ.get("CHANNELS_FAILED_RENDERS", 1010834704804614184)
        ),
        "REDIS_PORT": int(os.environ.get("REDIS_PORT", 6379)),
    }
    if "REDIS_PASSWORD" in os.environ:
        env["REDIS_PASSWORD"] = os.environ["REDIS_PASSWORD"]
    if "REDIS_HOST" in os.environ:
        env["REDIS_HOST"] = os.environ["REDIS_HOST"]
    return env


cfg: ShrimpyConfig = ShrimpyConfig.from_environ(environ=_build_environ())
