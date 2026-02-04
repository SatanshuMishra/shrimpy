__all__ = ["logger", "handler"]

import logging
import logging.handlers
import os
import sys

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs/shrimpy.log")

logger = logging.getLogger("shrimpy")
# DEBUG when SHIMPY_DEBUG=1 or LOG_LEVEL=DEBUG for verbose timestamp/summary logs
if os.environ.get("SHIMPY_DEBUG") or os.environ.get("LOG_LEVEL", "").upper() == "DEBUG":
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
logging.getLogger("discord").setLevel(logging.INFO)
logging.getLogger("discord.http").setLevel(logging.WARNING)

fmt = "[{asctime}] [{levelname:<8}] {name}: {message}"
dt_fmt = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(fmt, dt_fmt, style="{")

# File handler: primary log (bot/logs/shrimpy.log)
handler = logging.handlers.RotatingFileHandler(
    filename=PATH,
    encoding="utf-8",
    maxBytes=32 * 1024 * 1024,  # 32 MiB
    backupCount=5,  # Rotate through 5 files
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Stream handler: also write to stderr so dev.ps1 -RedirectStandardError captures logs
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
