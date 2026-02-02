import os
import platform
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import argparse
from typing import Union

import redis
from rq import Queue, SimpleWorker, Worker

from config import cfg
from bot.tasks import cooldown_handler, timeout_handler

QUEUES = ["single", "dual"]

_url = f"redis://:{cfg.redis.password}@{cfg.redis.host}:{cfg.redis.port}/"
_redis = redis.from_url(_url)


def run_worker(queues: Union[list, None]):
    queues = queues if queues else QUEUES

    # Use SimpleWorker on Windows since os.fork() is not available
    WorkerClass = SimpleWorker if platform.system() == "Windows" else Worker

    worker = WorkerClass(
        list(map(lambda q: Queue(q, connection=_redis), queues)),
        connection=_redis,
        exception_handlers=[cooldown_handler, timeout_handler]
    )
    worker.work()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the worker.")
    parser.add_argument(
        "-q",
        "--queues",
        nargs="+",
        choices=QUEUES,
        required=True,
    )
    args = parser.parse_args()

    run_worker(args.queues)
