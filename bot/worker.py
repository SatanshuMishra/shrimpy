"""
RQ Worker - processes render jobs from Redis queue.

This worker is typically started by the worker_supervisor.py, which handles:
- Spawning multiple workers (configurable 1-4)
- Auto-respawning crashed workers
- Graceful shutdown

RQ handles job retry automatically via the Retry object when jobs are enqueued.
"""

import logging
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


def _log_failure_handler(job, exc_type, exc_value, traceback):
    """Log every job failure so worker logs show which job failed and why."""
    logger.error(
        "[RENDER] job_id=%s worker_id=%s FAILED: %s %s",
        job.id if job else "?",
        os.environ.get("WORKER_ID", "?"),
        exc_type.__name__ if exc_type else "?",
        exc_value,
        exc_info=(exc_type, exc_value, traceback),
    )
    return False  # let other handlers run

# Logging setup - output to stderr for supervisor capture
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | worker-%(worker_id)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)

# Get worker ID from environment (set by supervisor)
WORKER_ID = os.environ.get("WORKER_ID", "0")

# Inject worker_id into log records
old_factory = logging.getLogRecordFactory()


def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.worker_id = WORKER_ID
    return record


logging.setLogRecordFactory(record_factory)

logger = logging.getLogger("rq_worker")

QUEUES = ["single", "dual"]

_url = f"redis://:{cfg.redis.password}@{cfg.redis.host}:{cfg.redis.port}/"
_redis = redis.from_url(_url)


def run_worker(queues: Union[list, None]):
    queues = queues if queues else QUEUES

    logger.info("Worker starting (PID %d, queues: %s)", os.getpid(), queues)

    # Use SimpleWorker on Windows since os.fork() is not available
    WorkerClass = SimpleWorker if platform.system() == "Windows" else Worker

    worker = WorkerClass(
        list(map(lambda q: Queue(q, connection=_redis), queues)),
        connection=_redis,
        exception_handlers=[_log_failure_handler, cooldown_handler, timeout_handler],
        log_job_description=True,  # Log job details
    )

    try:
        worker.work(logging_level=logging.INFO)
    except KeyboardInterrupt:
        logger.info("Worker received interrupt, shutting down...")
    except Exception as e:
        logger.error("Worker crashed with exception: %s", e, exc_info=True)
        raise
    finally:
        logger.info("Worker stopped (PID %d)", os.getpid())


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
