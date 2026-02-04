"""
Worker Supervisor - Industry-standard process supervision for RQ workers.

ROOT CAUSE OF RENDER STALLS (why the queue kept stopping):
1. Subprocess PIPE deadlock: Workers were spawned with stdout=subprocess.PIPE
   but the supervisor never read from the pipe. When workers wrote logs, the OS
   pipe buffer (e.g. 64KB on Windows) filled; workers then blocked on write().
   All workers hung, so no jobs were processed and the batch appeared "stalled".
2. Fix: Redirect worker stdout/stderr to per-worker log files. Do NOT use PIPE
   unless the parent reads from it (e.g. in a thread).

Features:
- Spawns N worker processes (configurable, 1-4)
- Monitors workers and respawns on crash
- Graceful shutdown on SIGTERM/SIGINT
- Log files kept open for process lifetime (closed on crash/stop)
- Failed jobs automatically retry (RQ Retry) up to 3 times

Best practices implemented:
- Supervisor pattern (like supervisord but pure Python for Windows compatibility)
- Health checks via process monitoring
- Exponential backoff on repeated crashes
- Clean shutdown handling
"""

import argparse
import logging
import os
import platform
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, Optional

# Add parent to path so we can import config
sys.path.insert(1, os.path.join(sys.path[0], ".."))

from config import cfg

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("worker_supervisor")

# Configuration
WORKER_SCRIPT = Path(__file__).parent / "worker.py"
QUEUES = ["single", "dual"]
MIN_WORKERS = 1
MAX_WORKERS = 4

# Crash recovery: if a worker crashes repeatedly, back off before respawning
CRASH_BACKOFF_INITIAL = 1.0  # seconds
CRASH_BACKOFF_MAX = 30.0  # seconds
CRASH_BACKOFF_MULTIPLIER = 2.0
CRASH_RESET_AFTER = 60.0  # reset backoff if worker runs this long without crashing


@dataclass
class WorkerState:
    """Tracks state for a single worker process."""
    worker_id: int
    process: Optional[subprocess.Popen] = None
    log_file: Optional[IO[str]] = None  # Kept open so worker stdout stays valid
    start_time: Optional[float] = None
    crash_count: int = 0
    last_crash_time: Optional[float] = None
    backoff: float = CRASH_BACKOFF_INITIAL


class WorkerSupervisor:
    """
    Supervisor that manages multiple RQ worker processes.
    
    Industry best practices:
    - Process isolation: each worker is a separate process
    - Crash recovery: automatic respawn with backoff
    - Graceful shutdown: SIGTERM handling
    - Health monitoring: periodic process checks
    """

    def __init__(self, worker_count: int):
        self.worker_count = max(MIN_WORKERS, min(MAX_WORKERS, worker_count))
        self.workers: dict[int, WorkerState] = {}
        self.running = True
        self.python_exe = self._find_python()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(
            "Supervisor starting with %d worker(s) (requested %d, clamped to %d-%d)",
            self.worker_count,
            worker_count,
            MIN_WORKERS,
            MAX_WORKERS,
        )

    def _find_python(self) -> str:
        """Find the Python executable (prefer venv)."""
        # Check for venv in project root
        project_root = Path(__file__).parent.parent
        if platform.system() == "Windows":
            venv_python = project_root / "venv" / "Scripts" / "python.exe"
        else:
            venv_python = project_root / "venv" / "bin" / "python"
        
        if venv_python.exists():
            return str(venv_python)
        return sys.executable

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM for graceful shutdown."""
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, initiating graceful shutdown...", sig_name)
        self.running = False

    def _spawn_worker(self, worker_id: int) -> Optional[subprocess.Popen]:
        """Spawn a single worker process."""
        try:
            # Build command
            cmd = [
                self.python_exe,
                str(WORKER_SCRIPT),
                "-q",
            ] + QUEUES
            
            # Environment setup
            env = os.environ.copy()
            env["WORKER_ID"] = str(worker_id)
            
            # Worker log files (one per worker for debugging)
            project_root = Path(__file__).parent.parent
            generated_dir = project_root / "generated"
            generated_dir.mkdir(exist_ok=True)
            worker_log = generated_dir / f"shrimpy-worker-{worker_id}.log"
            
            # Spawn process (no console window on Windows)
            creation_flags = 0
            if platform.system() == "Windows":
                creation_flags = subprocess.CREATE_NO_WINDOW
            
            # CRITICAL: Redirect to log file instead of PIPE to prevent deadlock.
            # Using PIPE without reading causes workers to hang when buffer fills.
            # Keep log_file open for process lifetime; close on respawn/stop.
            log_file = open(worker_log, "w")
            process = subprocess.Popen(
                cmd,
                cwd=str(project_root),
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=creation_flags,
            )
            
            state = self.workers.get(worker_id)
            if not state:
                state = WorkerState(worker_id=worker_id)
                self.workers[worker_id] = state
            state.process = process
            state.log_file = log_file
            state.start_time = time.time()
            
            logger.info(
                "Worker %d spawned (PID %d, log: %s)",
                worker_id,
                process.pid,
                worker_log.name,
            )
            return process
            
        except Exception as e:
            logger.error("Failed to spawn worker %d: %s", worker_id, e)
            return None

    def _check_worker(self, worker_id: int) -> bool:
        """
        Check if a worker is alive. Returns True if running, False if dead/crashed.
        If crashed, updates crash statistics for backoff calculation.
        """
        state = self.workers.get(worker_id)
        if not state or not state.process:
            return False
        
        # Check if process is still running
        poll_result = state.process.poll()
        if poll_result is None:
            # Still running
            # Reset crash backoff if worker has been stable for a while
            if state.start_time and (time.time() - state.start_time) >= CRASH_RESET_AFTER:
                if state.crash_count > 0:
                    logger.info(
                        "Worker %d stable for %.0fs, resetting crash backoff",
                        worker_id,
                        time.time() - state.start_time,
                    )
                    state.crash_count = 0
                    state.backoff = CRASH_BACKOFF_INITIAL
            return True
        
        # Process exited
        exit_code = poll_result
        runtime = time.time() - state.start_time if state.start_time else 0
        
        # Update crash statistics
        state.crash_count += 1
        state.last_crash_time = time.time()
        state.backoff = min(
            CRASH_BACKOFF_MAX,
            state.backoff * CRASH_BACKOFF_MULTIPLIER,
        )
        
        logger.warning(
            "Worker %d crashed (PID %d, exit %d, runtime %.1fs, crash #%d, next backoff %.1fs)",
            worker_id,
            state.process.pid,
            exit_code,
            runtime,
            state.crash_count,
            state.backoff,
        )
        
        if state.log_file:
            try:
                state.log_file.close()
            except Exception:
                pass
            state.log_file = None
        state.process = None
        state.start_time = None
        return False

    def _respawn_if_needed(self, worker_id: int):
        """Respawn a worker if it's not running, respecting backoff."""
        state = self.workers.get(worker_id)
        if not state:
            state = WorkerState(worker_id=worker_id)
            self.workers[worker_id] = state
        
        # Already running
        if state.process and state.process.poll() is None:
            return
        
        # Check backoff
        if state.last_crash_time:
            time_since_crash = time.time() - state.last_crash_time
            if time_since_crash < state.backoff:
                # Still in backoff period
                return
        
        # Close old log file before respawn (if any)
        if state.log_file:
            try:
                state.log_file.close()
            except Exception:
                pass
            state.log_file = None
        
        # Spawn new worker
        process = self._spawn_worker(worker_id)
        if process:
            state.process = process
            state.start_time = time.time()

    def _stop_all_workers(self):
        """Stop all worker processes gracefully."""
        logger.info("Stopping all workers...")
        
        for worker_id, state in self.workers.items():
            if state.process and state.process.poll() is None:
                logger.info("Stopping worker %d (PID %d)...", worker_id, state.process.pid)
                try:
                    state.process.terminate()
                except Exception as e:
                    logger.warning("Failed to terminate worker %d: %s", worker_id, e)
        
        # Wait for workers to exit (with timeout)
        deadline = time.time() + 10  # 10 second timeout
        for worker_id, state in self.workers.items():
            if state.process:
                remaining = max(0, deadline - time.time())
                try:
                    state.process.wait(timeout=remaining)
                    logger.info("Worker %d stopped", worker_id)
                except subprocess.TimeoutExpired:
                    logger.warning("Worker %d did not stop gracefully, killing...", worker_id)
                    state.process.kill()
                    state.process.wait()
            if state.log_file:
                try:
                    state.log_file.close()
                except Exception:
                    pass
                state.log_file = None
        
        logger.info("All workers stopped")

    def run(self):
        """Main supervisor loop."""
        logger.info("Supervisor running, monitoring %d worker(s)...", self.worker_count)
        
        # Initialize worker states
        for i in range(self.worker_count):
            self.workers[i] = WorkerState(worker_id=i)
        
        try:
            while self.running:
                # Check each worker and respawn if needed
                for worker_id in range(self.worker_count):
                    if not self.running:
                        break
                    
                    # Check health
                    self._check_worker(worker_id)
                    
                    # Respawn if needed
                    self._respawn_if_needed(worker_id)
                
                # Sleep before next check (short interval for responsiveness)
                if self.running:
                    time.sleep(1.0)
        
        finally:
            self._stop_all_workers()
            logger.info("Supervisor shutdown complete")


def main():
    parser = argparse.ArgumentParser(
        description="RQ Worker Supervisor - manages multiple workers with auto-respawn"
    )
    parser.add_argument(
        "-n", "--workers",
        type=int,
        default=cfg.worker.count,
        help=f"Number of workers to run (1-{MAX_WORKERS}, default from config: {cfg.worker.count})",
    )
    args = parser.parse_args()
    
    supervisor = WorkerSupervisor(worker_count=args.workers)
    supervisor.run()


if __name__ == "__main__":
    main()
