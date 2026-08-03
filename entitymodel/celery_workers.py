"""
Start and stop a fleet of Celery workers from a configuration dict.

Each entry names a queue, how many worker processes to run on it, and where
that worker logs:

    WORKERS = {
        "pipeline": {"queue": "pipeline", "concurrency": 4, "log_path": "logs/pipeline.log"},
        "archive":  {"queue": "archive",  "concurrency": 1, "log_path": "logs/archive.log"},
    }

    manager = WorkerFleet(WORKERS, app="myapp.celery_app", state_dir="run")
    manager.start()          # skips anything already running
    manager.supervise()      # blocks; SIGTERM stops the workers it started

The manager runs workers as its own children and waits, rather than detaching
them, because "SIGTERM stops the workers" only means anything if something is
alive to receive it. Liveness is tracked in pid files so a *second* invocation
can tell that a worker from a previous run is still up -- os.kill(pid, 0)
answers that without needing a broker, unlike Celery's own inspect().ping().

    start()          skip workers already running (the default)
    start(force=True) kill those first, then start fresh
    stop()           terminate whatever the pid files point at
    status()         what is running, according to the pid files

A pid file whose process is gone -- a worker that crashed, or a machine that
rebooted -- is treated as not running and cleaned up, so a stale file can't
wedge the fleet permanently.
"""

from __future__ import annotations

import errno
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

__all__ = ["WorkerFleet", "WorkerSpec", "load_specs"]

# How long to wait for a worker to exit after SIGTERM before SIGKILL. Celery
# uses SIGTERM for warm shutdown, so this is "finish the task in flight".
DEFAULT_STOP_TIMEOUT = 30.0


@dataclass(frozen=True)
class WorkerSpec:
    """One worker: which queue, how many processes, where it logs."""

    name: str
    queue: str
    concurrency: int = 1
    log_path: str | None = None
    loglevel: str = "INFO"
    extra_args: tuple[str, ...] = ()

    def node_name(self, hostname: str | None = None) -> str:
        return f"{self.name}@{hostname or os.uname().nodename}"


def load_specs(config: dict[str, dict]) -> list[WorkerSpec]:
    """
    Turn the configuration dict into WorkerSpecs, failing on anything
    malformed before a single process is started -- a typo in one entry should
    not leave half a fleet running.

    `concurrency` is Celery's -c: how many child processes that worker forks,
    which is what bounds how many tasks it runs at once.
    """
    specs = []
    for name, entry in config.items():
        if not isinstance(entry, dict):
            raise ValueError(f"worker {name!r}: expected a dict, got {type(entry).__name__}")

        unknown = set(entry) - {"queue", "concurrency", "log_path", "loglevel", "extra_args"}
        if unknown:
            raise ValueError(f"worker {name!r}: unknown key(s) {sorted(unknown)}")

        queue = entry.get("queue", name)
        concurrency = entry.get("concurrency", 1)
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError(f"worker {name!r}: concurrency must be a positive int, got {concurrency!r}")

        specs.append(
            WorkerSpec(
                name=name,
                queue=queue,
                concurrency=concurrency,
                log_path=entry.get("log_path"),
                loglevel=entry.get("loglevel", "INFO"),
                extra_args=tuple(entry.get("extra_args", ())),
            )
        )

    names = [s.name for s in specs]
    if len(set(names)) != len(names):
        raise ValueError("duplicate worker names in the configuration")
    return specs


def _alive(pid: int) -> bool:
    """Signal 0 checks for existence without delivering anything."""
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True  # someone else's process, but it exists
        raise
    return True


class WorkerFleet:
    def __init__(
        self,
        config: dict[str, dict],
        *,
        app: str,
        state_dir: str | Path = ".celery",
        celery_bin: str | None = None,
        stop_timeout: float = DEFAULT_STOP_TIMEOUT,
    ) -> None:
        self.specs = load_specs(config)
        self.app = app
        self.state_dir = Path(state_dir)
        # sys.executable -m celery, not a bare "celery", so the fleet uses the
        # same interpreter and virtualenv as whatever started it.
        self.celery_bin = celery_bin
        self.stop_timeout = stop_timeout
        self._started: dict[str, subprocess.Popen] = {}
        self._stopping = False

    # -- pid files ---------------------------------------------------------
    def pid_file(self, spec: WorkerSpec) -> Path:
        return self.state_dir / f"{spec.name}.pid"

    def _exited(self, spec: WorkerSpec, pid: int) -> bool:
        """
        Has this worker really gone?

        For a worker we started, ask the Popen handle: os.kill(pid, 0) still
        succeeds for a terminated child until it is reaped, so a zombie reads
        as alive and every stop would burn the full timeout before SIGKILLing
        something already dead. poll() reaps it and tells the truth. For a
        worker from a previous invocation there is no handle, and it is not
        our child, so it cannot be a zombie of ours -- os.kill is right there.
        """
        process = self._started.get(spec.name)
        if process is not None and process.pid == pid:
            return process.poll() is not None
        return not _alive(pid)

    def running_pid(self, spec: WorkerSpec) -> int | None:
        """The live pid for this worker, or None. Clears a stale pid file."""
        path = self.pid_file(spec)
        if not path.exists():
            return None
        try:
            pid = int(path.read_text().strip())
        except (ValueError, OSError):
            path.unlink(missing_ok=True)
            return None
        if not self._exited(spec, pid):
            return pid
        path.unlink(missing_ok=True)
        return None

    # -- commands ----------------------------------------------------------
    def command(self, spec: WorkerSpec) -> list[str]:
        binary = [self.celery_bin] if self.celery_bin else [sys.executable, "-m", "celery"]
        cmd = [
            *binary,
            "-A", self.app,
            "worker",
            "-Q", spec.queue,
            "-c", str(spec.concurrency),
            "-n", spec.node_name(),
            "--loglevel", spec.loglevel,
        ]
        if spec.log_path:
            cmd += ["--logfile", str(spec.log_path)]
        cmd += list(spec.extra_args)
        return cmd

    # -- lifecycle ---------------------------------------------------------
    def start(self, force: bool = False) -> dict[str, str]:
        """
        Start every configured worker. Returns name -> "started" | "skipped".

        Already-running workers are skipped unless force, which stops them
        first. Skipping is the default because an init script re-run should be
        harmless; restarting would drop tasks in flight.
        """
        self.state_dir.mkdir(parents=True, exist_ok=True)
        outcome: dict[str, str] = {}

        for spec in self.specs:
            pid = self.running_pid(spec)
            if pid is not None:
                if not force:
                    outcome[spec.name] = "skipped"
                    continue
                self._stop_one(spec, pid)

            # A worker must not inherit our stdout. Celery's --logfile covers
            # its own logging, but anything written before logging is set up --
            # a startup traceback, output from C extensions -- would otherwise
            # land in the parent's stream, and worse, hold it open: anything
            # reading this script's output would block until the workers
            # themselves exited, which for a worker is never.
            if spec.log_path:
                Path(spec.log_path).parent.mkdir(parents=True, exist_ok=True)
                stream = open(spec.log_path, "ab", buffering=0)
            else:
                stream = subprocess.DEVNULL

            try:
                process = subprocess.Popen(
                    self.command(spec),
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
            finally:
                if stream is not subprocess.DEVNULL:
                    stream.close()
            self.pid_file(spec).write_text(str(process.pid))
            self._started[spec.name] = process
            outcome[spec.name] = "started"

        return outcome

    def _stop_one(self, spec: WorkerSpec, pid: int) -> bool:
        """SIGTERM, wait for warm shutdown, then SIGKILL. True if it exited."""
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                raise

        deadline = time.monotonic() + self.stop_timeout
        while time.monotonic() < deadline:
            if self._exited(spec, pid):
                self._started.pop(spec.name, None)
                self.pid_file(spec).unlink(missing_ok=True)
                return True
            time.sleep(0.05)

        try:
            os.kill(pid, signal.SIGKILL)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                raise
        # Reap if it is one of ours, so it doesn't linger as a zombie.
        process = self._started.pop(spec.name, None)
        if process is not None:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        self.pid_file(spec).unlink(missing_ok=True)
        return False

    def stop(self) -> dict[str, str]:
        """Stop every configured worker. Returns name -> "stopped" | "killed" | "not running"."""
        outcome: dict[str, str] = {}
        for spec in self.specs:
            pid = self.running_pid(spec)
            if pid is None:
                outcome[spec.name] = "not running"
                continue
            outcome[spec.name] = "stopped" if self._stop_one(spec, pid) else "killed"
        return outcome

    def status(self) -> dict[str, int | None]:
        return {spec.name: self.running_pid(spec) for spec in self.specs}

    def supervise(self, poll_interval: float = 1.0) -> int:
        """
        Block until SIGTERM/SIGINT, then stop the workers and return.

        Only the workers this process started are supervised; ones that were
        skipped belong to whoever started them and are left alone.
        """
        received: list[int] = []

        def handler(signum, _frame):
            received.append(signum)
            self._stopping = True

        previous = {
            sig: signal.signal(sig, handler) for sig in (signal.SIGTERM, signal.SIGINT)
        }
        try:
            while not self._stopping:
                for name, process in list(self._started.items()):
                    if process.poll() is not None:
                        # A worker died on its own; drop its pid file so the
                        # next start() does not think it is still up.
                        spec = next(s for s in self.specs if s.name == name)
                        self.pid_file(spec).unlink(missing_ok=True)
                        del self._started[name]
                if not self._started:
                    break
                time.sleep(poll_interval)
        finally:
            for sig, handler_before in previous.items():
                signal.signal(sig, handler_before)

        for spec in self.specs:
            if spec.name in self._started:
                pid = self.running_pid(spec)
                if pid is not None:
                    self._stop_one(spec, pid)

        return received[0] if received else 0


# --------------------------------------------------------------------------
# Command line: the init script itself.
# --------------------------------------------------------------------------
def _import_attr(dotted: str):
    """Import "package.module:attr" or "package.module.attr"."""
    import importlib

    module_name, _, attr = dotted.partition(":")
    if not attr:
        module_name, _, attr = dotted.rpartition(".")
    if not module_name or not attr:
        raise SystemExit(f"expected module:attr or module.attr, got {dotted!r}")
    try:
        return getattr(importlib.import_module(module_name), attr)
    except ImportError as exc:
        raise SystemExit(f"cannot import {module_name!r}: {exc}") from exc
    except AttributeError as exc:
        raise SystemExit(f"{module_name!r} has no attribute {attr!r}") from exc


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m entitymodel.celery_workers",
        description="Start, stop and inspect a Celery worker fleet.",
    )
    parser.add_argument("--config", required=True,
                        help="worker config dict, as module:attr")
    parser.add_argument("--app", required=True, help="Celery app, as module:attr")
    parser.add_argument("--state-dir", default=".celery",
                        help="where pid files live (default: .celery)")
    parser.add_argument("--force", action="store_true",
                        help="restart workers that are already running")
    parser.add_argument("--stop", action="store_true", help="stop the fleet and exit")
    parser.add_argument("--status", action="store_true", help="report what is running and exit")
    parser.add_argument("--no-supervise", action="store_true",
                        help="start and exit, leaving the workers running")
    parser.add_argument("--stop-timeout", type=float, default=DEFAULT_STOP_TIMEOUT,
                        help="seconds to wait for warm shutdown before SIGKILL")
    args = parser.parse_args(argv)

    fleet = WorkerFleet(
        _import_attr(args.config),
        app=args.app,
        state_dir=args.state_dir,
        stop_timeout=args.stop_timeout,
    )

    if args.status:
        for name, pid in fleet.status().items():
            print(f"{name:20s} {pid if pid else 'not running'}")
        return 0

    if args.stop:
        for name, outcome in fleet.stop().items():
            print(f"{name:20s} {outcome}")
        return 0

    for name, outcome in fleet.start(force=args.force).items():
        print(f"{name:20s} {outcome}")

    if args.no_supervise:
        return 0

    # Blocks until SIGTERM/SIGINT, then stops the workers it started. Without
    # this the script would exit and its children would be orphaned, and
    # "SIGTERM stops the workers" would have nothing to signal.
    print("supervising; SIGTERM or Ctrl-C to stop", flush=True)
    fleet.supervise()
    return 0


if __name__ == "__main__":
    sys.exit(main())
