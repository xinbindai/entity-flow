"""
Tests for entitymodel.celery_workers -- starting and stopping a Celery worker
fleet from a configuration dict.

No broker and no real Celery workers: what is under test is process
management, so the fleet is pointed at a stub command that sleeps. That also
keeps the tests honest about the one thing a real worker would hide -- whether
the manager tracks liveness itself rather than asking the broker.

    python test/test_celery_workers.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

# Same bootstrap as the other suites: run standalone, without the project
# having to be installed first.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entitymodel.celery_workers import WorkerFleet, WorkerSpec, load_specs  # noqa: E402

CONFIG = {
    "pipeline": {"queue": "pipeline", "concurrency": 4, "log_path": None},
    "archive": {"queue": "archive", "concurrency": 1},
}


def sleeper_fleet(tmp: Path, config=None, **kwargs) -> WorkerFleet:
    """A fleet whose 'workers' are `sleep`, so start/stop can be exercised."""
    fleet = WorkerFleet(config or CONFIG, app="unused", state_dir=tmp / "run", **kwargs)
    fleet.command = lambda spec: ["sleep", "60"]  # noqa: ARG005
    return fleet


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def expect_error(fn, fragment: str, what: str):
    try:
        fn()
    except ValueError as exc:
        assert fragment in str(exc), f"{what}: expected {fragment!r} in {exc}"
    else:
        raise AssertionError(f"{what}: expected a ValueError")


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
def test_specs_are_parsed_from_the_config_dict(tmp: Path) -> None:
    specs = load_specs(CONFIG)
    by_name = {s.name: s for s in specs}

    assert by_name["pipeline"].queue == "pipeline"
    assert by_name["pipeline"].concurrency == 4
    assert by_name["archive"].concurrency == 1, "concurrency should default to 1"
    assert by_name["archive"].queue == "archive", "queue should default to the worker name"
    assert by_name["pipeline"].loglevel == "INFO"


def test_bad_config_is_rejected_before_anything_starts(tmp: Path) -> None:
    """A typo in one entry must not leave half a fleet running."""
    expect_error(lambda: load_specs({"w": {"concurrency": 0}}),
                 "positive int", "zero concurrency")
    expect_error(lambda: load_specs({"w": {"concurrency": "four"}}),
                 "positive int", "non-integer concurrency")
    expect_error(lambda: load_specs({"w": {"queeu": "typo"}}),
                 "unknown key", "misspelled key")
    expect_error(lambda: load_specs({"w": ["not", "a", "dict"]}),
                 "expected a dict", "non-dict entry")


def test_the_command_carries_queue_concurrency_and_log_path(tmp: Path) -> None:
    fleet = WorkerFleet(
        {"pipeline": {"queue": "pq", "concurrency": 7, "log_path": "/tmp/p.log",
                      "loglevel": "DEBUG", "extra_args": ("--pool", "solo")}},
        app="myapp:celery", state_dir=tmp / "run",
    )
    cmd = fleet.command(fleet.specs[0])

    assert cmd[:3] == [sys.executable, "-m", "celery"], "should use this interpreter"
    for flag, value in [("-A", "myapp:celery"), ("-Q", "pq"), ("-c", "7"),
                        ("--loglevel", "DEBUG"), ("--logfile", "/tmp/p.log")]:
        assert flag in cmd and cmd[cmd.index(flag) + 1] == value, f"{flag} {value} missing: {cmd}"
    assert cmd[-2:] == ["--pool", "solo"], "extra_args should be appended"
    assert "-n" in cmd and cmd[cmd.index("-n") + 1].startswith("pipeline@")


# --------------------------------------------------------------------------
# Starting and stopping
# --------------------------------------------------------------------------
def test_start_launches_every_worker_and_writes_pid_files(tmp: Path) -> None:
    fleet = sleeper_fleet(tmp)
    try:
        outcome = fleet.start()
        assert outcome == {"pipeline": "started", "archive": "started"}, outcome

        for spec in fleet.specs:
            pid = fleet.running_pid(spec)
            assert pid is not None, f"{spec.name} has no live pid"
            assert fleet.pid_file(spec).exists()
        assert all(v is not None for v in fleet.status().values())
    finally:
        fleet.stop()


def test_a_second_start_skips_running_workers(tmp: Path) -> None:
    """Re-running the init script must be harmless -- restarting would drop
    whatever those workers have in flight."""
    fleet = sleeper_fleet(tmp)
    try:
        fleet.start()
        pids_before = fleet.status()

        again = sleeper_fleet(tmp)  # a fresh manager, as a second invocation would be
        outcome = again.start()

        assert outcome == {"pipeline": "skipped", "archive": "skipped"}, outcome
        assert again.status() == pids_before, "a skipped worker should not be replaced"
    finally:
        fleet.stop()


def test_force_restarts_running_workers(tmp: Path) -> None:
    fleet = sleeper_fleet(tmp)
    try:
        fleet.start()
        pids_before = fleet.status()

        again = sleeper_fleet(tmp)
        outcome = again.start(force=True)

        assert outcome == {"pipeline": "started", "archive": "started"}, outcome
        pids_after = again.status()
        assert all(pids_after[n] != pids_before[n] for n in pids_before), \
            f"pids unchanged after force: {pids_before} -> {pids_after}"
        for pid in pids_before.values():
            assert not _running(pid), f"old worker {pid} survived --force"
    finally:
        again.stop()
        fleet.stop()


def test_stop_terminates_everything(tmp: Path) -> None:
    fleet = sleeper_fleet(tmp)
    fleet.start()
    pids = list(fleet.status().values())

    outcome = fleet.stop()

    assert set(outcome.values()) == {"stopped"}, outcome
    for pid in pids:
        assert not _alive(pid), f"{pid} still running after stop()"
    for spec in fleet.specs:
        assert not fleet.pid_file(spec).exists(), "pid file left behind"


def test_stop_reports_workers_that_were_not_running(tmp: Path) -> None:
    fleet = sleeper_fleet(tmp)
    assert set(fleet.stop().values()) == {"not running"}


def test_a_stale_pid_file_does_not_wedge_the_fleet(tmp: Path) -> None:
    """A crashed worker or a rebooted machine leaves a pid file pointing at
    nothing; that must read as 'not running', not block a restart forever."""
    fleet = sleeper_fleet(tmp)
    fleet.state_dir.mkdir(parents=True, exist_ok=True)
    spec = fleet.specs[0]

    dead = subprocess.Popen(["sleep", "0"])
    dead.wait()
    fleet.pid_file(spec).write_text(str(dead.pid))

    assert fleet.running_pid(spec) is None, "a dead pid should not read as running"
    assert not fleet.pid_file(spec).exists(), "the stale pid file should be cleaned up"

    try:
        assert fleet.start()[spec.name] == "started"
    finally:
        fleet.stop()


def test_a_garbage_pid_file_is_discarded(tmp: Path) -> None:
    fleet = sleeper_fleet(tmp)
    fleet.state_dir.mkdir(parents=True, exist_ok=True)
    spec = fleet.specs[0]
    fleet.pid_file(spec).write_text("not-a-pid")

    assert fleet.running_pid(spec) is None
    assert not fleet.pid_file(spec).exists()


def test_log_directories_are_created(tmp: Path) -> None:
    log = tmp / "deep" / "nested" / "worker.log"
    fleet = sleeper_fleet(tmp, {"w": {"queue": "q", "log_path": str(log)}})
    try:
        fleet.start()
        assert log.parent.is_dir(), "the log directory should be created for the worker"
    finally:
        fleet.stop()


# --------------------------------------------------------------------------
# SIGTERM
# --------------------------------------------------------------------------
def test_sigterm_stops_the_workers_it_started(tmp: Path) -> None:
    """
    The headline requirement. Run a manager in a child process, signal it, and
    check the grandchildren are gone -- doing it in-process would only prove
    the handler was installed.
    """
    script = tmp / "manager.py"
    script.write_text(
        "import json, sys\n"
        f"sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r})\n"
        "from entitymodel.celery_workers import WorkerFleet\n"
        f"fleet = WorkerFleet({{'w': {{'queue': 'q'}}}}, app='x', state_dir={str(tmp / 'run')!r})\n"
        "fleet.command = lambda spec: ['sleep', '60']\n"
        "fleet.start()\n"
        f"open({str(tmp / 'pids.json')!r}, 'w').write(json.dumps(fleet.status()))\n"
        "fleet.supervise(poll_interval=0.05)\n"
    )
    manager = subprocess.Popen([sys.executable, str(script)])
    pid_file = tmp / "pids.json"
    assert wait_until(pid_file.exists, 15), "manager never reported its workers"
    time.sleep(0.3)

    import json
    worker_pid = json.loads(pid_file.read_text())["w"]
    assert _alive(worker_pid), "worker was not running before the signal"

    manager.send_signal(signal.SIGTERM)
    assert manager.wait(timeout=20) is not None
    assert wait_until(lambda: not _alive(worker_pid), 15), \
        f"worker {worker_pid} survived SIGTERM to the manager"
    assert not (tmp / "run" / "w.pid").exists(), "pid file left behind after shutdown"


def test_supervise_returns_when_all_workers_exit(tmp: Path) -> None:
    """A fleet whose workers all die should not block forever."""
    fleet = sleeper_fleet(tmp, {"w": {"queue": "q"}})
    fleet.command = lambda spec: ["sleep", "0.1"]  # noqa: ARG005
    fleet.start()
    started = time.monotonic()
    fleet.supervise(poll_interval=0.05)
    assert time.monotonic() - started < 10, "supervise did not return after the worker exited"


def test_workers_do_not_inherit_the_parents_stdout(tmp: Path) -> None:
    """
    A worker holding the parent's stdout open means anything reading this
    script's output blocks until the worker exits -- which, for a worker, is
    never. Its stream must go to the log file, or to /dev/null when there is
    no log file.
    """
    log = tmp / "w.log"
    fleet = sleeper_fleet(tmp, {"logged": {"queue": "q", "log_path": str(log)},
                                "quiet": {"queue": "q2"}})
    try:
        fleet.start()
        logged, quiet = fleet.specs[0], fleet.specs[1]

        logged_fd1 = os.readlink(f"/proc/{fleet.running_pid(logged)}/fd/1")
        assert logged_fd1 == str(log), f"expected the log file, got {logged_fd1}"

        quiet_fd1 = os.readlink(f"/proc/{fleet.running_pid(quiet)}/fd/1")
        assert "null" in quiet_fd1, f"expected /dev/null, got {quiet_fd1}"

        for spec in fleet.specs:
            fd2 = os.readlink(f"/proc/{fleet.running_pid(spec)}/fd/2")
            assert "pipe:" not in fd2, f"{spec.name} stderr still points at a pipe: {fd2}"
    finally:
        fleet.stop()


def test_start_returns_promptly_when_its_output_is_piped(tmp: Path) -> None:
    """The end-to-end form of the same thing: a caller reading the output must
    not be held open by the workers."""
    script = tmp / "starter.py"
    script.write_text(
        f"import sys; sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r})\n"
        "from entitymodel.celery_workers import WorkerFleet\n"
        f"f = WorkerFleet({{'w': {{'queue': 'q'}}}}, app='x', state_dir={str(tmp / 'run')!r})\n"
        "f.command = lambda spec: ['sleep', '60']\n"
        "print(f.start())\n"
    )
    started = time.monotonic()
    done = subprocess.run([sys.executable, str(script)], capture_output=True, timeout=20)
    elapsed = time.monotonic() - started

    fleet = sleeper_fleet(tmp, {"w": {"queue": "q"}})
    try:
        assert done.returncode == 0, done.stderr.decode()
        assert elapsed < 10, f"reading the output took {elapsed:.1f}s -- a worker held the pipe"
    finally:
        fleet.stop()


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _running(pid: int) -> bool:
    """
    Alive *and* not a zombie.

    Both fleets in the force test share this process, so a worker killed by
    the second one stays a zombie until the first one's Popen handle reaps it
    -- os.kill would still report it alive. That is an artefact of the test:
    in production the second invocation is a separate process and the kernel
    re-parents the orphan to init, which reaps it. Reading the state directly
    asks the question that actually matters, "is it still executing".
    """
    try:
        state = Path(f"/proc/{pid}/stat").read_text().split()[2]
    except (OSError, IndexError):
        return False
    return state != "Z"


# --------------------------------------------------------------------------
TESTS = [
    test_specs_are_parsed_from_the_config_dict,
    test_bad_config_is_rejected_before_anything_starts,
    test_the_command_carries_queue_concurrency_and_log_path,
    test_start_launches_every_worker_and_writes_pid_files,
    test_a_second_start_skips_running_workers,
    test_force_restarts_running_workers,
    test_stop_terminates_everything,
    test_stop_reports_workers_that_were_not_running,
    test_a_stale_pid_file_does_not_wedge_the_fleet,
    test_a_garbage_pid_file_is_discarded,
    test_log_directories_are_created,
    test_workers_do_not_inherit_the_parents_stdout,
    test_start_returns_promptly_when_its_output_is_piped,
    test_sigterm_stops_the_workers_it_started,
    test_supervise_returns_when_all_workers_exit,
]


def main() -> int:
    passed, failed = 0, []
    for test in TESTS:
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                test(Path(tmpdir))
                print(f"PASS  {test.__name__}")
                passed += 1
            except Exception:
                print(f"FAIL  {test.__name__}")
                traceback.print_exc()
                failed.append(test.__name__)

    print(f"\n{passed} passed, {len(failed)} failed")
    for name in failed:
        print(f"  failed: {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
