from multiprocessing import Manager
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event
from time import sleep, time

import pytest

from procman import JobTracker, PersistentProcPool, ProcPool, make_job_error_hook, make_job_killed_hook


_DELAYED_MARKER_CODE = (
    "from pathlib import Path; "
    "import sys, time; "
    "time.sleep(float(sys.argv[2])); "
    "Path(sys.argv[1]).write_text('descendant-survived', encoding='utf-8')"
)
_MEMORY_CHILD_CODE = (
    "from pathlib import Path; "
    "import sys, time; "
    "payload = bytearray(int(sys.argv[2]) * 1024 * 1024); "
    "Path(sys.argv[1]).write_text(str(len(payload)), encoding='utf-8'); "
    "time.sleep(30)"
)
_GRANDCHILD_LAUNCHER_CODE = (
    "import subprocess, sys; "
    "subprocess.Popen("
    "[sys.executable, '-c', sys.argv[2], sys.argv[1], '2.5'], "
    "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)"
)
_DESCENDANT_ERROR_MESSAGE = (
    "Job left descendant processes running; they were terminated."
)


def _touch_file(path: str) -> None:
    Path(path).write_text("ok", encoding="utf-8")


def _raise_error() -> None:
    raise RuntimeError("boom")


def _mark_started_then_wait(path: str) -> None:
    Path(path).write_text("started", encoding="utf-8")
    sleep(30)


def _spawn_delayed_marker_then_wait(marker: str, pid_file: str) -> None:
    child = subprocess.Popen(
        [sys.executable, "-c", _DELAYED_MARKER_CODE, marker, "2.5"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    Path(pid_file).write_text(str(child.pid), encoding="utf-8")
    sleep(30)


def _spawn_delayed_marker_and_return(marker: str) -> None:
    subprocess.Popen(
        [sys.executable, "-c", _DELAYED_MARKER_CODE, marker, "1.5"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _spawn_memory_children_then_wait(ready_a: str, ready_b: str) -> None:
    for ready in (ready_a, ready_b):
        subprocess.Popen(
            [sys.executable, "-c", _MEMORY_CHILD_CODE, ready, "64"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    sleep(30)


def _spawn_reparented_grandchild_then_wait(marker: str) -> None:
    subprocess.Popen(
        [
            sys.executable,
            "-c",
            _GRANDCHILD_LAUNCHER_CODE,
            marker,
            _DELAYED_MARKER_CODE,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).wait(timeout=5)
    sleep(30)


def _wait_for(path: Path, timeout: float) -> bool:
    deadline = time() + timeout
    while time() < deadline:
        if path.exists():
            return True
        sleep(0.05)
    return path.exists()


def _make_pool(pool_type, killed: Event, reasons: list[str], errors=None):
    def on_killed(_args, reason: str) -> None:
        reasons.append(reason)
        killed.set()

    kwargs = {"on_job_killed": on_killed}
    if pool_type is PersistentProcPool:
        kwargs["on_job_error"] = lambda args, error: errors.append(error)
    return pool_type(1, **kwargs)


def test_proc_pool_callback_runs() -> None:
    callbacks: list[list[object]] = []
    with TemporaryDirectory() as tmpdir:
        out_path = Path(tmpdir).joinpath("out.txt")
        with ProcPool(1) as pool:
            pool.apply(_touch_file, [str(out_path)], callback=lambda args: callbacks.append(args))
        assert out_path.is_file()
    assert callbacks


@pytest.mark.parametrize(
    "pool_type",
    [ProcPool, PersistentProcPool],
    ids=["one-shot", "persistent"],
)
def test_time_limit_terminates_descendant_before_kill_hook(pool_type) -> None:
    killed = Event()
    reasons: list[str] = []
    errors: list[str] = []
    with TemporaryDirectory() as tmpdir:
        marker = Path(tmpdir).joinpath("descendant-marker.txt")
        pid_file = Path(tmpdir).joinpath("descendant-pid.txt")
        with _make_pool(pool_type, killed, reasons, errors) as pool:
            pool.apply(
                _spawn_delayed_marker_then_wait,
                [str(marker), str(pid_file)],
                limit_time=1,
            )
            assert _wait_for(pid_file, 5)
            assert killed.wait(8)
            assert not marker.exists()
        sleep(2)
        assert not marker.exists()
    assert reasons == [ProcPool.TIME]


@pytest.mark.parametrize(
    "pool_type",
    [ProcPool, PersistentProcPool],
    ids=["one-shot", "persistent"],
)
def test_completion_cleans_background_descendants_before_callback(pool_type) -> None:
    callback = Event()
    killed = Event()
    reasons: list[str] = []
    errors: list[str] = []
    with TemporaryDirectory() as tmpdir:
        marker = Path(tmpdir).joinpath("background-marker.txt")
        with _make_pool(pool_type, killed, reasons, errors) as pool:
            pool.apply(
                _spawn_delayed_marker_and_return,
                [str(marker)],
                callback=lambda _args: callback.set(),
            )
            assert callback.wait(5)
            assert not marker.exists()
        sleep(1.75)
        assert not marker.exists()
    assert reasons == []
    if pool_type is PersistentProcPool:
        assert errors == [_DESCENDANT_ERROR_MESSAGE]


@pytest.mark.parametrize(
    "pool_type",
    [ProcPool, PersistentProcPool],
    ids=["one-shot", "persistent"],
)
def test_memory_limit_accounts_for_descendant_rss(pool_type) -> None:
    killed = Event()
    reasons: list[str] = []
    errors: list[str] = []
    with TemporaryDirectory() as tmpdir:
        ready_a = Path(tmpdir).joinpath("memory-child-a.txt")
        ready_b = Path(tmpdir).joinpath("memory-child-b.txt")
        with _make_pool(pool_type, killed, reasons, errors) as pool:
            pool.apply(
                _spawn_memory_children_then_wait,
                [str(ready_a), str(ready_b)],
                limit_mem=110,
                limit_time=10,
            )
            assert _wait_for(ready_a, 5)
            assert _wait_for(ready_b, 5)
            assert killed.wait(8)
    assert reasons == [ProcPool.MEM]


@pytest.mark.parametrize(
    "pool_type",
    [ProcPool, PersistentProcPool],
    ids=["one-shot", "persistent"],
)
def test_time_limit_terminates_reparented_grandchild(pool_type) -> None:
    killed = Event()
    reasons: list[str] = []
    errors: list[str] = []
    with TemporaryDirectory() as tmpdir:
        marker = Path(tmpdir).joinpath("grandchild-marker.txt")
        with _make_pool(pool_type, killed, reasons, errors) as pool:
            pool.apply(
                _spawn_reparented_grandchild_then_wait,
                [str(marker)],
                limit_time=1,
            )
            assert killed.wait(8)
            assert not marker.exists()
        sleep(2)
        assert not marker.exists()
    assert reasons == [ProcPool.TIME]


def test_persistent_pool_error_hook_runs() -> None:
    errors: list[tuple[list[object], str]] = []
    callbacks: list[list[object]] = []
    with PersistentProcPool(
        1,
        on_job_error=lambda args, error: errors.append((args, error)),
    ) as pool:
        pool.apply(_raise_error, [], callback=lambda args: callbacks.append(args))
        deadline = time() + 5
        while time() < deadline and not errors:
            sleep(0.1)
    assert errors
    assert "boom" in errors[0][1]
    assert callbacks == [[]]


def test_persistent_shutdown_does_not_restart_active_worker() -> None:
    with TemporaryDirectory() as tmpdir:
        started = Path(tmpdir).joinpath("started.txt")
        with PersistentProcPool(1) as pool:
            pool.apply(_mark_started_then_wait, [str(started)])
            assert _wait_for(started, 5)

        assert not pool._mg_thrd.is_alive()
        assert pool._workers == {}


def _blocking_task(started, release) -> None:
    started.put(True)
    release.wait(5)


def _wait_for_queue_items(queue, count: int, timeout: float = 5.0) -> int:
    deadline = time() + timeout
    seen = 0
    while time() < deadline and seen < count:
        try:
            queue.get(timeout=0.1)
        except Exception:
            continue
        seen += 1
    return seen


def test_persistent_pool_starts_jobs_on_all_worker_slots() -> None:
    callbacks: list[list[object]] = []
    with Manager() as manager:
        started = manager.Queue()
        release = manager.Event()
        with PersistentProcPool(4) as pool:
            for _ in range(4):
                pool.apply(_blocking_task, [started, release], callback=lambda args: callbacks.append(args))
            assert _wait_for_queue_items(started, 4) == 4
            release.set()
            deadline = time() + 5
            while time() < deadline and len(callbacks) < 4:
                sleep(0.1)
    assert len(callbacks) == 4


def test_job_tracker_tracks_pending_and_done_jobs() -> None:
    tracker = JobTracker()
    tracker.submitted()
    tracker.submitted()
    assert tracker.pending() == 2
    assert tracker.wait(timeout=0.01) is False

    tracker.done([])
    assert tracker.pending() == 1
    tracker.cancelled()

    assert tracker.pending() == 0
    assert tracker.wait(timeout=0.01) is True


def test_logger_hook_helpers_write_procman_error_payloads() -> None:
    class Logger:
        def __init__(self) -> None:
            self.messages = []
            self.sent = False

        def write(self, payload) -> None:
            self.messages.append(payload)

        def send(self) -> None:
            self.sent = True

    class Malware:
        @staticmethod
        def sha256() -> str:
            return "a" * 64

    class Analyzer:
        @staticmethod
        def name() -> str:
            return "elf"

    logger = Logger()
    args = [Malware(), {"analyzer": "capa"}, logger, [Analyzer]]

    make_job_killed_hook("error_event")(args, ProcPool.TIME)
    make_job_error_hook("error_event")(args, "boom")

    assert logger.messages[0] == {
        "error_event": {
            "hash": "a" * 64,
            "analyzer": "capa",
            "analyzer_group": ["elf"],
            "error": "Process exceed the time limit.",
        }
    }
    assert logger.messages[1]["error_event"]["error"] == "Worker exception: boom"
    assert logger.sent is True
