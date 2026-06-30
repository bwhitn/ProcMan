from multiprocessing import Manager
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep, time

from procman import JobTracker, PersistentProcPool, ProcPool, make_job_error_hook, make_job_killed_hook


def _touch_file(path: str) -> None:
    Path(path).write_text("ok", encoding="utf-8")


def _raise_error() -> None:
    raise RuntimeError("boom")


def test_proc_pool_callback_runs() -> None:
    callbacks: list[list[object]] = []
    with TemporaryDirectory() as tmpdir:
        out_path = Path(tmpdir).joinpath("out.txt")
        with ProcPool(1) as pool:
            pool.apply(_touch_file, [str(out_path)], callback=lambda args: callbacks.append(args))
        assert out_path.is_file()
    assert callbacks


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
