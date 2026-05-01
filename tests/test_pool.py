from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep, time

from procman import PersistentProcPool, ProcPool


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
