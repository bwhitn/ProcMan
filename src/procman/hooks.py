from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64


def find_hash(args: Iterable[Any]) -> str | None:
    for arg in args:
        if isinstance(arg, Mapping):
            for field in ("sha256", "hash"):
                value = arg.get(field)
                if _is_sha256(value):
                    return value
        sha256 = getattr(arg, "sha256", None)
        if callable(sha256):
            try:
                value = sha256()
            except Exception:
                continue
            if _is_sha256(value):
                return value
    return None


def _display_name(item: Any) -> str | None:
    name = getattr(item, "name", None)
    if callable(name):
        try:
            value = name()
        except Exception:
            value = None
        if isinstance(value, str):
            return value
    if isinstance(name, str):
        return name
    py_name = getattr(item, "__name__", None)
    if isinstance(py_name, str):
        return py_name
    return None


def find_analyzer_group(args: Iterable[Any]) -> list[str] | None:
    for arg in args:
        if not isinstance(arg, Sequence) or isinstance(arg, (bytes, bytearray, str)) or not arg:
            continue
        names = [_display_name(item) for item in arg]
        if all(name is not None for name in names):
            return [name for name in names if name is not None]
    return None


def find_analyzer_name(args: Iterable[Any], analyzer_group: list[str] | None = None) -> str | None:
    for arg in args:
        if isinstance(arg, Mapping):
            analyzer = arg.get("analyzer")
            if isinstance(analyzer, str):
                return analyzer
    if analyzer_group:
        return analyzer_group[0]
    return None


def iter_loggers(args: Iterable[Any]) -> Iterator[Any]:
    for arg in args:
        write = getattr(arg, "write", None)
        if callable(write):
            yield arg


def flush_loggers(args: Iterable[Any]) -> None:
    for arg in args:
        send = getattr(arg, "send", None)
        if not callable(send):
            continue
        try:
            send()
        except Exception:
            print("procman log flush failed")
            import traceback

            print(traceback.format_exc())


def _write_to_loggers(args: list[Any], payload: Mapping[Any, Any]) -> None:
    for logger in iter_loggers(args):
        logger.write(payload)
    flush_loggers(args)


def job_killed_payload(args: Iterable[Any], reason: str, error_event_key: Any = "error") -> dict[Any, dict[str, Any]]:
    args = list(args)
    analyzer_group = find_analyzer_group(args)
    return {
        error_event_key: {
            "hash": find_hash(args),
            "analyzer": find_analyzer_name(args, analyzer_group),
            "analyzer_group": analyzer_group,
            "error": f"Process exceed the {reason} limit.",
        }
    }


def job_error_payload(args: Iterable[Any], error: str, error_event_key: Any = "error") -> dict[Any, dict[str, Any]]:
    args = list(args)
    analyzer_group = find_analyzer_group(args)
    message = str(error or "Worker process raised an exception.")
    return {
        error_event_key: {
            "hash": find_hash(args),
            "analyzer": find_analyzer_name(args, analyzer_group),
            "analyzer_group": analyzer_group,
            "error": f"Worker exception: {message}",
        }
    }


def make_job_killed_hook(error_event_key: Any = "error") -> Callable[[list[Any], str], None]:
    def handle_job_killed(args: list[Any], reason: str) -> None:
        _write_to_loggers(args, job_killed_payload(args, reason, error_event_key))

    return handle_job_killed


def make_job_error_hook(error_event_key: Any = "error") -> Callable[[list[Any], str], None]:
    def handle_job_error(args: list[Any], error: str) -> None:
        _write_to_loggers(args, job_error_payload(args, error, error_event_key))

    return handle_job_error
