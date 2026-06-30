from __future__ import annotations

from threading import Condition
from time import monotonic
from typing import Any


class JobTracker:
    def __init__(self) -> None:
        self._condition = Condition()
        self._submitted = 0
        self._finished = 0

    def submitted(self) -> None:
        with self._condition:
            self._submitted += 1

    def cancelled(self) -> None:
        with self._condition:
            self._submitted -= 1
            self._condition.notify_all()

    def done(self, _args: Any = None) -> None:
        with self._condition:
            self._finished += 1
            self._condition.notify_all()

    def wait(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else monotonic() + timeout
        with self._condition:
            while self._finished < self._submitted:
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def pending(self) -> int:
        with self._condition:
            return self._submitted - self._finished
