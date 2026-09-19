from __future__ import annotations

import multiprocessing
import os
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from multiprocessing.connection import Connection
from multiprocessing.connection import wait as wait_connections
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from queue import Empty, Queue
from signal import Signals
from threading import Condition, Thread
from time import monotonic, sleep
from typing import Any, cast

import psutil  # type: ignore[import-untyped]

from procman._containment import (
    ContainmentError,
    WorkerContainment,
    contained_rss,
    terminate_containment,
)
from procman._memory import available_memory_bytes

JobArgs = list[Any]
JobCallback = Callable[[JobArgs], None]
JobKillHook = Callable[[JobArgs, str], None]
JobErrorHook = Callable[[JobArgs, str], None]

_WORKER_START_TIMEOUT = 10.0
_JOB_START_ACK_TIMEOUT = 10.0
_MANAGER_INTERVAL = 0.25
_DESCENDANT_ERROR = "Job left descendant processes running; they were terminated."
_ACCOUNTING_UNAVAILABLE = "accounting_unavailable"
_CANCELLED_BEFORE_DISPATCH = "Persistent job was cancelled before dispatch."
_CANCELLED_BY_SHUTDOWN = (
    "Persistent job was cancelled before dispatch because the pool shut down."
)
_MEBIBYTE = 1024 * 1024
_TERMINAL_JOB_STATES = frozenset({"succeeded", "failed", "cancelled"})


class JobSubmissionError(RuntimeError):
    """A persistent job could not be serialized or sent to its worker."""


class JobQueueFull(JobSubmissionError):
    """A nonblocking submission could not fit in the pending-job queue."""


class JobDiagnostic(str):
    """A human-readable job error with stable machine-readable details."""

    code: str
    details: dict[str, Any]

    def __new__(  # noqa: PYI034 -- typing.Self is unavailable on Python 3.10
        cls,
        message: str,
        *,
        code: str,
        details: Mapping[str, Any] | None = None,
    ) -> JobDiagnostic:
        diagnostic = str.__new__(cls, message)
        diagnostic.code = code
        diagnostic.details = dict(details or {})
        return diagnostic

    @property
    def diagnostic(self) -> dict[str, Any]:
        return {"code": self.code, **self.details}


class JobHandle:
    """Handle for an accepted nonblocking persistent-pool submission."""

    def __init__(self, pool: PersistentProcPool, job_id: int) -> None:
        self._pool = pool
        self._job_id = job_id
        self._state = "pending"
        self._started = False
        self._error: str | None = None
        self._kill_reason: str | None = None
        self._submission_error: BaseException | None = None

    @property
    def job_id(self) -> int:
        return self._job_id

    def cancel(self) -> bool:
        """Cancel this job if it has not been dispatched yet."""

        return self._pool._cancel_pending_job(self._job_id)

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for a terminal outcome and return whether one was observed."""

        deadline = None if timeout is None else monotonic() + max(0.0, timeout)
        with self._pool._worker_condition:
            while self._state not in _TERMINAL_JOB_STATES:
                if deadline is None:
                    self._pool._worker_condition.wait()
                    continue
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return False
                self._pool._worker_condition.wait(remaining)
            return True

    def done(self) -> bool:
        with self._pool._worker_condition:
            return self._state in _TERMINAL_JOB_STATES

    def started(self) -> bool:
        with self._pool._worker_condition:
            return self._started

    def cancelled(self) -> bool:
        with self._pool._worker_condition:
            return self._state == "cancelled"

    def successful(self) -> bool:
        with self._pool._worker_condition:
            return self._state == "succeeded"

    @property
    def error(self) -> str | None:
        with self._pool._worker_condition:
            return self._error

    @property
    def kill_reason(self) -> str | None:
        with self._pool._worker_condition:
            return self._kill_reason


def _resolve_mp_context(mp_context: BaseContext | str | None) -> BaseContext:
    if mp_context is None:
        methods = multiprocessing.get_all_start_methods()
        method = "forkserver" if "forkserver" in methods else "spawn"
        return multiprocessing.get_context(method)
    if isinstance(mp_context, str):
        return multiprocessing.get_context(mp_context)
    if isinstance(mp_context, BaseContext):
        return mp_context
    raise TypeError(
        "mp_context must be a multiprocessing context, start-method name, or None"
    )


def _normalize_args(args: Iterable[Any]) -> JobArgs:
    if isinstance(args, list):
        return list(args)
    if isinstance(args, tuple):
        return list(args)
    return list(args)


def _worker_exit_error(exitcode: int | None, *, shutting_down: bool) -> str:
    if exitcode is None:
        status = "without an exit code"
    elif exitcode < 0 and os.name == "posix":
        signal_number = -exitcode
        try:
            signal_name = Signals(signal_number).name
        except ValueError:
            status = f"after signal {signal_number}"
        else:
            status = f"after signal {signal_number} ({signal_name})"
    else:
        status = f"with exit code {exitcode}"
    if shutting_down:
        return f"Worker process exited during pool shutdown {status}"
    return f"Worker process exited unexpectedly {status}"


def _safe_invoke_callback(
    callback: JobCallback | None, args: JobArgs, label: str
) -> None:
    if callback is None:
        return
    try:
        callback(args)
    except Exception:
        print(f"{label} callback failed")
        import traceback

        print(traceback.format_exc())


def _safe_invoke_kill_hook(
    hook: JobKillHook | None, args: JobArgs, reason: str, label: str
) -> None:
    if hook is None:
        return
    try:
        hook(args, reason)
    except Exception:
        print(f"{label} kill hook failed")
        import traceback

        print(traceback.format_exc())


def _safe_invoke_error_hook(
    hook: JobErrorHook | None, args: JobArgs, error: str, label: str
) -> None:
    if hook is None:
        return
    try:
        hook(args, error)
    except Exception:
        print(f"{label} error hook failed")
        import traceback

        print(traceback.format_exc())


def _run_proc_job(target: Callable, args: JobArgs, startup) -> None:
    try:
        containment = WorkerContainment()
        backend = containment.enter_job()
    except BaseException as error:
        try:
            startup.send(("error", f"{type(error).__name__}: {error}"))
        finally:
            startup.close()
        raise

    startup.send(("ready", backend))
    startup.close()
    try:
        target(*args)
    finally:
        containment.finish_job()


class ProcPool:
    """Custom process pool with per-job time and memory limits."""

    TIME = "time"
    MEM = "memory"

    class Proc:
        def __init__(
            self,
            target: Callable,
            args: Iterable[Any],
            limit_mem: int = 0,
            limit_time: int = 0,
            uid: int = -1,
            callback: JobCallback | None = None,
            mp_context: BaseContext | None = None,
        ):
            self._limit_mem = limit_mem
            self._limit_time = limit_time
            self._pid = -1
            self._pool_id = uid
            self._start_time = -1
            self._started_at = -1.0
            self._args = (target, _normalize_args(args))
            self._cb = callback
            self._kill_reason: str | None = None
            self._process: Any | None = None
            self._backend: str | None = None
            self._mp_context = mp_context or _resolve_mp_context(None)

        def get_psproc(self) -> psutil.Process | None:
            if not psutil.pid_exists(self._pid):
                return None
            try:
                psproc = psutil.Process(self._pid)
                if self._start_time == psproc.create_time():
                    return psproc
            except psutil.NoSuchProcess:
                return None
            return None

        def get_args(self) -> JobArgs:
            return self._args[1]

        def reason(self) -> str | None:
            return self._kill_reason

        def start(self) -> None:
            target, job_args = self._args
            startup, child_startup = self._mp_context.Pipe(duplex=False)
            proc = self._mp_context.Process(  # type: ignore[attr-defined]
                target=_run_proc_job,
                args=(target, job_args, child_startup),
                daemon=False,
            )
            try:
                proc.start()
                child_startup.close()
                self._process = proc
                if proc.pid is None:
                    raise RuntimeError("worker started without a process ID")
                self._pid = proc.pid
                self._start_time = psutil.Process(self._pid).create_time()
                if not startup.poll(_WORKER_START_TIMEOUT):
                    raise RuntimeError("worker did not establish process containment")
                status, detail = startup.recv()
                if status != "ready":
                    raise RuntimeError(f"worker containment setup failed: {detail}")
                self._backend = str(detail)
                self._started_at = monotonic()
            except BaseException:
                if proc.pid is not None:
                    terminate_containment(proc.pid, self._backend)
                    proc.join(timeout=1)
                raise
            finally:
                startup.close()
                child_startup.close()

        def get_time_limit(self) -> int:
            return int(self._limit_time)

        @property
        def pid(self) -> int:
            return self._pid

        def get_mem_limit(self) -> int:
            return int(self._limit_mem)

        def is_time_exceeded(self) -> bool:
            psproc = self.get_psproc()
            if psproc and psproc.is_running() and self._limit_time != 0:
                return monotonic() - self._started_at > self._limit_time
            return False

        def is_mem_exceeded(self) -> bool:
            psproc = self.get_psproc()
            if psproc and self._pid > 0 and self._limit_mem != 0:
                try:
                    mem_mb = contained_rss(self._pid, self._backend or "") / (
                        1024 * 1024
                    )
                except ContainmentError as error:
                    print(f"Process {self._pid} containment accounting failed: {error}")
                    return True
                return self._limit_mem < mem_mb
            return False

        def is_alive(self) -> bool:
            return self._process is not None and self._process.is_alive()

        def get_pool_pid(self) -> int:
            return self._pool_id

        def kill(self, reason: str) -> None:
            self._kill_reason = reason
            self.cleanup()

        def cleanup(self) -> None:
            if self._pid > 0:
                remaining = terminate_containment(self._pid, self._backend)
                if remaining:
                    print(
                        f"Process {self._pid} containment still has live processes: "
                        f"{remaining}"
                    )
            if self._process is not None:
                self._process.join(timeout=1)

        def status(self) -> str:
            psproc = self.get_psproc()
            if psproc:
                return psproc.status()
            return "dead"

        def get_callback(self) -> JobCallback | None:
            return self._cb

    def __init__(
        self,
        processes: int = 1,
        on_job_killed: JobKillHook | None = None,
        mp_context: BaseContext | str | None = None,
    ):
        if processes < 1:
            raise ValueError("Invalid number of processes")
        self._ids = set(range(processes))
        self._proc_limit = processes
        self._queue: Queue[ProcPool.Proc] = Queue(processes)
        self._procs: dict[int, ProcPool.Proc] = {}
        self._on_job_killed = on_job_killed
        self._mp_context = _resolve_mp_context(mp_context)
        self.running = True
        self._mg_thrd = Thread(target=self._thrd_mgr, daemon=True)

    @property
    def start_method(self) -> str:
        return self._mp_context.get_start_method()

    def __enter__(self):
        self._mg_thrd.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        while len(self._procs) > 0:
            sleep(1)
        self.running = False
        self._mg_thrd.join()

    def shutdown(self, force: bool = False) -> None:
        if force:
            for proc in list(self._procs.values()):
                if isinstance(proc, ProcPool.Proc):
                    proc.kill(ProcPool.TIME)
        self.running = False
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except Empty:
                break

    def _thrd_mgr(self):
        while self.running or len(self._procs) > 0:
            sleep(_MANAGER_INTERVAL)
            remove_pids = []
            for pid, proc in self._procs.items():
                if not proc.is_alive() or proc.status() in (
                    psutil.STATUS_ZOMBIE,
                    psutil.STATUS_DEAD,
                ):
                    remove_pids.append(pid)
                    continue
                if proc.is_time_exceeded():
                    print(
                        f"Process {pid} exceeded the time limit of {proc.get_time_limit()} seconds"
                    )
                    proc.kill(ProcPool.TIME)
                    continue
                if proc.is_mem_exceeded():
                    print(
                        f"Process {pid} exceeded the memory limit of {proc.get_mem_limit()}MB"
                    )
                    proc.kill(ProcPool.MEM)
                    continue
            for pid in remove_pids:
                proc = self._procs.pop(pid)
                proc.cleanup()
                if proc.reason():
                    _safe_invoke_kill_hook(
                        self._on_job_killed, proc.get_args(), proc.reason(), "ProcPool"
                    )
                _safe_invoke_callback(proc.get_callback(), proc.get_args(), "ProcPool")
                self._ids.add(proc.get_pool_pid())
            while (not self._queue.empty()) and (len(self._procs) < self._proc_limit):
                try:
                    proc = self._queue.get(timeout=1)
                    proc._pool_id = self._ids.pop()
                    try:
                        proc.start()
                    except Exception:
                        self._ids.add(proc.get_pool_pid())
                        print("ProcPool worker failed to start")
                        import traceback

                        print(traceback.format_exc())
                        _safe_invoke_callback(
                            proc.get_callback(), proc.get_args(), "ProcPool"
                        )
                    else:
                        self._procs[proc.pid] = proc
                except Empty:
                    break

    def apply(
        self,
        target: Callable,
        args: Iterable[Any],
        limit_mem: int = 0,
        limit_time: int = 0,
        callback: JobCallback | None = None,
    ):
        proc = ProcPool.Proc(
            target=target,
            args=args,
            limit_mem=limit_mem,
            limit_time=limit_time,
            callback=callback,
            mp_context=self._mp_context,
        )
        self._queue.put(proc)

    def map(
        self,
        func: Callable,
        iterables: Iterable[Iterable[Any]],
        limit_mem: int = 0,
        limit_time: int = 0,
        callback: JobCallback | None = None,
    ):
        for item in iterables:
            self.apply(
                target=func,
                args=item,
                limit_mem=limit_mem,
                limit_time=limit_time,
                callback=callback,
            )


def _worker_loop(
    worker_id: int,
    job_queue: Any,
    completion_sender: Any,
    finishing_event: Any,
    max_tasks: int,
    startup,
) -> None:
    try:
        containment = WorkerContainment()
    except BaseException as error:
        try:
            startup.send(("error", f"{type(error).__name__}: {error}"))
        finally:
            startup.close()
        raise

    startup.send(("ready", containment.backend))
    startup.close()
    tasks = 0
    while True:
        job = job_queue.get()
        if job is None:
            break
        job_id, serialized_job, retire_worker = job
        target, args = ForkingPickler.loads(serialized_job)
        finishing_event.clear()
        try:
            backend = containment.enter_job()
        except BaseException as error:
            completion_sender.send(
                (
                    "containment_error",
                    worker_id,
                    job_id,
                    f"{type(error).__name__}: {error}",
                )
            )
            raise
        completion_sender.send(
            (
                "start",
                worker_id,
                job_id,
                {"started": monotonic(), "backend": backend},
            )
        )
        exc = None
        fatal: BaseException | None = None
        try:
            target(*args)
        except Exception as err:  # noqa: BLE001
            exc = f"{type(err).__name__}: {err}"
        except BaseException as error:
            fatal = error
        descendants = False
        cleanup_error = None
        # Publish this synchronously before leaving the containment unit. The
        # completion connection cannot by itself close the accounting race
        # around finish_job().
        finishing_event.set()
        try:
            descendants = containment.finish_job()
        except BaseException as error:
            cleanup_error = f"{type(error).__name__}: {error}"
        if fatal is not None:
            raise fatal
        tasks += 1
        retire = bool(retire_worker or (max_tasks and tasks >= max_tasks))
        restart = (
            cleanup_error is not None
            or (descendants and containment.restart_after_descendants)
            or retire
        )
        completion_sender.send(
            (
                "done",
                worker_id,
                job_id,
                {
                    "error": exc,
                    "descendants": descendants,
                    "cleanup_error": cleanup_error,
                    "restart": restart,
                },
            )
        )
        if restart:
            # The manager replaces this worker before advertising its slot as
            # idle. Stay alive until then so process exit cannot be treated as
            # abnormal before the completion message is processed.
            job_queue.get()
            break
    completion_sender.close()


class PersistentProcPool:
    def __init__(
        self,
        processes: int = 1,
        max_tasks_per_worker: int = 0,
        on_job_killed: JobKillHook | None = None,
        on_job_error: JobErrorHook | None = None,
        start_ack_timeout: float = _JOB_START_ACK_TIMEOUT,
        mp_context: BaseContext | str | None = None,
        max_pending_jobs: int | None = None,
    ):
        if processes < 1:
            raise ValueError("Invalid number of processes")
        if start_ack_timeout <= 0:
            raise ValueError("Invalid start acknowledgement timeout")
        if max_pending_jobs is not None and max_pending_jobs < 1:
            raise ValueError("Invalid pending-job limit")
        self._proc_limit = processes
        self._max_tasks = max_tasks_per_worker
        self._max_pending_jobs = max_pending_jobs or max(1, processes * 4)
        self._max_admission_bypasses = max(1, processes)
        self._mp_context = _resolve_mp_context(mp_context)
        self._job_queues: dict[int, Any] = {}
        # Each worker exclusively owns one completion sender. Killing a worker
        # can corrupt only that worker's connection instead of retaining a
        # shared queue lock or blocking reports from surviving workers.
        self._completion_receivers: dict[int, Connection] = {}
        self._workers: dict[int, Any] = {}
        self._finishing_events: dict[int, Any] = {}
        self._running_jobs: dict[int, dict[str, Any]] = {}
        self._worker_jobs: dict[int, int | None] = {}
        self._pending_jobs: dict[int, dict[str, Any]] = {}
        self._capacity_waiters: deque[object] = deque()
        self._admission_waiters: deque[int] = deque()
        self._admission_ceiling: int | None = None
        self._admission_capacity: int | None = None
        self._job_id = 0
        self._on_job_killed = on_job_killed
        self._on_job_error = on_job_error
        self._start_ack_timeout = float(start_ack_timeout)
        self.running = True
        self._accepting = False
        self._worker_condition = Condition()
        self._wakeup_receiver, self._wakeup_sender = self._mp_context.Pipe(duplex=False)
        self._wakeup_pending = False
        self._outcome_queue: Queue[Any] = Queue()
        self._outcome_thread = Thread(target=self._deliver_outcomes, daemon=True)
        self._mg_thrd = Thread(target=self._thrd_mgr, daemon=True)

    @property
    def start_method(self) -> str:
        return self._mp_context.get_start_method()

    @property
    def max_pending_jobs(self) -> int:
        return self._max_pending_jobs

    def __enter__(self):
        try:
            for worker_id in range(self._proc_limit):
                self._spawn_worker(worker_id)
        except BaseException:
            self.running = False
            for worker_id in list(self._workers):
                self._terminate_worker(worker_id)
            self._close_completion_channels()
            self._close_wakeup_channel()
            raise
        self._admission_ceiling = available_memory_bytes()
        self._admission_capacity = self._admission_ceiling
        self._outcome_thread.start()
        self._mg_thrd.start()
        self._accepting = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(force=True)
        self._mg_thrd.join()
        self._outcome_thread.join()
        self._close_completion_channels()
        self._close_wakeup_channel()

    def _wake_manager_locked(self) -> None:
        if self._wakeup_pending or self._wakeup_sender.closed:
            return
        self._wakeup_pending = True
        try:
            self._wakeup_sender.send_bytes(b"\0")
        except (BrokenPipeError, EOFError, OSError):
            self._wakeup_pending = False

    def _deliver_outcomes(self) -> None:
        while True:
            outcome = self._outcome_queue.get()
            if outcome is None:
                return
            job, kill_reason, error, report_error = outcome
            args = job["args"]
            callback = job.get("callback")
            if kill_reason is not None:
                _safe_invoke_kill_hook(
                    self._on_job_killed,
                    args,
                    kill_reason,
                    "PersistentProcPool",
                )
            if report_error and error is not None:
                _safe_invoke_error_hook(
                    self._on_job_error,
                    args,
                    error,
                    "PersistentProcPool",
                )
            _safe_invoke_callback(
                callback,
                args,
                "PersistentProcPool",
            )

    def _queue_job_outcome(
        self,
        job: dict[str, Any],
        kill_reason: str | None,
        error: str | None,
        report_error: bool,
    ) -> None:
        self._outcome_queue.put((job, kill_reason, error, report_error))

    def _complete_job(
        self,
        job: dict[str, Any],
        *,
        error: str | None = None,
        kill_reason: str | None = None,
        cancelled: bool = False,
        report_error: bool = False,
        submission_error: BaseException | None = None,
    ) -> None:
        handle = cast("JobHandle", job["handle"])
        with self._worker_condition:
            if handle._state in _TERMINAL_JOB_STATES:
                return
            handle._state = (
                "cancelled"
                if cancelled
                else "failed"
                if error or kill_reason
                else "succeeded"
            )
            handle._error = error
            handle._kill_reason = kill_reason
            handle._submission_error = submission_error
            self._worker_condition.notify_all()

        self._queue_job_outcome(
            job,
            kill_reason,
            error,
            report_error,
        )

    def _cancel_pending_job(self, job_id: int) -> bool:
        with self._worker_condition:
            job = self._pending_jobs.pop(job_id, None)
            if job is None:
                return False
            with suppress(ValueError):
                self._admission_waiters.remove(job_id)
            self._complete_job(
                job,
                error=_CANCELLED_BEFORE_DISPATCH,
                cancelled=True,
                report_error=True,
            )
            self._wake_manager_locked()
            self._worker_condition.notify_all()
        return True

    def shutdown(self, force: bool = False) -> None:
        with self._worker_condition:
            self._accepting = False
            pending_jobs = list(self._pending_jobs.values())
            self._pending_jobs.clear()
            self._admission_waiters.clear()
            for job in pending_jobs:
                # Queue every accepted cancellation before allowing the
                # manager to publish the outcome-thread sentinel.
                self._complete_job(
                    job,
                    error=_CANCELLED_BY_SHUTDOWN,
                    cancelled=True,
                    report_error=True,
                )
            self.running = False
            self._wake_manager_locked()
            self._worker_condition.notify_all()
        for queue in list(self._job_queues.values()):
            try:
                queue.put(None)
            except Exception:
                pass
        if force:
            for worker_id in list(self._workers):
                job_id = self._worker_jobs.get(worker_id)
                running_job = (
                    self._running_jobs.get(job_id) if job_id is not None else None
                )
                backend = running_job.get("backend") if running_job else None
                self._terminate_worker(worker_id, backend=backend)

    def _discard_job_queue(self, worker_id: int) -> None:
        queue = self._job_queues.pop(worker_id, None)
        if queue is not None:
            try:
                queue.close()
            except Exception:
                pass

    def _discard_completion_receiver(
        self,
        worker_id: int,
        expected: Connection | None = None,
    ) -> None:
        receiver = None
        with self._worker_condition:
            current = self._completion_receivers.get(worker_id)
            if expected is None or current is expected:
                receiver = self._completion_receivers.pop(worker_id, None)
                self._worker_condition.notify_all()
            elif expected is not None:
                receiver = expected
        if receiver is not None:
            with suppress(OSError):
                receiver.close()

    def _close_completion_channels(self) -> None:
        with self._worker_condition:
            receivers = list(self._completion_receivers.values())
            self._completion_receivers.clear()
            self._worker_condition.notify_all()
        for receiver in receivers:
            with suppress(OSError):
                receiver.close()

    def _close_wakeup_channel(self) -> None:
        with suppress(OSError):
            self._wakeup_receiver.close()
        with suppress(OSError):
            self._wakeup_sender.close()

    def _receive_completion(self, timeout: float) -> Any | None:
        deadline = monotonic() + max(0.0, timeout)
        while True:
            with self._worker_condition:
                receivers = list(self._completion_receivers.items())
            remaining = max(0.0, deadline - monotonic())
            try:
                ready = wait_connections(
                    [
                        self._wakeup_receiver,
                        *(receiver for _worker_id, receiver in receivers),
                    ],
                    timeout=remaining,
                )
            except (OSError, ValueError):
                for worker_id, receiver in receivers:
                    if receiver.closed:
                        self._discard_completion_receiver(
                            worker_id,
                            expected=receiver,
                        )
                if monotonic() >= deadline:
                    return None
                continue
            if not ready:
                return None
            worker_by_receiver = {
                receiver: worker_id for worker_id, receiver in receivers
            }
            for ready_receiver in ready:
                receiver = cast("Connection", ready_receiver)
                if receiver is self._wakeup_receiver:
                    try:
                        receiver.recv_bytes()
                    except (EOFError, OSError):
                        return None
                    with self._worker_condition:
                        self._wakeup_pending = False
                    return ("wakeup", -1, -1, None)
                worker_id = worker_by_receiver[receiver]
                try:
                    return receiver.recv()
                except (EOFError, OSError):
                    self._discard_completion_receiver(
                        worker_id,
                        expected=receiver,
                    )
            if monotonic() >= deadline:
                return None

    def _spawn_worker(self, worker_id: int) -> None:
        job_queue = self._job_queues.get(worker_id)
        if job_queue is None:
            job_queue = self._mp_context.SimpleQueue()
            self._job_queues[worker_id] = job_queue
        finishing_event = self._mp_context.Event()
        self._finishing_events[worker_id] = finishing_event
        startup, child_startup = self._mp_context.Pipe(duplex=False)
        completion_receiver, child_completion_sender = self._mp_context.Pipe(
            duplex=False
        )
        proc = self._mp_context.Process(  # type: ignore[attr-defined]
            target=_worker_loop,
            args=(
                worker_id,
                job_queue,
                child_completion_sender,
                finishing_event,
                self._max_tasks,
                child_startup,
            ),
            daemon=False,
        )
        try:
            proc.start()
            child_startup.close()
            child_completion_sender.close()
            if not startup.poll(_WORKER_START_TIMEOUT):
                raise RuntimeError("persistent worker did not establish containment")
            status, detail = startup.recv()
            if status != "ready":
                raise RuntimeError(
                    f"persistent worker containment setup failed: {detail}"
                )
        except BaseException:
            if proc.pid is not None:
                terminate_containment(proc.pid, None)
                proc.join(timeout=1)
            completion_receiver.close()
            raise
        finally:
            startup.close()
            child_startup.close()
            child_completion_sender.close()
        with self._worker_condition:
            previous = self._completion_receivers.get(worker_id)
            self._completion_receivers[worker_id] = completion_receiver
            self._worker_condition.notify_all()
        if previous is not None:
            with suppress(OSError):
                previous.close()
        self._workers[worker_id] = proc
        self._set_worker_idle(worker_id)

    def _set_worker_idle(self, worker_id: int) -> None:
        with self._worker_condition:
            self._worker_jobs[worker_id] = None
            self._worker_condition.notify_all()

    def _pop_running_job(self, job_id: int) -> dict[str, Any] | None:
        with self._worker_condition:
            job = self._running_jobs.pop(job_id, None)
            self._worker_condition.notify_all()
            return job

    def _reserved_memory_bytes(self) -> int:
        return sum(
            max(0, int(job["reserve_mem"])) * _MEBIBYTE
            for job in self._running_jobs.values()
        )

    def _active_reserved_rss_bytes(self) -> int:
        active_rss = 0
        for job in self._running_jobs.values():
            reservation = max(0, int(job["reserve_mem"])) * _MEBIBYTE
            rss = max(0, int(job.get("rss", 0)))
            active_rss += min(reservation, rss)
        return active_rss

    def _admission_capacity_bytes(self) -> int | None:
        available = available_memory_bytes()
        if available is None:
            return self._admission_capacity

        if not self._running_jobs:
            # Rebase between waves so headroom released by unrelated workloads
            # can be used without weakening reservations for an active wave.
            self._admission_ceiling = available
            self._admission_capacity = available
            return available

        dynamic_capacity = available + self._active_reserved_rss_bytes()
        if self._admission_ceiling is None:
            self._admission_ceiling = dynamic_capacity
        self._admission_capacity = min(self._admission_ceiling, dynamic_capacity)
        return self._admission_capacity

    def _memory_admission_allows(self, limit_mem: int) -> bool:
        requested = max(0, int(limit_mem)) * _MEBIBYTE
        if requested == 0:
            return True
        reserved = self._reserved_memory_bytes()
        # The initial/idle snapshot is sufficient for the common light-job
        # path. Refresh only between waves or when a reservation would block.
        if (
            self._running_jobs
            and self._admission_capacity is not None
            and reserved + requested <= self._admission_capacity
        ):
            return True
        capacity = self._admission_capacity_bytes()
        if capacity is None:
            # Preserve worker-slot scheduling if both host and cgroup telemetry
            # are unavailable on a platform.
            return True
        if reserved + requested <= capacity:
            return True
        # A reservation is admission control, not a second per-job ceiling.
        # Let an oversized job make progress when it has the pool to itself;
        # its existing per-job limit remains authoritative while it runs.
        return not self._running_jobs

    def _next_admissible_job_id(self) -> int | None:
        waiting = list(self._admission_waiters)
        for index, job_id in enumerate(waiting):
            job = self._pending_jobs.get(job_id)
            if job is None:
                continue
            if self._memory_admission_allows(job["reserve_mem"]):
                for bypassed_id in waiting[:index]:
                    bypassed = self._pending_jobs.get(bypassed_id)
                    if bypassed is not None:
                        bypassed["bypasses"] += 1
                return job_id
            if job["bypasses"] >= self._max_admission_bypasses:
                break
        return None

    def _dispatch_pending_jobs(self) -> None:
        while True:
            submission_error: BaseException | None = None
            with self._worker_condition:
                if not self.running or not self._admission_waiters:
                    return
                worker_id = next(
                    (
                        candidate
                        for candidate, current in self._worker_jobs.items()
                        if current is None
                    ),
                    None,
                )
                if worker_id is None:
                    return
                job_id = self._next_admissible_job_id()
                if job_id is None:
                    return
                was_head = self._admission_waiters[0] == job_id
                job = self._pending_jobs.pop(job_id)
                self._admission_waiters.remove(job_id)
                if was_head:
                    # A blocked head made progress. Start a new bounded
                    # backfill wave for the next reservation instead of
                    # turning a heavy backlog into a convoy for light work.
                    for waiting_id in self._admission_waiters:
                        waiting_job = self._pending_jobs.get(waiting_id)
                        if waiting_job is not None:
                            waiting_job["bypasses"] = 0
                self._running_jobs[job_id] = job
                self._worker_jobs[worker_id] = job_id
                try:
                    self._job_queues[worker_id].put(
                        (job_id, job["serialized"], job["retire_worker"])
                    )
                except Exception as error:  # noqa: BLE001
                    submission_error = error
                    self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                else:
                    job.pop("serialized", None)
                    job["submitted"] = monotonic()
                    handle = cast("JobHandle", job["handle"])
                    handle._started = True
                    handle._state = "running"
                self._worker_condition.notify_all()

            if submission_error is None:
                continue
            if isinstance(submission_error, (EOFError, OSError, ValueError)):
                self._restart_worker(worker_id, replace_job_queue=True)
            else:
                self._set_worker_idle(worker_id)
            self._complete_job(
                job,
                error=f"Unable to submit persistent job {job_id}",
                report_error=True,
                submission_error=submission_error,
            )

    def _terminate_worker(
        self,
        worker_id: int,
        backend: str | None = None,
    ) -> str | None:
        proc = self._workers.get(worker_id)
        if proc is None or proc.pid is None:
            return None
        cleanup_error = None
        try:
            remaining = terminate_containment(proc.pid, backend)
            if remaining:
                print(
                    f"PersistentProcPool worker {worker_id} containment still has "
                    f"live processes: {remaining}"
                )
        except Exception as error:  # noqa: BLE001 -- cleanup must survive backend loss
            cleanup_error = f"{type(error).__name__}: {error}"
            print(
                f"PersistentProcPool worker {worker_id} containment termination "
                f"failed: {cleanup_error}"
            )
            with suppress(Exception):
                proc.kill()
        finally:
            proc.join(timeout=1)
        return cleanup_error

    def _restart_worker(
        self,
        worker_id: int,
        backend: str | None = None,
        replace_job_queue: bool = False,
    ) -> str | None:
        cleanup_error = self._terminate_worker(worker_id, backend=backend)
        self._discard_completion_receiver(worker_id)
        if replace_job_queue:
            self._discard_job_queue(worker_id)
        if self.running:
            self._spawn_worker(worker_id)
        else:
            self._workers.pop(worker_id, None)
            self._set_worker_idle(worker_id)
        return cleanup_error

    def _thrd_mgr(self) -> None:
        pending_message = None
        next_worker_check = monotonic()
        while self.running or self._running_jobs:
            now = monotonic()
            while True:
                if pending_message is not None:
                    msg = pending_message
                    pending_message = None
                else:
                    try:
                        msg = self._receive_completion(0.0)
                    except (OSError, ValueError):
                        msg = None
                    if msg is None:
                        break
                action, worker_id, job_id, payload = msg
                if action == "wakeup":
                    continue
                if action == "start":
                    job = self._running_jobs.get(job_id)
                    if job:
                        job["start"] = payload["started"]
                        job["backend"] = payload["backend"]
                elif action == "done":
                    job = self._pop_running_job(job_id)
                    if job is None:
                        continue
                    finishing_event = self._finishing_events.get(worker_id)
                    if finishing_event is not None:
                        finishing_event.clear()
                    if payload.get("restart"):
                        backend = job.get("backend")
                        self._restart_worker(
                            worker_id,
                            backend=backend,
                            replace_job_queue=True,
                        )
                    else:
                        self._set_worker_idle(worker_id)
                    errors = []
                    if payload.get("error"):
                        errors.append(str(payload["error"]))
                    if payload.get("descendants"):
                        errors.append(_DESCENDANT_ERROR)
                    if payload.get("cleanup_error"):
                        errors.append(
                            f"Process containment cleanup failed: "
                            f"{payload['cleanup_error']}"
                        )
                    error = " ".join(errors) if errors else None
                    if error:
                        print(
                            f"PersistentProcPool worker {worker_id} job "
                            f"{job_id} failed: {error}"
                        )
                    self._complete_job(
                        job,
                        error=error,
                        report_error=error is not None,
                    )
                elif action == "containment_error":
                    job = self._pop_running_job(job_id)
                    if job is None:
                        continue
                    self._restart_worker(worker_id)
                    self._complete_job(
                        job,
                        error=f"Process containment setup failed: {payload}",
                        report_error=True,
                    )
                elif action == "exit":
                    self._restart_worker(worker_id)
            self._dispatch_pending_jobs()
            now = monotonic()
            if now < next_worker_check:
                try:
                    pending_message = self._receive_completion(next_worker_check - now)
                except (OSError, ValueError):
                    pending_message = None
                continue
            for worker_id, job_id in list(self._worker_jobs.items()):
                proc = self._workers.get(worker_id)
                if proc is None:
                    continue
                if not proc.is_alive():
                    proc.join(timeout=0)
                    exitcode = proc.exitcode
                    job = None
                    backend = None
                    if job_id is not None:
                        job = self._pop_running_job(job_id)
                        backend = job.get("backend") if job else None
                    replace_job_queue = bool(job and job.get("start") is None)
                    self._restart_worker(
                        worker_id,
                        backend=backend,
                        replace_job_queue=replace_job_queue,
                    )
                    if job:
                        error = _worker_exit_error(
                            exitcode,
                            shutting_down=not self.running,
                        )
                        print(
                            f"PersistentProcPool worker {worker_id} job "
                            f"{job_id} failed: {error}"
                        )
                        self._complete_job(
                            job,
                            error=error,
                            report_error=True,
                        )
                    continue
                if job_id is None:
                    continue
                job = self._running_jobs.get(job_id)
                if not job:
                    continue
                finishing_event = self._finishing_events.get(worker_id)
                if finishing_event is not None and finishing_event.is_set():
                    continue
                if job.get("start") is None:
                    submitted = job.get("submitted")
                    if submitted is None or now - submitted <= self._start_ack_timeout:
                        continue
                    self._pop_running_job(job_id)
                    self._restart_worker(worker_id, replace_job_queue=True)
                    error = (
                        f"Worker did not acknowledge job {job_id} within "
                        f"{self._start_ack_timeout:g} seconds"
                    )
                    self._complete_job(
                        job,
                        error=error,
                        report_error=True,
                    )
                    continue
                if job["limit_time"] and now - job["start"] > job["limit_time"]:
                    print(
                        f"PersistentProcPool worker {worker_id} job {job_id} exceeded the time limit of "
                        f"{job['limit_time']} seconds"
                    )
                    self._pop_running_job(job_id)
                    self._restart_worker(worker_id, backend=job["backend"])
                    self._complete_job(
                        job,
                        error=(
                            f"Job exceeded the time limit of "
                            f"{job['limit_time']} seconds"
                        ),
                        kill_reason=ProcPool.TIME,
                    )
                    continue
                if job["limit_mem"] or job["reserve_mem"]:
                    if proc.pid is None:
                        continue
                    try:
                        rss = contained_rss(proc.pid, job["backend"])
                        job["rss"] = rss
                        mem_mb = rss / _MEBIBYTE
                    except ContainmentError as error:
                        if finishing_event is not None and finishing_event.is_set():
                            continue
                        print(
                            f"PersistentProcPool worker {worker_id} job {job_id} "
                            f"containment accounting failed: {error}"
                        )
                        job = self._pop_running_job(job_id)
                        if job is None:
                            continue
                        cleanup_error = self._restart_worker(
                            worker_id,
                            backend=job["backend"],
                        )
                        details: dict[str, Any] = {
                            "worker_id": worker_id,
                            "job_id": job_id,
                            "backend": job["backend"],
                        }
                        if cleanup_error is not None:
                            details["cleanup_error"] = cleanup_error
                        diagnostic = JobDiagnostic(
                            f"Process containment accounting unavailable: {error}",
                            code=_ACCOUNTING_UNAVAILABLE,
                            details=details,
                        )
                        self._complete_job(
                            job,
                            error=diagnostic,
                            kill_reason=(ProcPool.MEM if job["limit_mem"] else None),
                            report_error=True,
                        )
                        continue
                    if job["limit_mem"] and mem_mb > job["limit_mem"]:
                        print(
                            f"PersistentProcPool worker {worker_id} job {job_id} exceeded the memory limit of "
                            f"{job['limit_mem']}MB"
                        )
                        self._pop_running_job(job_id)
                        self._restart_worker(worker_id, backend=job["backend"])
                        self._complete_job(
                            job,
                            error=f"Job exceeded the memory limit of {job['limit_mem']}MB",
                            kill_reason=ProcPool.MEM,
                        )
                        continue
            with self._worker_condition:
                self._admission_capacity_bytes()
                self._worker_condition.notify_all()
            self._dispatch_pending_jobs()
            next_worker_check = monotonic() + _MANAGER_INTERVAL
            try:
                pending_message = self._receive_completion(
                    max(0.0, next_worker_check - monotonic())
                )
            except (OSError, ValueError):
                pending_message = None
        self._close_completion_channels()
        self._close_wakeup_channel()
        self._outcome_queue.put(None)

    def _enqueue_persistent_job(
        self,
        target: Callable,
        args: Iterable[Any],
        *,
        limit_mem: int,
        limit_time: int,
        callback: JobCallback | None,
        reserve_mem: int | None,
        retire_worker: bool,
        block_for_capacity: bool,
    ) -> JobHandle:
        with self._worker_condition:
            if not self._accepting:
                raise RuntimeError("PersistentProcPool is not accepting jobs")
        norm_args = _normalize_args(args)
        if reserve_mem is None:
            reserve_mem = limit_mem
        if reserve_mem < 0:
            raise ValueError("Invalid memory reservation")
        try:
            serialized = bytes(ForkingPickler.dumps((target, norm_args)))
        except Exception as error:
            raise JobSubmissionError("Unable to serialize persistent job") from error

        with self._worker_condition:
            capacity_token = None
            if block_for_capacity:
                capacity_token = object()
                self._capacity_waiters.append(capacity_token)
                try:
                    while self._accepting and (
                        self._capacity_waiters[0] is not capacity_token
                        or len(self._pending_jobs) >= self._max_pending_jobs
                    ):
                        self._worker_condition.wait()
                    if self._accepting:
                        self._capacity_waiters.popleft()
                        capacity_token = None
                        self._worker_condition.notify_all()
                finally:
                    if capacity_token is not None:
                        with suppress(ValueError):
                            self._capacity_waiters.remove(capacity_token)
                        self._worker_condition.notify_all()
            if not self._accepting:
                raise RuntimeError("PersistentProcPool is not accepting jobs")
            if not block_for_capacity and (
                self._capacity_waiters
                or len(self._pending_jobs) >= self._max_pending_jobs
            ):
                raise JobQueueFull(
                    f"PersistentProcPool pending-job limit of "
                    f"{self._max_pending_jobs} was reached"
                )
            self._job_id += 1
            job_id = self._job_id
            handle = JobHandle(self, job_id)
            job: dict[str, Any] = {
                "args": norm_args,
                "callback": callback,
                "limit_time": limit_time,
                "limit_mem": limit_mem,
                "reserve_mem": reserve_mem,
                "retire_worker": bool(retire_worker),
                "serialized": serialized,
                "start": None,
                "submitted": None,
                "rss": 0,
                "bypasses": 0,
                "handle": handle,
            }
            self._pending_jobs[job_id] = job
            self._admission_waiters.append(job_id)
            self._wake_manager_locked()
            self._worker_condition.notify_all()
            return handle

    def submit(
        self,
        target: Callable,
        args: Iterable[Any],
        limit_mem: int = 0,
        limit_time: int = 0,
        callback: JobCallback | None = None,
        reserve_mem: int | None = None,
        retire_worker: bool = False,
    ) -> JobHandle:
        """Accept a bounded submission without waiting for worker admission."""

        return self._enqueue_persistent_job(
            target,
            args,
            limit_mem=limit_mem,
            limit_time=limit_time,
            callback=callback,
            reserve_mem=reserve_mem,
            retire_worker=retire_worker,
            block_for_capacity=False,
        )

    def apply(
        self,
        target: Callable,
        args: Iterable[Any],
        limit_mem: int = 0,
        limit_time: int = 0,
        callback: JobCallback | None = None,
        reserve_mem: int | None = None,
        retire_worker: bool = False,
    ) -> None:
        handle = self._enqueue_persistent_job(
            target,
            args,
            limit_mem=limit_mem,
            limit_time=limit_time,
            callback=callback,
            reserve_mem=reserve_mem,
            retire_worker=retire_worker,
            block_for_capacity=True,
        )
        with self._worker_condition:
            while not handle._started and handle._state not in _TERMINAL_JOB_STATES:
                self._worker_condition.wait()
            if handle._started:
                return
            if handle._submission_error is not None:
                raise JobSubmissionError(
                    f"Unable to submit persistent job {handle.job_id}"
                ) from handle._submission_error
            raise RuntimeError(
                handle._error or "PersistentProcPool is not accepting jobs"
            )
