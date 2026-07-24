from __future__ import annotations

import os
from multiprocessing import Pipe, Process, Queue as MPQueue, SimpleQueue as MPSimpleQueue
from queue import Empty, Queue
from signal import Signals
from threading import Thread
from time import monotonic, sleep
from typing import Any, Callable, Iterable, Optional

import psutil  # type: ignore[import-untyped]

from procman._containment import (
    ContainmentError,
    WorkerContainment,
    contained_rss,
    terminate_containment,
)

JobArgs = list[Any]
JobCallback = Callable[[JobArgs], None]
JobKillHook = Callable[[JobArgs, str], None]
JobErrorHook = Callable[[JobArgs, str], None]

_WORKER_START_TIMEOUT = 10.0
_JOB_START_ACK_TIMEOUT = 10.0
_MANAGER_INTERVAL = 0.25
_DESCENDANT_ERROR = "Job left descendant processes running; they were terminated."


class JobSubmissionError(RuntimeError):
    """A persistent job could not be serialized or sent to its worker."""


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


def _safe_invoke_callback(callback: JobCallback | None, args: JobArgs, label: str) -> None:
    if callback is None:
        return
    try:
        callback(args)
    except Exception:
        print(f"{label} callback failed")
        import traceback

        print(traceback.format_exc())


def _safe_invoke_kill_hook(hook: JobKillHook | None, args: JobArgs, reason: str, label: str) -> None:
    if hook is None:
        return
    try:
        hook(args, reason)
    except Exception:
        print(f"{label} kill hook failed")
        import traceback

        print(traceback.format_exc())


def _safe_invoke_error_hook(hook: JobErrorHook | None, args: JobArgs, error: str, label: str) -> None:
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
            self._process: Process | None = None
            self._backend: str | None = None

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
            startup, child_startup = Pipe(duplex=False)
            proc = Process(
                target=_run_proc_job,
                args=(target, job_args, child_startup),
                daemon=True,
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
                    mem_mb = contained_rss(self._pid, self._backend or "") / (1024 * 1024)
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
    ):
        if processes < 1:
            raise ValueError("Invalid number of processes")
        self._ids = set(range(processes))
        self._proc_limit = processes
        self._queue: Queue[ProcPool.Proc] = Queue(processes)
        self._procs: dict[int, ProcPool.Proc] = {}
        self._on_job_killed = on_job_killed
        self.running = True
        self._mg_thrd = Thread(target=self._thrd_mgr, daemon=True)

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
                if not proc.is_alive() or proc.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                    remove_pids.append(pid)
                    continue
                if proc.is_time_exceeded():
                    print(f"Process {pid} exceeded the time limit of {proc.get_time_limit()} seconds")
                    proc.kill(ProcPool.TIME)
                    continue
                if proc.is_mem_exceeded():
                    print(f"Process {pid} exceeded the memory limit of {proc.get_mem_limit()}MB")
                    proc.kill(ProcPool.MEM)
                    continue
            for pid in remove_pids:
                proc = self._procs.pop(pid)
                proc.cleanup()
                if proc.reason():
                    _safe_invoke_kill_hook(self._on_job_killed, proc.get_args(), proc.reason(), "ProcPool")
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
                        _safe_invoke_callback(proc.get_callback(), proc.get_args(), "ProcPool")
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
        proc = ProcPool.Proc(target=target, args=args, limit_mem=limit_mem, limit_time=limit_time, callback=callback)
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
            self.apply(target=func, args=item, limit_mem=limit_mem, limit_time=limit_time, callback=callback)


def _worker_loop(
    worker_id: int,
    job_queue: Any,
    done_queue: MPQueue,
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
        job_id, target, args = job
        try:
            backend = containment.enter_job()
        except BaseException as error:
            done_queue.put(
                (
                    "containment_error",
                    worker_id,
                    job_id,
                    f"{type(error).__name__}: {error}",
                )
            )
            raise
        done_queue.put(
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
            exc = str(err)
        except BaseException as error:
            fatal = error
        descendants = False
        cleanup_error = None
        try:
            descendants = containment.finish_job()
        except BaseException as error:
            cleanup_error = f"{type(error).__name__}: {error}"
        if fatal is not None:
            raise fatal
        restart = cleanup_error is not None or (
            descendants and containment.restart_after_descendants
        )
        done_queue.put(
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
        tasks += 1
        if restart:
            break
        if max_tasks and tasks >= max_tasks:
            done_queue.put(("exit", worker_id, None, None))
            break


class PersistentProcPool:
    def __init__(
        self,
        processes: int = 1,
        max_tasks_per_worker: int = 0,
        on_job_killed: JobKillHook | None = None,
        on_job_error: JobErrorHook | None = None,
        start_ack_timeout: float = _JOB_START_ACK_TIMEOUT,
    ):
        if processes < 1:
            raise ValueError("Invalid number of processes")
        if start_ack_timeout <= 0:
            raise ValueError("Invalid start acknowledgement timeout")
        self._proc_limit = processes
        self._max_tasks = max_tasks_per_worker
        self._job_queues: dict[int, Any] = {}
        self._done_queue: MPQueue = MPQueue()
        self._workers: dict[int, Process] = {}
        self._running_jobs: dict[int, dict[str, Any]] = {}
        self._worker_jobs: dict[int, Optional[int]] = {}
        self._job_id = 0
        self._on_job_killed = on_job_killed
        self._on_job_error = on_job_error
        self._start_ack_timeout = float(start_ack_timeout)
        self.running = True
        self._accepting = False
        self._mg_thrd = Thread(target=self._thrd_mgr, daemon=True)

    def __enter__(self):
        try:
            for worker_id in range(self._proc_limit):
                self._spawn_worker(worker_id)
        except BaseException:
            self.running = False
            for worker_id in list(self._workers):
                self._terminate_worker(worker_id)
            raise
        self._mg_thrd.start()
        self._accepting = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(force=True)
        self._mg_thrd.join()

    def shutdown(self, force: bool = False) -> None:
        self._accepting = False
        self.running = False
        for queue in list(self._job_queues.values()):
            try:
                queue.put(None)
            except Exception:
                pass
        if force:
            for worker_id in list(self._workers):
                job_id = self._worker_jobs.get(worker_id)
                job = self._running_jobs.get(job_id) if job_id is not None else None
                backend = job.get("backend") if job else None
                self._terminate_worker(worker_id, backend=backend)

    def _discard_job_queue(self, worker_id: int) -> None:
        queue = self._job_queues.pop(worker_id, None)
        if queue is not None:
            try:
                queue.close()
            except Exception:
                pass

    def _spawn_worker(self, worker_id: int) -> None:
        job_queue = self._job_queues.get(worker_id)
        if job_queue is None:
            job_queue = MPSimpleQueue()
            self._job_queues[worker_id] = job_queue
        startup, child_startup = Pipe(duplex=False)
        proc = Process(
            target=_worker_loop,
            args=(
                worker_id,
                job_queue,
                self._done_queue,
                self._max_tasks,
                child_startup,
            ),
            daemon=True,
        )
        try:
            proc.start()
            child_startup.close()
            if not startup.poll(_WORKER_START_TIMEOUT):
                raise RuntimeError("persistent worker did not establish containment")
            status, detail = startup.recv()
            if status != "ready":
                raise RuntimeError(f"persistent worker containment setup failed: {detail}")
        except BaseException:
            if proc.pid is not None:
                terminate_containment(proc.pid, None)
            proc.join(timeout=1)
            raise
        finally:
            startup.close()
            child_startup.close()
        self._workers[worker_id] = proc
        self._worker_jobs[worker_id] = None

    def _terminate_worker(self, worker_id: int, backend: str | None = None) -> None:
        proc = self._workers.get(worker_id)
        if proc is not None:
            if proc.pid is None:
                return
            remaining = terminate_containment(proc.pid, backend)
            if remaining:
                print(
                    f"PersistentProcPool worker {worker_id} containment still has "
                    f"live processes: {remaining}"
                )
            proc.join(timeout=1)

    def _restart_worker(
        self,
        worker_id: int,
        backend: str | None = None,
        replace_job_queue: bool = False,
    ) -> None:
        self._terminate_worker(worker_id, backend=backend)
        if replace_job_queue:
            self._discard_job_queue(worker_id)
        if self.running:
            self._spawn_worker(worker_id)
        else:
            self._workers.pop(worker_id, None)
            self._worker_jobs[worker_id] = None

    def _thrd_mgr(self) -> None:
        while self.running or self._running_jobs:
            now = monotonic()
            while True:
                try:
                    msg = self._done_queue.get_nowait()
                except Empty:
                    break
                action, worker_id, job_id, payload = msg
                if action == "start":
                    job = self._running_jobs.get(job_id)
                    if job:
                        job["start"] = payload["started"]
                        job["backend"] = payload["backend"]
                elif action == "done":
                    job = self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                    if payload.get("restart"):
                        backend = job.get("backend") if job else None
                        self._restart_worker(worker_id, backend=backend)
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
                    if errors and job:
                        error = " ".join(errors)
                        print(
                            f"PersistentProcPool worker {worker_id} job "
                            f"{job_id} failed: {error}"
                        )
                        _safe_invoke_error_hook(
                            self._on_job_error,
                            job["args"],
                            error,
                            "PersistentProcPool",
                        )
                    if job and job.get("callback"):
                        _safe_invoke_callback(job["callback"], job["args"], "PersistentProcPool")
                elif action == "containment_error":
                    job = self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                    self._restart_worker(worker_id)
                    if job:
                        _safe_invoke_error_hook(
                            self._on_job_error,
                            job["args"],
                            f"Process containment setup failed: {payload}",
                            "PersistentProcPool",
                        )
                        if job.get("callback"):
                            _safe_invoke_callback(
                                job["callback"],
                                job["args"],
                                "PersistentProcPool",
                            )
                elif action == "exit":
                    self._restart_worker(worker_id)
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
                        job = self._running_jobs.pop(job_id, None)
                        backend = job.get("backend") if job else None
                    self._worker_jobs[worker_id] = None
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
                        _safe_invoke_error_hook(
                            self._on_job_error,
                            job["args"],
                            error,
                            "PersistentProcPool",
                        )
                        if job.get("callback"):
                            _safe_invoke_callback(
                                job["callback"],
                                job["args"],
                                "PersistentProcPool",
                            )
                    continue
                if job_id is None:
                    continue
                job = self._running_jobs.get(job_id)
                if not job:
                    continue
                if job.get("start") is None:
                    submitted = job.get("submitted")
                    if submitted is None or now - submitted <= self._start_ack_timeout:
                        continue
                    self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                    self._restart_worker(worker_id, replace_job_queue=True)
                    error = (
                        f"Worker did not acknowledge job {job_id} within "
                        f"{self._start_ack_timeout:g} seconds"
                    )
                    _safe_invoke_error_hook(
                        self._on_job_error,
                        job["args"],
                        error,
                        "PersistentProcPool",
                    )
                    if job.get("callback"):
                        _safe_invoke_callback(
                            job["callback"],
                            job["args"],
                            "PersistentProcPool",
                        )
                    continue
                if job["limit_time"] and now - job["start"] > job["limit_time"]:
                    print(
                        f"PersistentProcPool worker {worker_id} job {job_id} exceeded the time limit of "
                        f"{job['limit_time']} seconds"
                    )
                    self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                    self._restart_worker(worker_id, backend=job["backend"])
                    _safe_invoke_kill_hook(
                        self._on_job_killed,
                        job["args"],
                        ProcPool.TIME,
                        "PersistentProcPool",
                    )
                    if job.get("callback"):
                        _safe_invoke_callback(
                            job["callback"],
                            job["args"],
                            "PersistentProcPool",
                        )
                    continue
                if job["limit_mem"]:
                    if proc.pid is None:
                        continue
                    try:
                        mem_mb = contained_rss(proc.pid, job["backend"]) / (1024 * 1024)
                    except ContainmentError as error:
                        print(
                            f"PersistentProcPool worker {worker_id} job {job_id} "
                            f"containment accounting failed: {error}"
                        )
                        mem_mb = float("inf")
                    if mem_mb > job["limit_mem"]:
                        print(
                            f"PersistentProcPool worker {worker_id} job {job_id} exceeded the memory limit of "
                            f"{job['limit_mem']}MB"
                        )
                        self._running_jobs.pop(job_id, None)
                        self._worker_jobs[worker_id] = None
                        self._restart_worker(worker_id, backend=job["backend"])
                        _safe_invoke_kill_hook(
                            self._on_job_killed,
                            job["args"],
                            ProcPool.MEM,
                            "PersistentProcPool",
                        )
                        if job.get("callback"):
                            _safe_invoke_callback(
                                job["callback"],
                                job["args"],
                                "PersistentProcPool",
                            )
                        continue
            sleep(_MANAGER_INTERVAL)

    def apply(
        self,
        target: Callable,
        args: Iterable[Any],
        limit_mem: int = 0,
        limit_time: int = 0,
        callback: JobCallback | None = None,
    ):
        if not self._accepting:
            raise RuntimeError("PersistentProcPool is not accepting jobs")
        self._job_id += 1
        job_id = self._job_id
        norm_args = _normalize_args(args)
        while self._accepting:
            for worker_id, current in self._worker_jobs.items():
                if current is None:
                    self._worker_jobs[worker_id] = job_id
                    job: dict[str, Any] = {
                        "args": norm_args,
                        "callback": callback,
                        "limit_time": limit_time,
                        "limit_mem": limit_mem,
                        "start": None,
                        "submitted": None,
                    }
                    self._running_jobs[job_id] = job
                    try:
                        self._job_queues[worker_id].put((job_id, target, norm_args))
                    except Exception as error:
                        self._running_jobs.pop(job_id, None)
                        if self._worker_jobs.get(worker_id) == job_id:
                            self._worker_jobs[worker_id] = None
                        if isinstance(error, (EOFError, OSError, ValueError)):
                            self._restart_worker(worker_id, replace_job_queue=True)
                        raise JobSubmissionError(
                            f"Unable to submit persistent job {job_id}"
                        ) from error
                    job["submitted"] = monotonic()
                    return
            sleep(0.1)
        raise RuntimeError("PersistentProcPool is not accepting jobs")
