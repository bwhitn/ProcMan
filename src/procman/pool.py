from __future__ import annotations

from inspect import signature
from multiprocessing import Process, Queue as MPQueue
from queue import Empty, Queue
from threading import Thread
from time import sleep, time
from typing import Any, Callable, Iterable, Optional

import psutil

JobArgs = list[Any]
JobCallback = Callable[[JobArgs], None]
JobKillHook = Callable[[JobArgs, str], None]
JobErrorHook = Callable[[JobArgs, str], None]


def _normalize_args(args: Iterable[Any]) -> JobArgs:
    if isinstance(args, list):
        return list(args)
    if isinstance(args, tuple):
        return list(args)
    return list(args)


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
            self._args = (target, _normalize_args(args))
            self._cb = callback
            self._kill_reason: str | None = None

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
            run_args = list(job_args)
            if len(signature(target).parameters) > len(run_args):
                run_args.append(self._pool_id)
            proc = Process(target=target, args=run_args, daemon=True)
            proc.start()
            self._start_time = psutil.Process(proc.pid).create_time()
            self._pid = proc.pid

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
                return time() - psproc.create_time() > self._limit_time
            return False

        def is_mem_exceeded(self) -> bool:
            psproc = self.get_psproc()
            if psproc and self._pid > 0 and self._limit_mem != 0:
                return self._limit_mem < (psproc.memory_info().rss / (1024 * 1024))
            return False

        def is_alive(self) -> bool:
            psproc = self.get_psproc()
            if psproc:
                return psproc.is_running()
            return False

        def get_pool_pid(self) -> int:
            return self._pool_id

        def kill(self, reason: str) -> None:
            psproc = self.get_psproc()
            self._kill_reason = reason
            if psproc:
                psproc.kill()

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
        self._queue = Queue(processes)
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
            sleep(1)
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
                if proc.reason():
                    _safe_invoke_kill_hook(self._on_job_killed, proc.get_args(), proc.reason(), "ProcPool")
                _safe_invoke_callback(proc.get_callback(), proc.get_args(), "ProcPool")
                self._ids.add(proc.get_pool_pid())
            while (not self._queue.empty()) and (len(self._procs) < self._proc_limit):
                try:
                    proc = self._queue.get(timeout=1)
                    proc._pool_id = self._ids.pop()
                    proc.start()
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


def _worker_loop(worker_id: int, job_queue: MPQueue, done_queue: MPQueue, max_tasks: int) -> None:
    tasks = 0
    while True:
        job = job_queue.get()
        if job is None:
            break
        job_id, target, args = job
        done_queue.put(("start", worker_id, job_id, time()))
        exc = None
        try:
            target(*args)
        except Exception as err:  # noqa: BLE001
            exc = str(err)
        done_queue.put(("done", worker_id, job_id, exc))
        tasks += 1
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
    ):
        if processes < 1:
            raise ValueError("Invalid number of processes")
        self._proc_limit = processes
        self._max_tasks = max_tasks_per_worker
        self._job_queue: MPQueue = MPQueue()
        self._done_queue: MPQueue = MPQueue()
        self._workers: dict[int, Process] = {}
        self._running_jobs: dict[int, dict[str, Any]] = {}
        self._worker_jobs: dict[int, Optional[int]] = {}
        self._job_id = 0
        self._on_job_killed = on_job_killed
        self._on_job_error = on_job_error
        self.running = True
        self._mg_thrd = Thread(target=self._thrd_mgr, daemon=True)

    def __enter__(self):
        for worker_id in range(self._proc_limit):
            self._spawn_worker(worker_id)
        self._mg_thrd.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(force=True)
        self._mg_thrd.join()

    def shutdown(self, force: bool = False) -> None:
        self.running = False
        for _ in range(len(self._workers)):
            try:
                self._job_queue.put_nowait(None)
            except Exception:
                pass
        if force:
            for proc in list(self._workers.values()):
                try:
                    psutil.Process(proc.pid).kill()
                except Exception:
                    pass

    def _spawn_worker(self, worker_id: int) -> None:
        proc = Process(
            target=_worker_loop,
            args=(worker_id, self._job_queue, self._done_queue, self._max_tasks),
            daemon=True,
        )
        proc.start()
        self._workers[worker_id] = proc
        self._worker_jobs[worker_id] = None

    def _restart_worker(self, worker_id: int) -> None:
        proc = self._workers.get(worker_id)
        if proc is not None:
            try:
                psutil.Process(proc.pid).kill()
            except Exception:
                pass
        self._spawn_worker(worker_id)

    def _thrd_mgr(self) -> None:
        while self.running or self._running_jobs:
            now = time()
            while True:
                try:
                    msg = self._done_queue.get_nowait()
                except Empty:
                    break
                action, worker_id, job_id, payload = msg
                if action == "start":
                    job = self._running_jobs.get(job_id)
                    if job:
                        job["start"] = payload
                elif action == "done":
                    job = self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                    if payload and job:
                        print(f"PersistentProcPool worker {worker_id} job {job_id} failed: {payload}")
                        _safe_invoke_error_hook(self._on_job_error, job["args"], str(payload), "PersistentProcPool")
                    if job and job.get("callback"):
                        _safe_invoke_callback(job["callback"], job["args"], "PersistentProcPool")
                elif action == "exit":
                    self._restart_worker(worker_id)
            for worker_id, job_id in list(self._worker_jobs.items()):
                proc = self._workers.get(worker_id)
                if proc is None:
                    continue
                if not proc.is_alive():
                    if job_id is not None:
                        job = self._running_jobs.pop(job_id, None)
                        if job:
                            _safe_invoke_kill_hook(self._on_job_killed, job["args"], ProcPool.TIME, "PersistentProcPool")
                            if job.get("callback"):
                                _safe_invoke_callback(job["callback"], job["args"], "PersistentProcPool")
                    self._restart_worker(worker_id)
                    continue
                if job_id is None:
                    continue
                job = self._running_jobs.get(job_id)
                if not job or not job.get("start"):
                    continue
                try:
                    psproc = psutil.Process(proc.pid)
                except (psutil.Error, ProcessLookupError):
                    if job_id is not None:
                        job = self._running_jobs.pop(job_id, None)
                        if job:
                            _safe_invoke_kill_hook(self._on_job_killed, job["args"], ProcPool.TIME, "PersistentProcPool")
                            if job.get("callback"):
                                _safe_invoke_callback(job["callback"], job["args"], "PersistentProcPool")
                    self._worker_jobs[worker_id] = None
                    self._restart_worker(worker_id)
                    continue
                if job["limit_time"] and now - job["start"] > job["limit_time"]:
                    print(
                        f"PersistentProcPool worker {worker_id} job {job_id} exceeded the time limit of "
                        f"{job['limit_time']} seconds"
                    )
                    _safe_invoke_kill_hook(self._on_job_killed, job["args"], ProcPool.TIME, "PersistentProcPool")
                    try:
                        psproc.kill()
                    except Exception:
                        pass
                    if job.get("callback"):
                        _safe_invoke_callback(job["callback"], job["args"], "PersistentProcPool")
                    self._running_jobs.pop(job_id, None)
                    self._worker_jobs[worker_id] = None
                    self._restart_worker(worker_id)
                    continue
                if job["limit_mem"]:
                    try:
                        mem_mb = psproc.memory_info().rss / (1024 * 1024)
                    except (psutil.Error, ProcessLookupError):
                        self._worker_jobs[worker_id] = None
                        self._restart_worker(worker_id)
                        continue
                    if mem_mb > job["limit_mem"]:
                        print(
                            f"PersistentProcPool worker {worker_id} job {job_id} exceeded the memory limit of "
                            f"{job['limit_mem']}MB"
                        )
                        _safe_invoke_kill_hook(self._on_job_killed, job["args"], ProcPool.MEM, "PersistentProcPool")
                        try:
                            psproc.kill()
                        except Exception:
                            pass
                        if job.get("callback"):
                            _safe_invoke_callback(job["callback"], job["args"], "PersistentProcPool")
                        self._running_jobs.pop(job_id, None)
                        self._worker_jobs[worker_id] = None
                        self._restart_worker(worker_id)
                        continue
            sleep(0.5)

    def apply(
        self,
        target: Callable,
        args: Iterable[Any],
        limit_mem: int = 0,
        limit_time: int = 0,
        callback: JobCallback | None = None,
    ):
        self._job_id += 1
        job_id = self._job_id
        norm_args = _normalize_args(args)
        while True:
            for worker_id, current in self._worker_jobs.items():
                if current is None:
                    self._worker_jobs[worker_id] = job_id
                    self._running_jobs[job_id] = {
                        "args": norm_args,
                        "callback": callback,
                        "limit_time": limit_time,
                        "limit_mem": limit_mem,
                        "start": None,
                    }
                    self._job_queue.put((job_id, target, list(norm_args)))
                    return
            sleep(0.1)
