from __future__ import annotations

import os
import signal
from time import monotonic, sleep

import psutil  # type: ignore[import-untyped]

POSIX_PROCESS_GROUP = "process_group"
WINDOWS_JOB_OBJECT = "job_object"
_TERMINATION_TIMEOUT = 5.0


class ContainmentError(RuntimeError):
    """Raised when a job cannot be placed in or cleaned from containment."""


def _is_live(process: psutil.Process) -> bool:
    try:
        return process.is_running() and process.status() not in {
            psutil.STATUS_DEAD,
            psutil.STATUS_ZOMBIE,
        }
    except (psutil.Error, ProcessLookupError):
        return False


def _posix_group_processes(pgid: int) -> list[psutil.Process]:
    processes: list[psutil.Process] = []
    try:
        for process in psutil.process_iter(attrs=("pid",)):
            try:
                if os.getpgid(process.pid) == pgid and _is_live(process):
                    processes.append(process)
            except (OSError, psutil.Error, ProcessLookupError):
                continue
    except (OSError, psutil.AccessDenied, PermissionError) as error:
        raise ContainmentError(
            f"cannot enumerate process group {pgid} for resource accounting"
        ) from error
    return processes


def _process_tree(root_pid: int) -> list[psutil.Process]:
    try:
        root = psutil.Process(root_pid)
        try:
            processes = [root, *root.children(recursive=True)]
        except (psutil.AccessDenied, PermissionError):
            processes = [root]
    except (psutil.Error, ProcessLookupError, PermissionError):
        return []
    return list(
        {process.pid: process for process in processes if _is_live(process)}.values()
    )


def _posix_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError as error:
        raise ContainmentError(
            f"permission denied probing process group {pgid}"
        ) from error
    return True


def _terminate_posix_group(
    pgid: int,
    timeout: float = _TERMINATION_TIMEOUT,
) -> tuple[bool, list[int]]:
    if pgid <= 1 or pgid == os.getpgrp():
        raise ContainmentError(f"refusing to terminate unsafe process group {pgid}")

    saw_members = _posix_group_exists(pgid)
    if not saw_members:
        return False, []
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return saw_members, []
    except PermissionError as error:
        raise ContainmentError(
            f"permission denied terminating process group {pgid}"
        ) from error

    # A second signal closes the narrow race with a concurrent fork. EPERM here
    # can mean that the first signal left only dead, re-parented group members.
    sleep(min(timeout, 0.01))
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    return saw_members, []


def _terminate_process_tree(
    root_pid: int,
    timeout: float = _TERMINATION_TIMEOUT,
) -> list[int]:
    processes = _process_tree(root_pid)
    for process in reversed(processes):
        try:
            process.kill()
        except (psutil.Error, ProcessLookupError):
            continue

    deadline = monotonic() + timeout
    while monotonic() < deadline:
        live = [process for process in processes if _is_live(process)]
        if not live:
            return []
        sleep(0.01)
    return [process.pid for process in processes if _is_live(process)]


def contained_rss(root_pid: int, backend: str) -> int:
    """Return RSS for every live process in a job's containment unit."""

    if backend == POSIX_PROCESS_GROUP:
        processes = _posix_group_processes(root_pid)
    else:
        processes = _process_tree(root_pid)
    if not processes:
        raise ContainmentError(
            f"containment unit for process {root_pid} is unavailable"
        )

    total = 0
    for process in processes:
        try:
            total += process.memory_info().rss
        except (psutil.AccessDenied, PermissionError) as error:
            raise ContainmentError(
                f"cannot account for contained process {process.pid}"
            ) from error
        except (psutil.NoSuchProcess, ProcessLookupError):
            continue
    return total


def terminate_containment(
    root_pid: int,
    backend: str | None,
    timeout: float = _TERMINATION_TIMEOUT,
) -> list[int]:
    """Terminate a worker and all members of its active containment unit."""

    if backend is None and os.name == "posix":
        try:
            pgid = os.getpgid(root_pid)
        except (OSError, ProcessLookupError):
            pgid = None
        if pgid == root_pid and pgid != os.getpgrp():
            backend = POSIX_PROCESS_GROUP
    if backend == POSIX_PROCESS_GROUP:
        _, remaining = _terminate_posix_group(root_pid, timeout=timeout)
        return remaining
    return _terminate_process_tree(root_pid, timeout=timeout)


def _create_windows_job_object() -> int:
    import ctypes
    from ctypes import wintypes

    job_object_extended_limit_information = 9
    job_object_limit_kill_on_job_close = 0x00002000

    class IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimitInformation),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    win_dll = getattr(ctypes, "WinDLL")
    win_error = getattr(ctypes, "WinError")
    get_last_error = getattr(ctypes, "get_last_error")
    kernel32 = win_dll("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        raise win_error(get_last_error())

    info = ExtendedLimitInformation()
    info.BasicLimitInformation.LimitFlags = job_object_limit_kill_on_job_close
    if not kernel32.SetInformationJobObject(
        handle,
        job_object_extended_limit_information,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        error = win_error(get_last_error())
        kernel32.CloseHandle(handle)
        raise error
    if not kernel32.AssignProcessToJobObject(handle, kernel32.GetCurrentProcess()):
        error = win_error(get_last_error())
        kernel32.CloseHandle(handle)
        raise error
    return int(handle)


class WorkerContainment:
    """Child-side lifetime management for one worker and its descendants."""

    def __init__(self) -> None:
        self._active = False
        self._original_pgid: int | None = None
        self._windows_job_handle: int | None = None
        if os.name == "posix":
            self.backend = POSIX_PROCESS_GROUP
            self._original_pgid = os.getpgrp()
        elif os.name == "nt":
            self.backend = WINDOWS_JOB_OBJECT
            self._windows_job_handle = _create_windows_job_object()
        else:
            raise ContainmentError(
                f"unsupported process-containment platform: {os.name}"
            )

    @property
    def restart_after_descendants(self) -> bool:
        return self.backend == WINDOWS_JOB_OBJECT

    def enter_job(self) -> str:
        if self._active:
            raise ContainmentError("worker is already inside a job containment unit")
        if self.backend == POSIX_PROCESS_GROUP:
            os.setpgid(0, 0)
            if os.getpgrp() != os.getpid():
                raise ContainmentError("failed to create an isolated process group")
        self._active = True
        return self.backend

    def finish_job(self) -> bool:
        """Clean descendants and return whether the job left any behind."""

        if not self._active:
            return False
        if self.backend == WINDOWS_JOB_OBJECT:
            descendants = [
                process
                for process in _process_tree(os.getpid())
                if process.pid != os.getpid()
            ]
            self._active = False
            return bool(descendants)

        if self._original_pgid is None:
            raise ContainmentError("original process group was not recorded")
        job_pgid = os.getpgrp()
        if job_pgid != os.getpid():
            raise ContainmentError("worker left its isolated process group")
        os.setpgid(0, self._original_pgid)
        self._active = False
        saw_descendants, remaining = _terminate_posix_group(job_pgid)
        if remaining:
            raise ContainmentError(
                f"process group {job_pgid} still contains live processes: {remaining}"
            )
        return saw_descendants
