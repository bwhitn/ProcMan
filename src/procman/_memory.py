from __future__ import annotations

from pathlib import Path, PurePosixPath

import psutil  # type: ignore[import-untyped]

_PROC_CGROUP = Path("/proc/self/cgroup")
_PROC_MOUNTINFO = Path("/proc/self/mountinfo")
_CGROUP_ROOT = Path("/sys/fs/cgroup")
_V1_UNLIMITED_THRESHOLD = 1 << 60


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None


def _read_counter(path: Path, *, v1_limit: bool = False) -> int | None:
    value = _read_text(path)
    if value is None:
        return None
    value = value.strip()
    if not value or value == "max":
        return None
    try:
        counter = int(value)
    except ValueError:
        return None
    if counter < 0 or (v1_limit and counter >= _V1_UNLIMITED_THRESHOLD):
        return None
    return counter


def _unescape_mount_path(value: str) -> str:
    # Linux mountinfo uses octal escapes for these four path characters.
    return (
        value.replace(r"\040", " ")
        .replace(r"\011", "\t")
        .replace(r"\012", "\n")
        .replace(r"\134", "\\")
    )


def _cgroup_memberships() -> tuple[str | None, str | None]:
    contents = _read_text(_PROC_CGROUP)
    if contents is None:
        return None, None
    v2_path = None
    v1_memory_path = None
    for line in contents.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            continue
        hierarchy, controllers, path = fields
        if hierarchy == "0" and not controllers:
            v2_path = path
        elif "memory" in controllers.split(","):
            v1_memory_path = path
    return v2_path, v1_memory_path


def _cgroup_mounts() -> list[tuple[str, Path, str, frozenset[str]]]:
    contents = _read_text(_PROC_MOUNTINFO)
    if contents is None:
        return []
    mounts = []
    for line in contents.splitlines():
        try:
            mount_fields, filesystem_fields = line.split(" - ", 1)
        except ValueError:
            continue
        left = mount_fields.split()
        right = filesystem_fields.split()
        if len(left) < 5 or len(right) < 3:
            continue
        filesystem = right[0]
        if filesystem not in {"cgroup", "cgroup2"}:
            continue
        mounts.append(
            (
                _unescape_mount_path(left[3]),
                Path(_unescape_mount_path(left[4])),
                filesystem,
                frozenset(right[2].split(",")),
            )
        )
    return mounts


def _membership_directory(
    mount_root: str,
    mount_point: Path,
    membership: str,
) -> Path:
    root = PurePosixPath(mount_root)
    member = PurePosixPath(membership)
    try:
        relative = member.relative_to(root)
    except ValueError:
        # A cgroup namespace may expose the member as '/' even when the host
        # mount has a deeper root. In that case the visible path is relative
        # to the mount point itself.
        relative = PurePosixPath(str(member).lstrip("/"))
    return mount_point.joinpath(*relative.parts)


def _hierarchy_headroom(
    member_directory: Path,
    mount_point: Path,
    *,
    limit_name: str,
    usage_name: str,
    v1: bool = False,
) -> int | None:
    headrooms = []
    current = member_directory
    while True:
        limit = _read_counter(current / limit_name, v1_limit=v1)
        usage = _read_counter(current / usage_name)
        if limit is not None and usage is not None:
            headrooms.append(max(0, limit - usage))
        if current == mount_point or current.parent == current:
            break
        current = current.parent
    return min(headrooms) if headrooms else None


def _cgroup_available_memory_bytes() -> int | None:
    v2_membership, v1_membership = _cgroup_memberships()
    candidates: list[int] = []
    mounts = _cgroup_mounts()

    if v2_membership is not None:
        v2_mounts = [mount for mount in mounts if mount[2] == "cgroup2"]
        if not v2_mounts:
            v2_mounts = [("/", _CGROUP_ROOT, "cgroup2", frozenset())]
        for mount_root, mount_point, _filesystem, _options in v2_mounts:
            directory = _membership_directory(
                mount_root,
                mount_point,
                v2_membership,
            )
            available = _hierarchy_headroom(
                directory,
                mount_point,
                limit_name="memory.max",
                usage_name="memory.current",
            )
            if available is not None:
                candidates.append(available)

    if v1_membership is not None:
        v1_mounts = [
            mount for mount in mounts if mount[2] == "cgroup" and "memory" in mount[3]
        ]
        if not v1_mounts:
            v1_mounts = [
                ("/", _CGROUP_ROOT / "memory", "cgroup", frozenset({"memory"}))
            ]
        for mount_root, mount_point, _filesystem, _options in v1_mounts:
            directory = _membership_directory(
                mount_root,
                mount_point,
                v1_membership,
            )
            available = _hierarchy_headroom(
                directory,
                mount_point,
                limit_name="memory.limit_in_bytes",
                usage_name="memory.usage_in_bytes",
                v1=True,
            )
            if available is not None:
                candidates.append(available)

    return min(candidates) if candidates else None


def _host_available_memory_bytes() -> int | None:
    try:
        available = int(psutil.virtual_memory().available)
    except (AttributeError, OSError, TypeError, ValueError):
        return None
    return max(0, available)


def available_memory_bytes() -> int | None:
    """Return the tightest observable host or cgroup memory headroom.

    ``None`` means that neither portable host telemetry nor Linux cgroup
    telemetry was available. Callers can then preserve their pre-admission
    behavior rather than treating missing telemetry as zero memory.
    """

    candidates = [
        value
        for value in (
            _host_available_memory_bytes(),
            _cgroup_available_memory_bytes(),
        )
        if value is not None
    ]
    return min(candidates) if candidates else None
