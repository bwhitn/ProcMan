from pathlib import Path

import procman._memory as memory


def test_available_memory_uses_tightest_telemetry(monkeypatch) -> None:
    monkeypatch.setattr(memory, "_host_available_memory_bytes", lambda: 900)
    monkeypatch.setattr(memory, "_cgroup_available_memory_bytes", lambda: 700)

    assert memory.available_memory_bytes() == 700


def test_available_memory_is_unknown_when_all_telemetry_fails(monkeypatch) -> None:
    monkeypatch.setattr(memory, "_host_available_memory_bytes", lambda: None)
    monkeypatch.setattr(memory, "_cgroup_available_memory_bytes", lambda: None)

    assert memory.available_memory_bytes() is None


def test_cgroup_v2_headroom_uses_nested_hard_limit(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cgroup_root = tmp_path / "cgroup"
    member = cgroup_root / "workload"
    member.mkdir(parents=True)
    (member / "memory.max").write_text("1000", encoding="utf-8")
    (member / "memory.current").write_text("300", encoding="utf-8")
    (cgroup_root / "memory.max").write_text("800", encoding="utf-8")
    (cgroup_root / "memory.current").write_text("250", encoding="utf-8")

    proc_cgroup = tmp_path / "proc-self-cgroup"
    proc_cgroup.write_text("0::/workload\n", encoding="utf-8")
    mountinfo = tmp_path / "proc-self-mountinfo"
    mountinfo.write_text(
        f"1 0 0:1 / {cgroup_root} rw - cgroup2 cgroup rw\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(memory, "_PROC_CGROUP", proc_cgroup)
    monkeypatch.setattr(memory, "_PROC_MOUNTINFO", mountinfo)

    assert memory._cgroup_available_memory_bytes() == 550


def test_cgroup_v1_headroom_uses_memory_controller(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cgroup_root = tmp_path / "memory"
    member = cgroup_root / "workload"
    member.mkdir(parents=True)
    (member / "memory.limit_in_bytes").write_text("2000", encoding="utf-8")
    (member / "memory.usage_in_bytes").write_text("500", encoding="utf-8")
    (cgroup_root / "memory.limit_in_bytes").write_text("1000", encoding="utf-8")
    (cgroup_root / "memory.usage_in_bytes").write_text("100", encoding="utf-8")

    proc_cgroup = tmp_path / "proc-self-cgroup"
    proc_cgroup.write_text("5:cpu,memory:/workload\n", encoding="utf-8")
    mountinfo = tmp_path / "proc-self-mountinfo"
    mountinfo.write_text(
        f"2 0 0:2 / {cgroup_root} rw - cgroup cgroup rw,memory\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(memory, "_PROC_CGROUP", proc_cgroup)
    monkeypatch.setattr(memory, "_PROC_MOUNTINFO", mountinfo)

    assert memory._cgroup_available_memory_bytes() == 900
