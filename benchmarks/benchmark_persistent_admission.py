from __future__ import annotations

import argparse
from time import perf_counter

from procman import JobTracker, PersistentProcPool


def _noop() -> None:
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark persistent-pool submission and memory admission.",
    )
    parser.add_argument("--jobs", type=int, default=1_000)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument(
        "--limit-mem",
        type=int,
        default=256,
        help="Per-job enforcement limit in MiB; use 0 to disable it.",
    )
    parser.add_argument(
        "--reserve-mem",
        type=int,
        default=64,
        help="Admission reservation in MiB; use 0 for the ungated baseline.",
    )
    args = parser.parse_args()
    if (
        args.jobs < 1
        or args.processes < 1
        or args.limit_mem < 0
        or args.reserve_mem < 0
    ):
        parser.error(
            "jobs and processes must be positive; memory values cannot be negative"
        )

    tracker = JobTracker()
    started = perf_counter()
    with PersistentProcPool(args.processes) as pool:
        submission_started = perf_counter()
        for _ in range(args.jobs):
            tracker.submitted()
            try:
                pool.apply(
                    _noop,
                    [],
                    limit_mem=args.limit_mem,
                    reserve_mem=args.reserve_mem,
                    callback=tracker.done,
                )
            except Exception:
                tracker.cancelled()
                raise
        submitted = perf_counter()
        if not tracker.wait(timeout=300):
            raise TimeoutError("benchmark jobs did not complete within 300 seconds")
    finished = perf_counter()
    submission_seconds = submitted - submission_started
    total_seconds = finished - started
    print(
        f"jobs={args.jobs} processes={args.processes} "
        f"limit_mem_mib={args.limit_mem} "
        f"reserve_mem_mib={args.reserve_mem} "
        f"submission_s={submission_seconds:.6f} "
        f"total_s={total_seconds:.6f} "
        f"jobs_per_s={args.jobs / total_seconds:.2f}"
    )


if __name__ == "__main__":
    main()
