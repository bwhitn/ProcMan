# procman

`procman` provides lightweight process-pool helpers with:

- per-job memory limits
- per-job time limits
- optional worker recycling
- parent-side completion callbacks
- parent-side kill/error hooks
- descendant-process cleanup and aggregate memory accounting

It is intended for callers that need stricter process supervision than the
standard library pools provide, while still keeping the pool logic generic.

## API

The package exports:

- `ProcPool`
- `PersistentProcPool`
- `JobSubmissionError`
- `JobTracker`
- logger hook helpers: `make_job_killed_hook()` and `make_job_error_hook()`

`JobTracker` tracks submitted, cancelled, completed, and pending jobs. Its `done()` method is shaped to be passed directly as a pool callback.

Both pools support:

- `apply(target, args, limit_mem=0, limit_time=0, callback=None)`

`PersistentProcPool.apply()` additionally accepts `reserve_mem=None`, measured
in MB. It defaults to `limit_mem` and controls only aggregate admission; it
does not change the job's enforced memory limit.

Both constructors accept `mp_context`, either a multiprocessing context object
or a start-method name. By default ProcMan uses `forkserver` when the platform
supports it and `spawn` otherwise. The selected value is exposed through the
pool's `start_method` property. ProcMan never changes the process-global start
method.

`PersistentProcPool` serializes each job synchronously before `apply()`
returns. An unserializable target or argument raises `JobSubmissionError`
without consuming a worker slot. Its constructor accepts
`start_ack_timeout=10.0` to control how long an accepted submission may wait
for its worker to acknowledge the job before that worker is replaced.

Pool constructors also accept optional hooks. The hook helpers can build these hooks for logger-like objects passed in job args:

- `on_job_killed(args, reason)` for enforced time or memory limits
- `on_job_error(args, error)` for `PersistentProcPool` target failures,
  submission/start failures, abnormal worker exits, and interrupted shutdown jobs

`reason` is one of:

- `ProcPool.TIME`
- `ProcPool.MEM`

## Notes

- `limit_mem` is measured in MB of aggregate RSS for the worker and its
  descendants.
- `limit_time` is measured in seconds.
- callbacks and hooks run in the parent process.
- `forkserver` and `spawn` require targets and arguments to be pickleable.
  Callers may explicitly request `mp_context="fork"` on supporting POSIX
  systems, but doing so from a multithreaded application can deadlock and is
  not recommended.
- Multiprocessing queues, locks, and other synchronization objects supplied in
  job arguments must be compatible with the pool's selected context.
- An abnormal persistent-worker exit reports its exit code or POSIX signal to
  the error hook, replaces the worker, and then invokes the completion callback.
- limit and completion callbacks run only after ProcMan has terminated the
  job's remaining contained descendants.
- Worker processes are explicitly non-daemonic so a supervised job may create
  multiprocessing children. ProcMan still joins or terminates every worker
  explicitly; descendant cleanup and resource accounting apply to those
  children as described below.
- Each persistent worker owns a dedicated one-way completion connection. The
  parent multiplexes those connections without a SyncManager process or a
  shared writer lock, so terminating one worker cannot block completion
  reports from the others.
- Persistent pools reserve `reserve_mem` (or `limit_mem` by default) for each
  active job. Reservations must fit within the tightest available host or
  Linux cgroup v1/v2 memory headroom before another job is assigned. This makes
  `apply()` provide FIFO backpressure when several individually valid jobs
  would exceed their aggregate container budget; it does not reject, kill, or
  truncate the job and does not impose a new input, analysis, or output limit.
  A reservation larger than observed headroom is allowed to run when no other
  job is active, while `limit_mem` remains the authoritative per-job limit.
- Set a smaller `reserve_mem` for measured-light work to retain worker
  concurrency while keeping a larger defensive `limit_mem`; use measured
  aggregate RSS to size heavy-work reservations. Jobs with both values at zero
  are not memory-reserved. If neither host nor cgroup memory telemetry is
  available, persistent pools retain worker-slot-only admission. Headroom is
  sampled again while waiting and rebased between job waves, so unrelated
  memory pressure can delay new work without cancelling accepted work.

## Admission benchmark

Run the included benchmark with and without memory reservations:

```bash
python benchmarks/benchmark_persistent_admission.py --jobs 1000 --processes 4 --limit-mem 256 --reserve-mem 64
python benchmarks/benchmark_persistent_admission.py --jobs 1000 --processes 4 --limit-mem 256 --reserve-mem 0
```

For container-specific results, run it inside the same cgroup and memory limit
as the production workload.

## Process containment

ProcMan establishes containment before invoking a job target:

- POSIX workers run each active job in an isolated process group. Time and
  memory termination signals the complete group, including normally spawned
  children and re-parented grandchildren.
- Windows workers assign themselves to a Job Object configured with
  kill-on-close. Terminating or recycling the worker therefore terminates its
  associated descendants.

These mechanisms require no administrator or superuser privileges for
processes owned by the caller. ProcMan does not mount, create, configure, or
move processes between cgroups. A container's existing cgroup limits remain an
independent outer resource boundary. Deployments that require per-job cgroups
must arrange an unprivileged delegated cgroup v2 subtree outside ProcMan.

ProcMan is process supervision, not a sandbox. In particular, trusted POSIX
target code can deliberately escape a process group by creating a new session
or group. Use an externally configured container, delegated cgroup, or other
platform sandbox when jobs can execute untrusted code.
