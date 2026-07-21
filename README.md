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
- `JobTracker`
- logger hook helpers: `make_job_killed_hook()` and `make_job_error_hook()`

`JobTracker` tracks submitted, cancelled, completed, and pending jobs. Its `done()` method is shaped to be passed directly as a pool callback.

Both pools support:

- `apply(target, args, limit_mem=0, limit_time=0, callback=None)`

Pool constructors also accept optional hooks. The hook helpers can build these hooks for logger-like objects passed in job args:

- `on_job_killed(args, reason)`
- `on_job_error(args, error)` for `PersistentProcPool`

`reason` is one of:

- `ProcPool.TIME`
- `ProcPool.MEM`

## Notes

- `limit_mem` is measured in MB of aggregate RSS for the worker and its
  descendants.
- `limit_time` is measured in seconds.
- callbacks and hooks run in the parent process.
- limit and completion callbacks run only after ProcMan has terminated the
  job's remaining contained descendants.

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
