# procman

`procman` provides lightweight process-pool helpers with:

- per-job memory limits
- per-job time limits
- optional worker recycling
- parent-side completion callbacks
- parent-side kill/error hooks

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

- `limit_mem` is measured in MB of RSS.
- `limit_time` is measured in seconds.
- callbacks and hooks run in the parent process.
