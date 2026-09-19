# procman TODO

## Current aggregate-memory admission handoff

- [x] Design and implement aggregate-memory-aware admission for persistent
  process pools while retaining per-job process isolation, cancellation,
  worker-death handling, cleanup, and complete outcome delivery. Admission
  must delay competing heavy work rather than introduce a new input, analysis,
  or output ceiling.
- [x] Cover the ALES failure shape where four workers in a 4 GiB container
  caused four independently valid FLOSS jobs to be killed, while two workers
  completed all 42 FLOSS results. Preserve four-way concurrency for light jobs
  when aggregate headroom permits and define portable behavior when cgroup or
  host memory telemetry is unavailable.
- [x] Add deterministic admission/backpressure, shutdown, cancellation, and
  mixed-light/heavy tests plus a runnable benchmark.
- [x] Publish immutable aggregate-admission revision
  `13986532f912160b7419d28fdccc56b7c7bff848` for ALES pinning and restoration
  testing.
- [x] Cover a container-wide OOM/worker-loss race in which the containment unit
  can disappear before final accounting. Preserve the primary memory-kill
  outcome, emit a structured accounting-unavailable
  diagnostic, clean up and replace the worker, and never deadlock or report a
  successful job. ALES observed this with the older pinned revision during a
  two-worker, 4 GiB broad-corpus run; the aggregate-admission revision must
  prevent competing heavy work when telemetry is available and degrade
  explicitly when it is not.
- [x] Add a bounded nonblocking submission API for admission-controlled
  persistent pools. Accept a job without blocking its producer until memory and
  a worker are available, preserve deterministic backpressure and cancellation,
  and keep callback/outcome, worker replacement, and shutdown semantics
  identical to `apply()`. Admission must be fair without forcing independent
  light jobs to wait behind an entire heavy-job backlog or allowing heavy jobs
  to starve. Retain the blocking `apply()` contract for compatibility and add
  generated mixed-wave tests.
- [x] Account for memory retained by idle persistent workers when admitting the
  next heavy job, or provide a per-job retirement hint that replaces a worker
  after high-retention work without imposing global one-task worker churn. In an
  exact ALES 4 GiB run, two 2,048 MiB-reserved Capa jobs used one heavy CPU phase
  at a time, but allocations retained by the first idle worker drove the second
  phase to a 4,221,784,064-byte cgroup peak (only 73,183,232 bytes below
  `memory.max`). Add deterministic retained-RSS tests and preserve cached-worker
  efficiency for measured-light jobs, complete callbacks, cleanup, and the rule
  that admission never rejects an individually valid job.
- [ ] Publish the OOM-loss, nonblocking/fair-admission, and per-job retirement
  work as an immutable revision for ALES to pin and validate against the
  425-child large-JAR lifecycle and retained-RSS heavy-job sequence.
