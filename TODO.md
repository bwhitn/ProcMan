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
- [ ] Publish an immutable revision and provide its exact identity for ALES
  pinning and restoration testing.
