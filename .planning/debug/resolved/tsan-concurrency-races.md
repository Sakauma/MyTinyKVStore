---
status: resolved
trigger: "TSan reports races in the signal-interruption test, prepared request coordination, and request lifetime during shutdown."
created: 2026-08-31
updated: 2026-08-31
---

# Symptoms

- expected: Release, ASan, UBSan, and TSan tests complete without sanitizer findings.
- actual: TSan reported a signal flag race, a 256-lock deadlock-detector limitation, and 45-53 warnings after deadlock detection was disabled.
- errors: Reports referenced `g_interrupt_observed`, `prepared_`, worker/coordinator synchronization, WAL accounting, and teardown request lifetime.
- timeline: Observed after enabling a usable GCC 10 ThreadSanitizer runtime for the redesigned engine.
- reproduction: Configure a TSan RelWithDebInfo build with `/usr/bin/g++-10`, run CTest, then run the one-second balanced concurrency stress workload.

# Current Focus

- hypothesis: confirmed and resolved
- test: full CTest plus the real concurrency stress workload under TSan
- expecting: no race report, while the incompatible deadlock detector alone remains disabled
- next_action: none
- reasoning_checkpoint: GCC 10 libstdc++ uses `pthread_cond_clockwait` for steady-clock waits, but its libtsan does not intercept that symbol.
- tdd_checkpoint: existing concurrent compaction/stress coverage and the EINTR unit test reproduce the affected paths.

# Evidence

- timestamp: 2026-08-31; TSan directly reported the concurrent read/write of `g_interrupt_observed`.
- timestamp: 2026-08-31; a stress run emitted 45 warnings, including `latest_wal_record_bytes_::clear` versus `account_live_records`.
- timestamp: 2026-08-31; `nm -D libtsan.so.0` showed interceptors for `pthread_cond_wait` and `pthread_cond_timedwait`, but none for `pthread_cond_clockwait`.
- timestamp: 2026-08-31; libstdc++ 10's `<condition_variable>` maps steady-clock `wait_until` and `wait_for` to `pthread_cond_clockwait`.
- timestamp: 2026-08-31; after compatible waits and WAL-accounting serialization, full TSan CTest and the one-second stress test completed with no report.

# Eliminated

- hypothesis: the 256-shard Scan warning proves an engine deadlock; reason: GCC 10's detector aborts after tracking 64 simultaneous locks, and no lock cycle was reported.
- hypothesis: all prepared-map reports are independent engine races; reason: both reported accesses held the same mutex, and the reports disappeared when waits used an intercepted condition-variable path.

# Resolution

- root_cause: one signal-test race, one real compaction/WAL-accounting race, and GCC 10 TSan's missing `pthread_cond_clockwait` interceptor generated the remaining prepared/lifecycle cascade.
- fix: use an async-signal-safe acknowledgement pipe, keep WAL accounting inside `commit_mutex_`, use TSan-compatible timed waits with monotonic deadline checks, make coordinator stop atomic, assign request sequences at successful queue insertion, and force a coordinator wake during teardown.
- verification: Release 2/2, ASan 2/2, UBSan 2/2, TSan 2/2, plus TSan balanced concurrency stress passed on WSL native ext4.
- files_changed: `src/internal/storage_engine.cpp`, `tests/unit/internal_helpers_test.cpp`, `CMakeLists.txt`, `scripts/tsan.sh`, `README.md`, and `docs/runbook.md`.
