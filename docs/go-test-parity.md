# Go test parity checklist

Tracks every Python unit/system test function (≈127 unit tests across
`test/` and `test/unit/`, plus the systest suites) as one of:

- **ported** — the assertion was ported to a Go test (name the Go test).
- **Go-idiomatic** — replaced by a Go-native equivalent (e.g. `-race` tests
  replacing thread-interleaving tricks, `httptest` replacing `responses`).
- **dropped-with-reason** — Python-internals-only test with no Go
  equivalent; the reason is recorded.

Each task's builder updates its rows; Task 29 (final parity sweep) verifies
completeness. Table state is per migration commit.

## Task 3 — config loader and running-config writer

Source: `test/test_running_config.py` (5 tests), `systest/test_running_config.py` (7 cases).

| Python test | Status | Go counterpart |
|---|---|---|
| test_running_config_path_configured | ported | `TestRunningConfigPath/configured wins over RUNTIME_DIRECTORY` |
| test_running_config_path_runtime_directory | ported | `TestRunningConfigPath/RUNTIME_DIRECTORY fallback` |
| test_running_config_path_configured_wins | ported | same subtest as above (configured path is asserted against a set RUNTIME_DIRECTORY) |
| test_running_config_path_unset | ported | `TestRunningConfigPath/both unset` |
| test_write_running_config_warns_when_path_set_without_enable | ported | `TestWriteRunningConfigSkips/path set without enable warns` (asserts the exact Python warning text) |
| systest: writes to runtime directory | Go-idiomatic (unit level here) | `TestWriteRunningConfigWrites/sorted full document with modes` (content bytes, file 0600, dir 0700) + `TestWriteRunningConfigRuntimeDirectory` (RUNTIME_DIRECTORY fallback); the spawn-based cases land with the harness in plan Task 10 |
| systest: with secret filters | Go-idiomatic (unit level here) | raw-document preservation incl. `secret_filters` covered by `TestDecodeGoldenFullConfig` + `TestDecodeIgnoresUnknownKeys`; spawn-based case in Task 10 |
| systest: no write without opt-in | Go-idiomatic (unit level here) | `TestWriteRunningConfigSkips/not enabled and no path is silent` (no file created) |
| systest: no write from path alone | Go-idiomatic (unit level here) | `TestWriteRunningConfigSkips/path set without enable warns` (no write); spawn-based case in Task 10 |
| systest: no write when enabled without path | Go-idiomatic (unit level here) | `TestWriteRunningConfigSkips/enabled without path warns` |
| systest: writes to configured path (modes 0600/0700) | Go-idiomatic (unit level here) | `TestWriteRunningConfigWrites/sorted full document with modes`; spawn-based case in Task 10 |
| systest: does not chmod existing directory | Go-idiomatic (unit level here) | `TestWriteRunningConfigWrites/existing directory is not chmodded` |

Recorded deviation (user decision, Task 3 review; NEWS in plan Task 28):
the running-config dump is parsed-JSON-equal to Python's
`json.dump(..., sort_keys=True)` but **not byte-identical** — Go renders
floats like 60.0 as "60", 1e-05 as "0.00001", and 1e+20 as
"100000000000000000000" (Python: "60.0", "1e-05", "1e+20"), and emits non-ASCII as raw UTF-8 where Python's
`ensure_ascii=True` writes \uXXXX. All parity assertions above compare
parsed JSON, so none of them depend on the byte-level difference.