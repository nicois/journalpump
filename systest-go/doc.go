// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

// Package systestgo holds the Go system-test suites (plan Task 15):
// rotation, reload, sigterm, running-config, and state-file interop, plus
// the JournalpumpProcess harness. They are excluded from the default
// `go test ./...` run and executed by `make systest-go`.
package systestgo
