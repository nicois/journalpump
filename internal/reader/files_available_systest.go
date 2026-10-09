// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

//go:build systest

package reader

import "os"

// Test-build shim, the Go analogue of the Python systest harness's
// sitecustomize patch of has_persistent_files/has_runtime_files: the
// fixture journals live outside the persistent journal directory, so
// libsystemd reports no persistent/runtime files and the reader would
// retry forever. When the systest binary is built with -tags systest
// AND JOURNALPUMP_FILES_AVAILABLE=1 is set, the files-available gate is
// forced open. The production binary (no systest tag) never compiles
// this file, so FilesAvailableOverride stays nil there.
func init() {
	if os.Getenv("JOURNALPUMP_FILES_AVAILABLE") == "1" {
		FilesAvailableOverride = func(bool, bool) bool { return true }
	}
}
