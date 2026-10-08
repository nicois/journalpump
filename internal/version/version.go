// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package version exposes the journalpump binary version.
//
// The value is set at build time by the Makefile target `build-go` via
//
//	-ldflags "-X github.com/Aiven-Open/journalpump/internal/version.Version=$(VERSION)"
//
// where VERSION comes from `git describe --tags` with an env override,
// mirroring the Python Makefile's short_ver/long_ver logic. The version is
// the source of the `User-Agent: journalpump/{version}` header (see the
// websocket sender) and of general version reporting.
package version

// Version is the long version string of the journalpump binary, e.g.
// "v2.1.3-14-gabcdef0". Overridden at build time via -ldflags.
var Version = "unknown"
