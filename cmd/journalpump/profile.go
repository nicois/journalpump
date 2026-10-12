// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
)

// startProfiling is the ENV-GATED profiling hook used by the performance
// harness (plan Task 31). It is inert in production: unless the environment
// names an output file, it does nothing and the process is unaffected.
//
//	JOURNALPUMP_CPU_PROFILE=/path   start a CPU profile (runtime/pprof)
//	JOURNALPUMP_MEM_PROFILE=/path   write a heap profile on shutdown
//
// The returned stop function flushes both; call it before the process
// exits (the daemon's own shutdown is the natural point). The production
// binary therefore stays clean — no HTTP pprof endpoint is opened, and no
// profile file is produced — while the harness can capture a representative
// profile of the real shipping binary rather than of a special build.
//
// Errors are NOT swallowed. A profiling run that cannot create or flush a
// requested profile returns an error, and the caller exits non-zero, so the
// harness fails loudly instead of profiling nothing and reporting success.
// Without the env vars both return values are nil.
func startProfiling() (stop func() error, err error) {
	cpuPath := os.Getenv("JOURNALPUMP_CPU_PROFILE")
	memPath := os.Getenv("JOURNALPUMP_MEM_PROFILE")
	if cpuPath == "" && memPath == "" {
		return func() error { return nil }, nil
	}

	var cpuFile *os.File
	if cpuPath != "" {
		f, err := os.Create(cpuPath)
		if err != nil {
			return nil, fmt.Errorf("profiling: cpu profile %s: %w", cpuPath, err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("profiling: cpu profile %s: %w", cpuPath, err)
		}
		cpuFile = f
	}

	return func() error {
		var errs []error
		if cpuFile != nil {
			pprof.StopCPUProfile()
			if err := cpuFile.Close(); err != nil {
				errs = append(errs, fmt.Errorf("profiling: cpu profile %s: %w", cpuPath, err))
			}
		}
		if memPath != "" {
			f, err := os.Create(memPath)
			if err != nil {
				errs = append(errs, fmt.Errorf("profiling: heap profile %s: %w", memPath, err))
			} else {
				runtime.GC() // measure reachable heap, not the uncollected tail
				if err := pprof.WriteHeapProfile(f); err != nil {
					errs = append(errs, fmt.Errorf("profiling: heap profile %s: %w", memPath, err))
				}
				if err := f.Close(); err != nil {
					errs = append(errs, fmt.Errorf("profiling: heap profile %s: %w", memPath, err))
				}
			}
		}
		return errors.Join(errs...)
	}, nil
}
