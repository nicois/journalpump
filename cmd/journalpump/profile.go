// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package main

import (
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
func startProfiling() func() {
	cpuPath := os.Getenv("JOURNALPUMP_CPU_PROFILE")
	memPath := os.Getenv("JOURNALPUMP_MEM_PROFILE")
	if cpuPath == "" && memPath == "" {
		return func() {}
	}

	var cpuFile *os.File
	if cpuPath != "" {
		if f, err := os.Create(cpuPath); err == nil {
			if err := pprof.StartCPUProfile(f); err == nil {
				cpuFile = f
			} else {
				_ = f.Close()
			}
		}
	}

	return func() {
		if cpuFile != nil {
			pprof.StopCPUProfile()
			_ = cpuFile.Close()
		}
		if memPath != "" {
			if f, err := os.Create(memPath); err == nil {
				runtime.GC() // measure reachable heap, not the uncollected tail
				_ = pprof.WriteHeapProfile(f)
				_ = f.Close()
			}
		}
	}
}
