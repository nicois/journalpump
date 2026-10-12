// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The profiling hook must be completely inert in production: with no env
// vars set it must neither panic nor create files.
func TestStartProfilingIsInertWithoutEnv(t *testing.T) {
	t.Setenv("JOURNALPUMP_CPU_PROFILE", "")
	t.Setenv("JOURNALPUMP_MEM_PROFILE", "")
	stop := startProfiling()
	stop()
}

func TestStartProfilingWritesBothProfiles(t *testing.T) {
	dir := t.TempDir()
	cpu := filepath.Join(dir, "cpu.prof")
	mem := filepath.Join(dir, "mem.prof")
	t.Setenv("JOURNALPUMP_CPU_PROFILE", cpu)
	t.Setenv("JOURNALPUMP_MEM_PROFILE", mem)

	stop := startProfiling()
	sink := 0
	for i := 0; i < 2_000_000; i++ { // burn a little CPU so the profile has samples
		sink += i * i
	}
	stop()
	_ = sink

	for _, path := range []string{cpu, mem} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("profile %s was not written: %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile %s is empty", path)
		}
	}
}
