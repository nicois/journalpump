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
	stop, err := startProfiling()
	if err != nil {
		t.Fatalf("inert hook returned an error: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("inert stop returned an error: %v", err)
	}
}

func TestStartProfilingWritesBothProfiles(t *testing.T) {
	dir := t.TempDir()
	cpu := filepath.Join(dir, "cpu.prof")
	mem := filepath.Join(dir, "mem.prof")
	t.Setenv("JOURNALPUMP_CPU_PROFILE", cpu)
	t.Setenv("JOURNALPUMP_MEM_PROFILE", mem)

	stop, err := startProfiling()
	if err != nil {
		t.Fatalf("startProfiling: %v", err)
	}
	sink := 0
	for i := 0; i < 2_000_000; i++ { // burn a little CPU so the profile has samples
		sink += i * i
	}
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
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

// A profiling run that cannot create the requested profile must report it,
// not silently produce nothing and let the harness treat the stale absence
// as success (the PGO path copies the profile into the build).
func TestStartProfilingFailsWhenTheProfileCannotBeCreated(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "no-such-dir", "cpu.prof")
	t.Setenv("JOURNALPUMP_CPU_PROFILE", missing)
	t.Setenv("JOURNALPUMP_MEM_PROFILE", "")

	if _, err := startProfiling(); err == nil {
		t.Fatal("startProfiling succeeded with an unwritable cpu profile path")
	}
}

func TestStartProfilingStopFailsWhenTheHeapProfileCannotBeWritten(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("JOURNALPUMP_CPU_PROFILE", "")
	t.Setenv("JOURNALPUMP_MEM_PROFILE", filepath.Join(dir, "no-such-dir", "mem.prof"))

	stop, err := startProfiling()
	if err != nil {
		t.Fatalf("startProfiling: %v", err)
	}
	if err := stop(); err == nil {
		t.Fatal("stop succeeded with an unwritable heap profile path")
	}
}
