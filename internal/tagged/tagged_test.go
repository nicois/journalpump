// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package tagged_test

import (
	"reflect"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/tagged"
)

// TestTaggedMakeTags ports test/unit/senders/test_base.py:test_tagged.
func TestTaggedMakeTags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{
			name: "plain",
			in:   map[string]string{"foo": "bar"},
			want: map[string]string{"foo": "bar"},
		},
		{
			name: "unsafe name and value characters",
			in:   map[string]string{"foo|wow,": "super!cool"},
			want: map[string]string{"foo_wow": "super!cool"},
		},
		{
			name: "protocol-significant value characters",
			in:   map[string]string{"host": "localhost:1234"},
			want: map[string]string{"host": "localhost_1234"},
		},
		{
			name: "trailing equals sign trimmed",
			in:   map[string]string{"base64": "YmFzZTY0Cg=="},
			want: map[string]string{"base64": "YmFzZTY0Cg"},
		},
		{
			name: "dots kept",
			in:   map[string]string{"source_address": "127.0.0.1"},
			want: map[string]string{"source_address": "127.0.0.1"},
		},
		// Quirk locks verified against CPython (base.py sanitize classes):
		{
			name: "unicode letters are word characters",
			in:   map[string]string{"café": "vâ"},
			want: map[string]string{"café": "vâ"},
		},
		{
			name: "runs of unsafe characters collapse to one underscore",
			in:   map[string]string{"a\nb|c=d:e": "x\ty"},
			want: map[string]string{"a_b_c_d_e": "x_y"},
		},
		{
			name: "leading and trailing underscores stripped",
			in:   map[string]string{"_leading_": "_value_"},
			want: map[string]string{"leading": "value"},
		},
		{
			name: "all underscores sanitize to empty",
			in:   map[string]string{"___": "___"},
			want: map[string]string{"": ""},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			taggedInstance := tagged.New(nil)
			got := taggedInstance.MakeTags(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("MakeTags(%v) = %v want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestTaggedBaseTags ports the base-tags copy semantics of
// base.py:Tagged.__init__ / make_tags.
func TestTaggedBaseTags(t *testing.T) {
	t.Parallel()
	base := map[string]string{"reader": "r"}
	instance := tagged.New(base)
	// Mutating the caller's map must not affect the held tags.
	base["reader"] = "changed"
	got := instance.MakeTags(nil)
	if got["reader"] != "r" {
		t.Errorf("base tags aliased: %v", got)
	}
	// Incoming tags are merged over the base tags.
	got = instance.MakeTags(map[string]string{"foo": "bar"})
	if got["reader"] != "r" || got["foo"] != "bar" {
		t.Errorf("merge failed: %v", got)
	}
	// MakeTags must not mutate the held base set.
	again := instance.MakeTags(nil)
	if _, present := again["foo"]; present {
		t.Errorf("MakeTags leaked incoming tags into the base set: %v", again)
	}
	// ReplaceTags swaps the base set.
	instance.ReplaceTags(map[string]string{"sender": "s"})
	got = instance.MakeTags(nil)
	if _, present := got["reader"]; present {
		t.Errorf("ReplaceTags left the old base set: %v", got)
	}
	if got["sender"] != "s" {
		t.Errorf("ReplaceTags lost the new base set: %v", got)
	}
}
