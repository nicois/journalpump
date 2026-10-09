// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package geohash_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Aiven-Open/journalpump/internal/geohash"
)

// goldenCase is one row of geohash_golden.json, produced by running the
// REAL journalpump.geohash.encode (see testdata/generate_golden.py).
type goldenCase struct {
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Precision int     `json:"precision"`
	Geohash   string  `json:"geohash,omitempty"`
	Error     string  `json:"error,omitempty"`
}

func loadGolden(t *testing.T) []goldenCase {
	t.Helper()
	data, err := os.ReadFile("testdata/geohash_golden.json")
	if err != nil {
		t.Fatalf("read golden file: %v", err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("parse golden file: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("empty golden file")
	}
	return cases
}

// TestEncodeGolden locks the encoder byte-for-byte against Python's
// geohash.encode over the generated battery.
func TestEncodeGolden(t *testing.T) {
	t.Parallel()
	for _, tc := range loadGolden(t) {
		tc := tc
		t.Run("", func(t *testing.T) {
			t.Parallel()
			got, err := geohash.Encode(tc.Lat, tc.Lon, tc.Precision)
			if tc.Error != "" {
				if err == nil {
					t.Fatalf("lat=%v lon=%v precision=%d: want error %q, got %q", tc.Lat, tc.Lon, tc.Precision, tc.Error, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("lat=%v lon=%v precision=%d: %v", tc.Lat, tc.Lon, tc.Precision, err)
			}
			if got != tc.Geohash {
				t.Errorf("lat=%v lon=%v precision=%d: got %q want %q", tc.Lat, tc.Lon, tc.Precision, got, tc.Geohash)
			}
		})
	}
}

// TestEncodeLatitudeBounds locks the spec'd raise semantics: lat >= 90
// and lat < -90 error with Python's "invalid latitude." message, the
// boundary-inclusive set matching geohash.py:encode.
func TestEncodeLatitudeBounds(t *testing.T) {
	t.Parallel()
	for _, lat := range []float64{90.0, 90.5, -90.5, 1e9} {
		if _, err := geohash.Encode(lat, 0.0, 8); err == nil || err.Error() != "invalid latitude." {
			t.Errorf("lat=%v: want 'invalid latitude.' error, got %v", lat, err)
		}
	}
	// Exactly -90 is valid: Python raises for latitude >= 90 or
	// latitude < -90 (the lower bound is open).
	for _, lat := range []float64{-90.0, -89.999999} {
		if _, err := geohash.Encode(lat, 0.0, 8); err != nil {
			t.Errorf("lat=%v must be valid: %v", lat, err)
		}
	}
}

// TestEncodeLongitudeWrap locks the [-180, 180) wrapping.
func TestEncodeLongitudeWrap(t *testing.T) {
	t.Parallel()
	base, err := geohash.Encode(52.3759, 0.0, 9)
	if err != nil {
		t.Fatal(err)
	}
	// Multiples of 360 wrap to longitude 0.
	for _, lon := range []float64{360.0, 720.0, -360.0, -720.0} {
		got, err := geohash.Encode(52.3759, lon, 9)
		if err != nil {
			t.Fatalf("lon=%v: %v", lon, err)
		}
		if got != base {
			t.Errorf("lon=%v wrapped geohash = %q want %q", lon, got, base)
		}
	}
	// [180, 360) wraps to [180-360, ...) = [-180, 0): 180, -180, 540 and
	// -540 all land on longitude -180.
	neg180, err := geohash.Encode(52.3759, -180.0, 9)
	if err != nil {
		t.Fatal(err)
	}
	for _, lon := range []float64{180.0, 540.0, -540.0} {
		got, err := geohash.Encode(52.3759, lon, 9)
		if err != nil {
			t.Fatalf("lon=%v: %v", lon, err)
		}
		if got != neg180 {
			t.Errorf("lon=%v wrapped geohash = %q want %q", lon, got, neg180)
		}
	}
}
