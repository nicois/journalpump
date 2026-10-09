// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package geoip ports the GeoIP half of journalpump.py's enrichment: the
// `geoip_database` .mmdb loading (handle_new_config, journalpump.py:
// 957-965) and the ip_to_geohash search-tag callable (journalpump.py:
// 488-504). Per spec §4.1/§4.4 this lives with the reader/search
// handling, NOT in internal/geohash (which is the pure encoder); the
// pump wiring composes the callable into the searches' tag-function
// registry: `map[string]filter.TagFunc{"ip_to_geohash": db.IPToGeohash}`
// (the signature matches structurally, so geoip need not import filter).
package geoip

import (
	"net/netip"
	"strconv"

	"github.com/oschwald/geoip2-golang/v2"

	"github.com/Aiven-Open/journalpump/internal/geohash"
)

// DB wraps one open MaxMind .mmdb reader. The zero value / nil *DB is
// the "geoip_database not configured" state (Python: self.geoip None).
type DB struct {
	reader *geoip2.Reader
}

// Open loads the .mmdb at path (journalpump.py:957-965: Python builds
// geoip2.database.Reader(path) at config time and a missing/unreadable
// file surfaces there — the Go wiring must do the same).
func Open(path string) (*DB, error) {
	reader, err := geoip2.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{reader: reader}, nil
}

// Close releases the database file handle.
func (db *DB) Close() error {
	if db == nil || db.reader == nil {
		return nil
	}
	return db.reader.Close()
}

// IPToGeohash ports journalpump.py:488-504:
//
//	ip_to_geohash(ip_tag_name, precision) -> geohash of the IP
//
// Every failure mode Python raises mid-run (ValueError on a
// non-integer/missing precision, KeyError on a missing tag, the geoip2
// AddressValueError/AddressNotFoundError family, the AttributeError when
// a record has no location, and ValueError("invalid latitude.")) returns
// "" here — the sanctioned crash-free runtime tolerance (charter §2): a
// malformed value never kills the pump, it degrades to an empty tag.
// Python's `self.geoip is None` short-circuit (empty string when
// geoip_database is not configured) is preserved exactly.
func (db *DB) IPToGeohash(tags map[string]string, args []string) string {
	if db == nil || db.reader == nil {
		// Python: `if self.geoip is None: return ""`.
		return ""
	}
	precision := 8
	if len(args) > 1 {
		// The tag-function regex constrains args to [a-z0-9_,]+, so
		// Atoi's acceptance matches Python int()'s here.
		parsed, err := strconv.Atoi(args[1])
		if err != nil {
			// Python: int(args[1]) raises ValueError mid-run.
			return ""
		}
		precision = parsed
	}
	ip, ok := tags[args[0]]
	if !ok {
		// Python: tags[args[0]] raises KeyError mid-run when the named
		// group did not match.
		return ""
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		// Python: geoip2 raises AddressValueError (a ValueError) for an
		// invalid IP string.
		return ""
	}
	record, err := db.reader.City(addr)
	if err != nil || !record.HasData() {
		// Python: AddressNotFoundError for an IP absent from the
		// database; v2 signals the same with an empty record.
		return ""
	}
	loc := record.Location
	if !loc.HasCoordinates() {
		// Python: `loc.latitude` raises AttributeError when the record
		// carries no location object.
		return ""
	}
	hash, err := geohash.Encode(*loc.Latitude, *loc.Longitude, precision)
	if err != nil {
		// Python: geohash.encode raises ValueError("invalid latitude.")
		// mid-run for out-of-range coordinates.
		return ""
	}
	return hash
}
