// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

// Package geohash is a pure port of journalpump/geohash.py (Hiroaki
// Kawai's geohash library): latitude/longitude -> geohash base32 string.
// Per spec §4.1/§4.4 this package contains ONLY the encoder — the GeoIP
// database loading and the ip_to_geohash search-tag callable live
// elsewhere (internal/geoip).
//
// The port keeps Python's exact arithmetic: the fixed-point conversion
// runs on the IEEE-754 mantissa/exponent like Python's float.hex()
// trick, with big integers standing in for Python's arbitrary-precision
// ints. Outputs are byte-identical to Python's geohash.encode (locked by
// Python-generated golden vectors in geohash_golden.json).
package geohash

import (
	"errors"
	"math"
	"math/big"
)

// base32 mirrors geohash.py:_base32 and its reverse map.
var base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

// errInvalidLatitude mirrors Python's ValueError("invalid latitude.").
var errInvalidLatitude = errors.New("invalid latitude.") //nolint:staticcheck,revive // error-strings: mirrors Python's exact ValueError("invalid latitude.") text

// floatHexToInt ports geohash.py:_float_hex_to_int for f in [-1, 1).
// Python computes it from float.hex()'s "0x1.<13 hex digits>p<exp>"
// rendering; for a normal double that is exactly the 52-bit mantissa and
// the unbiased exponent, so:
//
//	half_len = 52 - exp
//	r = (1 << half_len) ± ((1 << 52) + mantissa)   (+ for f > 0, - for f < 0)
//
// The second result is half_len + 1. The ok=false result covers the
// callers' out-of-range inputs; a subnormal f (unreachable through
// encode(), where latitude/90 either underflows to 0.0 or is normal)
// makes Python's assert fire — Go reports it as not-ok instead of
// crashing (sanctioned crash-free tolerance).
func floatHexToInt(f float64) (r *big.Int, bits int, ok bool) {
	if f < -1.0 || f >= 1.0 {
		return nil, 0, false
	}
	if f == 0.0 {
		return big.NewInt(1), 1, true
	}
	structBits := math.Float64bits(f)
	mantissa := structBits & (1<<52 - 1)
	biased := int64(structBits >> 52 & 0x7FF)
	if biased == 0 {
		// Subnormal: Python's float.hex has no "0x1." here and its assert
		// raises AssertionError. See the doc comment.
		return nil, 0, false
	}
	exp := biased - 1023
	// half_len = len(frac)*4 - exp (13 hex digits = 52 bits); the sign of
	// f only decides whether the mantissa term is added or subtracted
	// (Python's `if x == 0` branch, x being the "-0x1." prefix offset).
	halfLen := int64(52) - exp
	mantissaInt := new(big.Int).SetUint64(1<<52 | mantissa) // (1 << 52) + mantissa
	r = new(big.Int).SetUint64(1)
	r.Lsh(r, uint(halfLen))
	if structBits>>63 == 1 {
		r.Sub(r, mantissaInt)
	} else {
		r.Add(r, mantissaInt)
	}
	return r, int(halfLen + 1), true
}

// encodeI2C ports geohash.py:_encode_i2c: interleaves the fixed-point
// latitude/longitude integers bit by bit into a base32 string.
func encodeI2C(lat, lon *big.Int, latLength, lonLength int) string {
	precision := (latLength + lonLength) / 5
	a, b := lat, lon
	if latLength < lonLength {
		a, b = lon, lat
	}
	boost := [8]int{0, 1, 4, 5, 16, 17, 20, 21}
	ret := make([]byte, precision)
	mask7 := big.NewInt(7)
	mask3 := big.NewInt(3)
	aMod := new(big.Int)
	bMod := new(big.Int)
	for i := 0; i < precision; i++ {
		aMod.And(a, mask7)
		bMod.And(b, mask3)
		ret[i] = base32[(boost[aMod.Int64()]+(boost[bMod.Int64()]<<1))&0x1F]
		// Python: t = a >> 3; a = b >> 2; b = t — fresh big.Ints per
		// iteration; reusing scratch values aliases a/b (they are the
		// previous iteration's temporaries).
		a, b = new(big.Int).Rsh(b, 2), new(big.Int).Rsh(a, 3)
	}
	// Python returns ret[::-1].
	for l, r := 0, len(ret)-1; l < r; l, r = l+1, r-1 {
		ret[l], ret[r] = ret[r], ret[l]
	}
	return string(ret)
}

// Encode ports geohash.py:encode: latitude/longitude to a geohash string
// of `precision` characters. latitude >= 90 or < -90 raises (Python
// ValueError "invalid latitude."); longitude is wrapped into
// [-180, 180) with Python's subtract-360 loop (identical float results;
// absurd magnitudes loop in Python too, and the GeoIP-driven callers
// cannot produce them).
func Encode(latitude, longitude float64, precision int) (string, error) {
	if latitude >= 90.0 || latitude < -90.0 {
		return "", errInvalidLatitude
	}
	for longitude < -180.0 {
		longitude += 360.0
	}
	for longitude >= 180.0 {
		longitude -= 360.0
	}

	xprecision := precision + 1
	latLength := xprecision * 5 / 2
	lonLength := latLength
	if xprecision%2 == 1 {
		lonLength++
	}

	a, aBits, ok := floatHexToInt(latitude / 90.0)
	if !ok {
		// Python: AssertionError on the subnormal assert; unreachable for
		// normal latitudes (latitude/90 underflows to 0.0 below ~1e-321).
		return "", errors.New("invalid latitude.") //nolint:staticcheck,revive // error-strings: Python-parity message
	}
	o, oBits, ok := floatHexToInt(longitude / 180.0)
	if !ok {
		return "", errors.New("invalid longitude.") //nolint:staticcheck,revive // error-strings: Python-parity message family
	}

	ai := new(big.Int)
	if aBits > latLength {
		ai.Rsh(a, uint(aBits-latLength))
	} else {
		ai.Lsh(a, uint(latLength-aBits))
	}
	oi := new(big.Int)
	if oBits > lonLength {
		oi.Rsh(o, uint(oBits-lonLength))
	} else {
		oi.Lsh(o, uint(lonLength-oBits))
	}

	return pythonPrefix(encodeI2C(ai, oi, latLength, lonLength), precision), nil
}

// pythonPrefix applies Python's ret[:precision] slicing, including the
// negative-index semantics ([s[:-1]] drops the last character) that
// Python's slice accepts and Go's slicing does not.
func pythonPrefix(s string, precision int) string {
	if precision >= len(s) {
		return s
	}
	if precision < 0 {
		precision += len(s)
		if precision < 0 {
			precision = 0
		}
	}
	if precision > len(s) {
		return s
	}
	return s[:precision]
}
