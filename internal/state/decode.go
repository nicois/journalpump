// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package state

import (
	"encoding/json"
)

// decodeRawValue consumes one complete JSON value and returns its raw
// bytes (json.Decoder interleaves Token and Decode calls).
func decodeRawValue(dec *json.Decoder) (json.RawMessage, error) {
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// skipValue consumes one complete JSON value, discarding it.
func skipValue(dec *json.Decoder) error {
	_, err := decodeRawValue(dec)
	return err
}
