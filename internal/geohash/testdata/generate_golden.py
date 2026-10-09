"""Generates geohash_golden.json by running the REAL journalpump.geohash
encoder over a (latitude, longitude, precision) battery. The Go test
asserting the file stays byte-coupled to Python's geohash.encode.

Subnormal latitudes make Python's _float_hex_to_int assert fire
(AssertionError); they are recorded as error cases and the Go port
returns an error instead (sanctioned crash-free tolerance).

Run:  python3 testdata/generate_golden.py
"""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))

import journalpump.geohash as geohash  # noqa: E402

LATITUDES = [
    0.0, 52.3759, -33.8688, 45.123, -45.123, 89.999999, -89.999999,
    89.999999999, -89.999999999, 1.0, -1.0, 0.5, -0.5, 1e-6, -1e-6,
    1e-300, -1e-300, 1e-310, 1e-320, 2e-320, 5e-323, 90.0, -90.0, 91.0,
]
LONGITUDES = [
    0.0, 4.8975, -4.8975, 151.2, 179.999999, -179.999999, 180.0, -180.0,
    180.0001, -180.0001, 359.999, 540.0, -540.0, 720.5, 1.0, -1.0,
    1e-300, -1e-300, 123.456789,
]
PRECISIONS = [-1, 0, 1, 2, 5, 6, 8, 9, 12, 20]

# Full (lat, lon, precision) cross product would be huge; keep the
# battery explicit: every latitude with a few longitudes, every
# longitude with a few latitudes, all precisions on the anchors.
PAIRS = []
for lat in LATITUDES:
    for lon in (0.0, 4.8975, 179.999999, 180.0, -180.0001, 540.0):
        PAIRS.append((lat, lon))
for lon in LONGITUDES:
    PAIRS.append((52.3759, lon))
for lat, lon in [(52.3759, 4.8975), (-33.8688, 151.2), (0.0, 0.0)]:
    for precision in PRECISIONS:
        PAIRS.append((lat, lon, precision))
for lat in LATITUDES:
    PAIRS.append((lat, 4.8975, 8))


def main() -> None:
    cases = []
    for case in PAIRS:
        lat, lon = case[0], case[1]
        precision = case[2] if len(case) > 2 else 8
        try:
            result = geohash.encode(lat, lon, precision)
            cases.append({
                "lat": lat, "lon": lon, "precision": precision,
                "geohash": result,
            })
        except Exception as ex:  # noqa: BLE001 — the error IS the expectation
            cases.append({
                "lat": lat, "lon": lon, "precision": precision,
                "error": f"{type(ex).__name__}: {ex}",
            })
    out = pathlib.Path(__file__).parent / "geohash_golden.json"
    out.write_text(
        json.dumps(cases, indent=1) + "\n", encoding="utf-8",
    )
    print(f"wrote {out} ({len(cases)} cases)")


if __name__ == "__main__":
    main()
