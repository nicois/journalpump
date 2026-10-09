"""Generates geoip_test.mmdb (a real MaxMind City-format database via
mmdb_writer) and geoip_expectations.json — the expected ip_to_geohash tag
values computed with the REAL journalpump.geohash encoder over the
fixture's coordinates.

The fixture covers every ip_to_geohash branch of journalpump.py:488-504:
a located IP, a record WITHOUT location (Python: `loc.latitude` raises
AttributeError mid-run; Go returns ""), present zero coordinates (must
NOT be confused with an absent location — geoip2-golang/v2 gives pointer
fields), an out-of-range longitude (wrap), and an IP missing from the DB
(Python: AddressNotFoundError mid-run; Go returns "").

Run:  python3 testdata/generate_mmdb.py
"""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))

import mmdb_writer as mmdb  # noqa: E402
import netaddr  # noqa: E402

import journalpump.geohash as geohash  # noqa: E402

located = {
    "city": {"names": {"en": "Amsterdam"}},
    "country": {"iso_code": "NL"},
    "location": {"latitude": 52.3759, "longitude": 4.8975},
}
no_location = {"country": {"iso_code": "NL"}}
zero_location = {"location": {"latitude": 0.0, "longitude": 0.0}}
edge_location = {"location": {"latitude": 89.5, "longitude": -179.99}}
wrapping_location = {"location": {"latitude": 52.0, "longitude": 200.0}}

RAW_NETWORKS = [
    ("10.0.0.0/24", located),
    ("10.1.0.0/24", no_location),
    ("10.2.0.0/24", zero_location),
    ("10.3.0.0/24", edge_location),
    ("10.4.0.0/24", wrapping_location),
]
NETWORKS = [(netaddr.IPSet([netaddr.IPNetwork(net)]), content) for net, content in RAW_NETWORKS]

CASES = [
    # (ip, args, expected) — args as Python's split(",") produces them.
    ("10.0.0.7", ["ip_tag", "5"], None),
    ("10.0.0.7", ["ip_tag", "8"], None),
    ("10.0.0.7", ["ip_tag", "12"], None),
    ("10.0.0.7", ["ip_tag"], None),  # default precision 8
    ("10.1.0.9", ["ip_tag", "8"], ""),  # no location in the record
    ("10.2.0.4", ["ip_tag", "8"], None),  # PRESENT zero coordinates
    ("10.3.0.1", ["ip_tag", "6"], None),
    ("10.4.0.2", ["ip_tag", "6"], None),  # longitude 200 wraps
    ("10.9.9.9", ["ip_tag", "8"], ""),  # not in the database
    ("not-an-ip", ["ip_tag", "8"], ""),  # invalid IP string
    ("", ["ip_tag", "8"], ""),  # empty IP string
]


class PinnedEpochWriter(mmdb.MMDBWriter):
    """MMDBWriter._build_meta stamps int(time.time()); overriding it pins
    the metadata epoch so regeneration is byte-reproducible."""

    BUILD_EPOCH = 1700000000

    def __init__(self) -> None:
        super().__init__(
            ip_version=4,
            database_type="GeoLite2-City",
            languages=["en"],
            description={"en": "journalpump Go-port test database"},
        )

    def _build_meta(self) -> dict:
        meta = super()._build_meta()
        meta["build_epoch"] = self.BUILD_EPOCH
        return meta


def main() -> None:
    writer = PinnedEpochWriter()
    for network, content in NETWORKS:
        writer.insert_network(network, content)
    out = pathlib.Path(__file__).parent
    writer.to_db_file(str(out / "geoip_test.mmdb"))

    cases = []
    for ip, args, expected in CASES:
        if expected is None:
            expected = compute(ip, args)
        cases.append({"ip": ip, "args": args, "expected": expected})
    (out / "geoip_expectations.json").write_text(
        json.dumps(cases, indent=1) + "\n", encoding="utf-8"
    )
    print(f"wrote {out/'geoip_test.mmdb'} and {out/'geoip_expectations.json'}")


def compute(ip: str, args: list[str]) -> str:
    """Python reference: journalpump.py:488-504 ip_to_geohash."""
    for network, content in NETWORKS:
        if netaddr.IPAddress(ip) in network:
            break
    else:
        # Python: self.geoip.city(ip) raises AddressNotFoundError; the Go
        # port returns "" (sanctioned crash-free tolerance).
        return ""
    if "location" not in content:
        # Python: `loc.latitude` raises AttributeError when the record has
        # no location; the Go port returns "".
        return ""
    lat = content["location"]["latitude"]
    lon = content["location"]["longitude"]
    precision = int(args[1]) if len(args) > 1 else 8
    return geohash.encode(lat, lon, precision)


if __name__ == "__main__":
    main()