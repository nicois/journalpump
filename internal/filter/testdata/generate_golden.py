"""Generates glob_match_matrix.json by running the REAL Python fnmatch
over a (glob, candidate) battery. The Go test asserting the file stays
byte-coupled to Python's fnmatch.fnmatchcase semantics.

Run:  python3 testdata/generate_golden.py
"""
import fnmatch
import json
import pathlib

GLOBS = [
    "unit-a*", "systemd*", "*.service", "foo?.service", "[a-c]*", "[!a-c]x",
    "[]]x", "[x", "café*", "a**b", "*a*ab", "unit.service", "a*b*c",
    "*end", "mid*dle*", "?end", "a[-b]", "[]", "[!]", "a**", "**", "*",
    "with space", "with.dot", "with-dash", "[a-]", "[-a]", "[a^b]", "[^a]",
    "[a\\]b", "[0-9]*x", "*[a-z]", "[[:alpha:]]", "é?x", "**/*",
    "[a-c][!x]z",
]
STRINGS = [
    "unit-a-foo.service", "unit-a", "unit", "systemd-journald.service",
    "systemd", "foo.service", "fooX.service", "abc.service", "bbc", "zbc",
    "wxy", "]]x", "]x", "[x", "café", "café-au-lait", "cafx", "aab", "ab",
    "axbx", "axbxc", "unit.service", "unitxservice", "end", "the end",
    "middle", "midXXdleYY", "a-b", "axb", "with space", "with  space",
    "withXspace", "with.dot", "withxdot", "with-dash", "withdash", "a^b",
    "a\\b", "9xx", "x9x", "prefixaz", "az", "hello", "hello\tworld", "éxx",
    "éyx", "x/y/z", "//", "axz", "ayz", "axbz", "", "a", "é", "café^",
    "xxx.service",
]


def main() -> None:
    table = {
        glob: {string: fnmatch.fnmatchcase(string, glob) for string in STRINGS}
        for glob in GLOBS
    }
    out = pathlib.Path(__file__).parent / "glob_match_matrix.json"
    out.write_text(
        json.dumps(table, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
