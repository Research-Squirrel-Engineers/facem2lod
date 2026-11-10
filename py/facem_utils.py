"""Shared constants and helpers for the FACEM pipeline.

Nothing in here touches the network or the clock.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

# The only date that may appear in generated files. Bump it by hand when a
# release is cut; never call datetime.now() in a generator.
RELEASE = "2026-11-10"

BASE_URL = "https://facem.at"

# Decided 2026-11-10 (PRIMER A4); not registered at w3id.org yet.
NS_BASE = "http://w3id.org/facem/"

ROOT = Path(__file__).resolve().parent.parent

DATA_RAW = ROOT / "data" / "raw"
RAW_HTML = DATA_RAW / "html"            # fabric pages, unchanged as obtained
RAW_SITES = RAW_HTML / "sites"          # map/*.php pages, unchanged as obtained
HARVEST_MANIFEST = DATA_RAW / "harvest_manifest.json"

DATA_DERIVED = ROOT / "data" / "derived"
DERIVED_JSON = DATA_DERIVED / "json"    # one parsed JSON per raw page
FIELD_CATALOGUE = DATA_DERIVED / "field_catalogue.json"

DIST = ROOT / "dist"


def ensure_dirs() -> None:
    for p in (RAW_HTML, RAW_SITES, DERIVED_JSON, DIST):
        p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj) -> None:
    """Canonical JSON: sorted keys, UTF-8, LF line endings, trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


def read_json(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


def content_fingerprint(paths) -> str:
    """SHA-256 over the named files, in sorted order (for owl:versionInfo later)."""
    h = hashlib.sha256()
    for p in sorted(Path(x) for x in paths):
        h.update(str(p.name).encode("utf-8"))
        h.update(p.read_bytes())
    return h.hexdigest()
