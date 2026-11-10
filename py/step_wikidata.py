"""Propose Wikidata candidates for FACEM places and periods (network, own verb).

    python main.py wikidata            ask Wikidata, merge into data/curated/wikidata_candidates.csv
    python py/step_wikidata.py --probe one request, nothing is written

Reads the parsed JSON (data/derived/json/), so run 'python main.py' first.
The terms come from the data, not from a list in this file:

  * places   the labels of the map points ('Oudna / Uthina' -> 'Oudna', 'Uthina'),
             with the FACEM coordinates for the distance check
  * periods  the period at the start of 'chronology of object'
             ('Roman-Late Antiquity ...' -> 'Roman-Late Antiquity', 'Roman', 'Late Antiquity')

For each term Wikidata is asked (wbsearchentities), then the hits are read in one go
(wbgetentities): coordinates (P625), GeoNames ID (P1566), OSM relation ID (P402), instance of (P31).
GeoNames and OSM are not searched, only taken from Wikidata where Wikidata has them.

The result is data/curated/wikidata_candidates.csv. You decide: fill `confirmed` with
ja, nein or offen (empty = offen). A rerun keeps your `confirmed` and `note` cells and never
drops a row you confirmed. Nothing is taken over without 'ja' (S6 reads only those rows).
"""

import csv
import json
import math
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from facem_utils import DERIVED_JSON, ROOT, read_json

# =============================================================================
#  CONFIG
# =============================================================================
API = "https://www.wikidata.org/w/api.php"
USER_AGENT = ("facem2lod-wikidata/0.4 (research; "
              "https://github.com/Research-Squirrel-Engineers/facem2lod)")
LANGUAGE = "en"
HITS_PER_TERM = 7
DELAY = 1.0                 # seconds between requests
MAXLAG = 5
TIMEOUT = 30
OUT_CSV = ROOT / "data" / "curated" / "wikidata_candidates.csv"

# Extra search words for period parts that Wikidata names differently. Keys are the parts
# as they appear in FACEM; the part itself is always searched too.
PERIOD_EXTRA_QUERIES = {
    "Roman": ["Roman Empire", "ancient Rome"],
    "Early Imperial": ["Principate", "early Roman Empire"],
}
# =============================================================================

COLUMNS = ["kind", "source_label", "query", "qid", "label", "description", "lat", "long",
           "distance_km", "geonames_id", "osm_relation_id", "instance_of", "rank",
           "confirmed", "note"]
KEY = ("kind", "source_label", "qid")


# ---------------------------------------------------------------- terms (offline)

def split_place_names(label: str) -> list:
    return [p.strip() for p in label.split("/") if p.strip()]


def period_of(chronology: str):
    """'Roman-Late Antiquity c. 530-580 CE' -> 'Roman-Late Antiquity' (None if no match).

    The period is the capitalised words after 'Roman-' up to the first lower-case word or digit.
    """
    m = re.match(r"^(Roman-[A-Z][A-Za-z]*(?: [A-Z][A-Za-z]*)*)(?:\s|$)", chronology.strip())
    return m.group(1) if m else None


def collect_terms(pages: list) -> tuple:
    """(places, periods): places {label: (lat, long)}, periods {label: n_pages}."""
    places, periods = {}, {}
    for page in pages:
        for p in page.get("map_points", []):
            places.setdefault(p["label"], (p["lat"], p["long"]))
        for s in page.get("sections", []):
            for f in s["fields"]:
                if f["key"] == "chronology of object" and f.get("value"):
                    per = period_of(f["value"])
                    if per:
                        periods[per] = periods.get(per, 0) + 1
    return places, periods


def queries_for_place(label: str) -> list:
    return split_place_names(label)


def queries_for_period(label: str) -> list:
    parts = [p for p in label.split("-") if p]
    out = [label.replace("-", " ")] + parts
    for part in parts:
        out += PERIOD_EXTRA_QUERIES.get(part, [])
    seen, uniq = set(), []
    for q in out:
        if q.lower() not in seen:
            seen.add(q.lower())
            uniq.append(q)
    return uniq


# ---------------------------------------------------------------- Wikidata reading

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def claim_values(entity: dict, prop: str) -> list:
    out = []
    for c in entity.get("claims", {}).get(prop, []):
        if c.get("rank") == "deprecated":
            continue
        snak = c.get("mainsnak", {})
        if snak.get("snaktype") != "value":
            continue
        out.append(snak["datavalue"]["value"])
    return out


def read_entity(entity: dict) -> dict:
    coords = claim_values(entity, "P625")
    lat = coords[0]["latitude"] if coords else None
    lon = coords[0]["longitude"] if coords else None
    labels = entity.get("labels", {})
    descs = entity.get("descriptions", {})
    return {
        "qid": entity.get("id", ""),
        "label": labels.get(LANGUAGE, {}).get("value", ""),
        "description": descs.get(LANGUAGE, {}).get("value", ""),
        "lat": lat, "long": lon,
        "geonames_id": ";".join(str(v) for v in claim_values(entity, "P1566")),
        "osm_relation_id": ";".join(str(v) for v in claim_values(entity, "P402")),
        "instance_of": ";".join(v["id"] for v in claim_values(entity, "P31")),
    }


def make_row(kind, source_label, query, info, origin=None) -> dict:
    row = {c: "" for c in COLUMNS}
    row.update(kind=kind, source_label=source_label, query=query, qid=info["qid"],
               label=info["label"], description=info["description"],
               geonames_id=info["geonames_id"], osm_relation_id=info["osm_relation_id"],
               instance_of=info["instance_of"])
    if info["lat"] is not None:
        row["lat"], row["long"] = f"{info['lat']:.5f}", f"{info['long']:.5f}"
        if origin:
            row["distance_km"] = f"{haversine_km(origin[0], origin[1], info['lat'], info['long']):.2f}"
    return row


def rank_rows(rows: list) -> None:
    """Fills `rank` per (kind, source_label): places by distance (no coordinates last),
    periods by exact label match first, then by how many queries found the item."""
    groups = {}
    for r in rows:
        groups.setdefault((r["kind"], r["source_label"]), []).append(r)
    for (kind, source), grp in groups.items():
        if kind == "place":
            names = [n.lower() for n in split_place_names(source)]

            def sort_key(r):
                d = float(r["distance_km"]) if r["distance_km"] else 1e9
                exact = 0 if r["label"].lower() in names else 1
                return (d, exact, r["qid"])
        else:
            want = source.replace("-", " ").lower()

            def sort_key(r):
                exact = 0 if r["label"].lower() == want else 1
                return (exact, r["qid"])
        for i, r in enumerate(sorted(grp, key=sort_key), 1):
            r["rank"] = str(i)


def merge_rows(new_rows: list, old_rows: list) -> list:
    """Keep `confirmed` and `note` of old rows; keep old rows with a decision even if the
    new search no longer finds them."""
    old = {tuple(r[k] for k in KEY): r for r in old_rows}
    merged, seen = [], set()
    for r in new_rows:
        key = tuple(r[k] for k in KEY)
        if key in seen:
            continue
        seen.add(key)
        if key in old:
            r["confirmed"] = old[key].get("confirmed", "")
            r["note"] = old[key].get("note", "")
        merged.append(r)
    for key, r in old.items():
        if key not in seen and (r.get("confirmed") or r.get("note")):
            merged.append({c: r.get(c, "") for c in COLUMNS})
    rank_rows(merged)
    order = {"place": 0, "period": 1}
    merged.sort(key=lambda r: (order.get(r["kind"], 9), r["source_label"],
                               int(r["rank"] or 999), r["qid"]))
    return merged


def read_csv(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in COLUMNS})


# ---------------------------------------------------------------- network

class WikidataError(Exception):
    pass


def api_get(params: dict) -> dict:
    q = dict(params, format="json", maxlag=MAXLAG)
    url = API + "?" + urllib.parse.urlencode(q)
    for attempt in range(4):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 503):
                wait = int(e.headers.get("Retry-After", "5") or 5)
                time.sleep(min(wait, 60))
                continue
            raise WikidataError(f"HTTP {e.code} for {params.get('action')}")
        except urllib.error.URLError as e:
            raise WikidataError(f"cannot reach Wikidata: {e.reason}")
        if data.get("error", {}).get("code") == "maxlag":
            time.sleep(5)
            continue
        if "error" in data:
            raise WikidataError(data["error"].get("info", "API error"))
        return data
    raise WikidataError("Wikidata stayed busy (maxlag/429); try again later")


def search(term: str) -> list:
    data = api_get({"action": "wbsearchentities", "search": term, "language": LANGUAGE,
                    "uselang": LANGUAGE, "type": "item", "limit": HITS_PER_TERM})
    return [h["id"] for h in data.get("search", [])]


def fetch_entities(qids: list) -> dict:
    out = {}
    for i in range(0, len(qids), 50):
        batch = qids[i:i + 50]
        data = api_get({"action": "wbgetentities", "ids": "|".join(batch),
                        "props": "labels|descriptions|claims", "languages": LANGUAGE})
        for qid, ent in data.get("entities", {}).items():
            if "missing" not in ent:
                out[qid] = read_entity(ent)
        time.sleep(DELAY)
    return out


# ---------------------------------------------------------------- run

def load_pages() -> list:
    pages = []
    for p in sorted(DERIVED_JSON.glob("*.json")):
        if p.name == "index.json":
            continue
        pages.append(read_json(p))
    return pages


def run(probe: bool = False) -> int:
    pages = load_pages()
    if not pages:
        print("wikidata: no parsed JSON in data/derived/json/ (run 'python main.py' first)")
        return 1
    places, periods = collect_terms(pages)
    print(f"wikidata: {len(places)} places, {len(periods)} periods in the data")

    jobs = []                                   # (kind, source_label, query, origin)
    for label, origin in sorted(places.items()):
        for q in queries_for_place(label):
            jobs.append(("place", label, q, origin))
    for label in sorted(periods):
        for q in queries_for_period(label):
            jobs.append(("period", label, q, None))
    print(f"wikidata: {len(jobs)} searches")

    try:
        if probe:
            print("probe:", jobs[0][2], "->", search(jobs[0][2]))
            return 0
        hits = []
        for kind, label, q, origin in jobs:
            ids = search(q)
            print(f"  {kind:6s} {label} / {q!r}: {len(ids)} hits")
            hits.append((kind, label, q, origin, ids))
            time.sleep(DELAY)
        qids = sorted({i for h in hits for i in h[4]})
        info = fetch_entities(qids)
    except WikidataError as e:
        print(f"wikidata: {e}")
        return 1

    rows = []
    for kind, label, q, origin, ids in hits:
        for qid in ids:
            if qid in info:
                rows.append(make_row(kind, label, q, info[qid], origin))
    merged = merge_rows(rows, read_csv(OUT_CSV))
    write_csv(OUT_CSV, merged)

    open_n = sum(1 for r in merged if not r["confirmed"])
    print(f"wikidata: {len(merged)} candidate rows in {OUT_CSV.relative_to(ROOT)}, "
          f"{open_n} without a decision")
    for (kind, label) in sorted({(r["kind"], r["source_label"]) for r in merged}):
        top = next(r for r in merged if r["kind"] == kind and r["source_label"] == label)
        extra = f", {top['distance_km']} km" if top["distance_km"] else ""
        print(f"  {kind:6s} {label}: first candidate {top['qid']} {top['label']!r}{extra}")
    return 0


if __name__ == "__main__":
    sys.exit(run(probe="--probe" in sys.argv))
