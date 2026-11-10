"""Parse the raw FACEM pages into JSON (offline, no network, no clock).

Reads   data/raw/html/*.html and data/raw/html/sites/*.html
Writes  data/derived/json/<id>.json            one file per page
        data/derived/json/index.json           overview of all pages
        data/derived/field_catalogue.json      every section / key / value seen

The parser makes no assumption about which fields exist. Everything it can see
goes into the JSON (tables as sections of key/value rows, headings, links,
images, plain text lines), so the modelling decisions can be taken by looking
at the result, not by guessing the markup.
"""

import re
import sys
from collections import Counter, OrderedDict
from pathlib import Path
from urllib.parse import urljoin

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bs4 import BeautifulSoup

from facem_utils import (
    BASE_URL, RAW_HTML, RAW_SITES, HARVEST_MANIFEST, DERIVED_JSON,
    FIELD_CATALOGUE, ensure_dirs, write_json, read_json,
)

TAB_MARKERS = ["object data", "fabric description", "archaeometry", "petrography"]
RECORD_RE = re.compile(r"^https?://(www\.)?facem\.at/[a-z0-9]{1,8}-[a-z0-9]{1,6}-\d+/?$", re.I)
MAX_DISTINCT = 60          # distinct values kept per (section, key)


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def classify_link(href: str) -> str:
    low = href.lower()
    if low.split("?")[0].endswith(".pdf"):
        return "pdf"
    if "/map/" in low:
        return "site"
    if "/img/" in low or low.split("?")[0].endswith((".jpg", ".jpeg", ".png", ".gif")):
        return "image"
    if RECORD_RE.match(href):
        return "record"
    return "other"


IMAGE_ROLES = [
    ("/img/thin-sections/", "thin_section"),
    ("/img/medium-drawings/", "drawing"),
    ("/img/medium/", "macro"),
    ("/img/photos/", "photo"),
    ("/img/smallpad/", "thumbnail_other_sample"),
    ("/img/pad/", "thumbnail_sample"),
    ("/prototype/images/", "interface"),
    ("/pictures/", "interface"),
]


def image_role(src: str) -> str:
    for part, role in IMAGE_ROLES:
        if part in src:
            return role
    return "other"


def page_kind(page_id: str) -> str:
    """Sample pages have slugs like m-201-2, fabric pages like ou-ars-1."""
    return "sample" if re.fullmatch(r"[a-z]{1,2}-\d+-\d+", page_id) else "fabric"


def card_tab(table):
    """facem.at wraps the four tabs in <div id="card_1".."card_4">."""
    card = table.find_parent("div", id=re.compile(r"^card_\d+$"))
    if card is None:
        return None
    n = int(card["id"].split("_")[1])
    return TAB_MARKERS[n - 1] if 1 <= n <= len(TAB_MARKERS) else None


def section_label(s) -> str:
    return " > ".join(x for x in (s.get("tab"), s["name"]) if x)


def table_rows(table):
    """Rows of this table only (not of nested tables), as lists of cell texts."""
    rows = []
    for tr in table.find_all("tr"):
        if tr.find_parent("table") is not table:
            continue
        cells = [clean(c.get_text(" ")) for c in tr.find_all(["td", "th"], recursive=False)]
        if any(cells):
            rows.append(cells)
    return rows


def rows_to_sections(rows, table_index, tab=None):
    """Group rows into sections. A one-cell row starts a new section."""
    sections, current = [], None

    def new_section(name):
        nonlocal current
        current = {"name": name, "tab": tab, "table": table_index, "fields": []}
        sections.append(current)

    for cells in rows:
        filled = [c for c in cells if c]
        if filled and all(c.lower() in TAB_MARKERS for c in filled):
            continue                      # the tab bar, not data
        # A section header is a row with one filled cell and no key: either a
        # single cell, or (as on facem.at) an empty first cell and the name second.
        if len(filled) == 1 and (len(cells) == 1 or not cells[0]):
            new_section(filled[0])
            continue
        if current is None:
            new_section("")
        key = cells[0]
        rest = filled[1:] if key else filled
        if len(rest) == 1:
            current["fields"].append({"key": key, "value": rest[0]})
        else:
            current["fields"].append({"key": key, "values": rest})
    return [s for s in sections if s["fields"]]


def parse_html(markup, source_url: str, page_id: str, kind: str) -> dict:
    soup = BeautifulSoup(markup, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = clean(soup.title.get_text()) if soup.title else ""

    headings = [
        {"level": int(h.name[1]), "text": clean(h.get_text(" "))}
        for h in soup.find_all(["h1", "h2", "h3", "h4"])
        if clean(h.get_text(" "))
    ]

    lines = []
    for raw in soup.get_text("\n").split("\n"):
        t = clean(raw)
        if t:
            lines.append(t)

    tables, sections = [], []
    for i, table in enumerate(soup.find_all("table")):
        rows = table_rows(table)
        if not rows:
            continue
        tables.append({"index": i, "rows": rows})
        sections.extend(rows_to_sections(rows, i, card_tab(table)))

    dls = []
    for dl in soup.find_all("dl"):
        items, term = [], None
        for el in dl.find_all(["dt", "dd"], recursive=False):
            if el.name == "dt":
                term = clean(el.get_text(" "))
            elif term is not None:
                items.append({"term": term, "definition": clean(el.get_text(" "))})
        if items:
            dls.append(items)

    links, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        full = urljoin(source_url, href)
        key = (full, clean(a.get_text(" ")))
        if key in seen:
            continue
        seen.add(key)
        links.append({"href": full, "text": clean(a.get_text(" ")), "kind": classify_link(full)})

    images, seen_img = [], set()
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        full = urljoin(source_url, src)
        holder = img.find_parent("figure")
        caption = ""
        if holder is not None and holder.find("figcaption"):
            caption = clean(holder.find("figcaption").get_text(" "))
        else:
            cell = img.find_parent(["td", "div"])
            if cell is not None:
                caption = clean(cell.get_text(" "))[:300]
        key = (full, clean(img.get("alt", "")))
        if key in seen_img:
            continue
        seen_img.add(key)
        images.append({"src": full, "alt": clean(img.get("alt", "")), "caption": caption,
                       "role": image_role(full)})

    # Map points: facem.at puts coordinates on <a lat=.. long=..> elements, one per
    # sample; the symbol tells production (red) from discovery (blue) sites.
    points = {}
    for a in soup.find_all("a", attrs={"lat": True, "long": True}):
        symbol = a.get("symbolsrc", "")
        role = ("production" if "symbol-red" in symbol
                else "discovery" if "symbol-blue" in symbol else "unknown")
        key = (role, clean(a.get_text(" ")), a["lat"], a["long"])
        points[key] = points.get(key, 0) + 1
    map_points = [
        {"role": r, "label": lab, "lat": float(lat), "long": float(lon), "n_samples": n}
        for (r, lab, lat, lon), n in sorted(points.items())
    ]

    # Further samples of the same fabric (own pages, e.g. /m-201-2).
    other_samples = []
    for a in soup.find_all("a", class_="sample", href=True):
        pad = a.find(class_="padtitle")
        other_samples.append({
            "id": urljoin(source_url, a["href"]).rstrip("/").rsplit("/", 1)[-1],
            "label": clean(pad.get_text(" ")) if pad else clean(a.get_text(" ")),
            "href": urljoin(source_url, a["href"]),
        })

    low_lines = {l.lower() for l in lines}
    tabs_found = [m for m in TAB_MARKERS if m in low_lines]

    return {
        "id": page_id,
        "kind": kind,
        "source_url": source_url,
        "title": title,
        "breadcrumb": [l for l in lines if "»" in l][:3],
        "headings": headings,
        "tabs_found": tabs_found,
        "sections": sections,
        "tables": tables,
        "definition_lists": dls,
        "links": links,
        "images": images,
        "map_points": map_points,
        "other_samples": other_samples,
        "text_lines": lines,
    }


def source_url_for(rel: str, page_id: str, kind: str, manifest: dict) -> str:
    entry = manifest.get(rel) or {}
    if entry.get("url"):
        return entry["url"]
    return f"{BASE_URL}/{page_id}" if kind in ("fabric", "sample") else f"{BASE_URL}/map/{page_id}"


def run() -> int:
    ensure_dirs()
    manifest = read_json(HARVEST_MANIFEST) if HARVEST_MANIFEST.exists() else {}

    pages = [(p, page_kind(p.stem), f"html/{p.name}") for p in sorted(RAW_HTML.glob("*.html"))]
    pages += [(p, "site", f"html/sites/{p.name}") for p in sorted(RAW_SITES.glob("*.html"))]

    if not pages:
        print("parse: nothing to do (no HTML in data/raw/html/; run 'python main.py harvest')")
        return 0

    index = []
    catalogue_keys = OrderedDict()           # (section, key) -> Counter of values
    section_order = OrderedDict()
    warnings = 0

    for path, kind, rel in pages:
        page_id = path.stem
        url = source_url_for(rel, page_id, kind, manifest)
        record = parse_html(path.read_bytes(), url, page_id, kind)
        out_name = f"sites__{page_id}" if kind == "site" else page_id
        write_json(DERIVED_JSON / f"{out_name}.json", record)

        n_fields = sum(len(s["fields"]) for s in record["sections"])
        index.append({
            "id": page_id, "kind": kind, "title": record["title"],
            "sections": [section_label(s) for s in record["sections"]],
            "n_fields": n_fields, "n_images": len(record["images"]),
            "n_links": len(record["links"]), "tabs_found": record["tabs_found"],
            "n_map_points": len(record["map_points"]),
            "n_other_samples": len(record["other_samples"]),
        })
        if kind in ("fabric", "sample") and not record["sections"]:
            warnings += 1
            print(f"WARN {page_id}: no table sections found (layout changed?)")

        for s in record["sections"]:
            label = section_label(s)
            section_order.setdefault(label, 0)
            section_order[label] += 1
            for f in s["fields"]:
                counter = catalogue_keys.setdefault((label, f["key"]), Counter())
                val = f.get("value")
                if val is None:
                    val = " | ".join(f.get("values", []))
                counter[val] += 1

    catalogue = {"sections": {}, "n_pages": len(pages)}
    for (section, key), counter in catalogue_keys.items():
        sec = catalogue["sections"].setdefault(section, {})
        distinct = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
        sec[key] = {
            "count": sum(counter.values()),
            "n_distinct": len(counter),
            "values": [{"value": v, "count": c} for v, c in distinct[:MAX_DISTINCT]],
        }

    write_json(DERIVED_JSON / "index.json", sorted(index, key=lambda r: (r["kind"], r["id"])))
    write_json(FIELD_CATALOGUE, catalogue)

    n_fabric = sum(1 for r in index if r["kind"] == "fabric")
    n_sample = sum(1 for r in index if r["kind"] == "sample")
    print(f"parse: {n_fabric} fabric pages, {n_sample} sample pages, "
          f"{len(catalogue_keys)} distinct section/key pairs, {warnings} without sections")
    return 0


if __name__ == "__main__":
    sys.exit(run())
