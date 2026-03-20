"""
facem_pipeline.py
-----------------
Scrapes FACEM fabric-type pages and produces JSON + RDF/Turtle.
Called directly by facem_run.py — do not set URLs here.
All configuration lives in facem_run.py.

Dependency chain:
    facem_ontology.py  →  namespace constants + build_and_save()
    facem_pipeline.py  →  scraping + RDF instance building
    facem_run.py       →  orchestration + all user-facing CONFIG
"""

import re
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD, SKOS, PROV

from facem_ontology import (
    NS_FACEM, NS_FACEMV, NS_CRM, NS_CRMSCI, NS_BFO,
    ONTOLOGY_URI,
)

FACEM  = Namespace(NS_FACEM)
FACEMV = Namespace(NS_FACEMV)
CRM    = Namespace(NS_CRM)
CRMSCI = Namespace(NS_CRMSCI)
BFO    = Namespace(NS_BFO)

HEADERS = {"User-Agent": "facem-rdf-pipeline/0.2 (research; https://github.com/leiza-scit)"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ──────────────────────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────────────────────

def fetch_soup(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


# ──────────────────────────────────────────────────────────────────────────────
# Search-results crawler with automatic pagination
# ──────────────────────────────────────────────────────────────────────────────

FABRIC_URL_RE = re.compile(r"^https://facem\.at/[a-z]{2,8}-[a-z]{1,4}-\d+$")


def extract_fabric_urls(soup: BeautifulSoup) -> list[str]:
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = href if href.startswith("http") else f"https://facem.at{href}"
        if FABRIC_URL_RE.match(full) and full not in urls:
            urls.append(full)
    return urls


def parse_totals(soup: BeautifulSoup) -> tuple[int, int]:
    text = soup.get_text(" ", strip=True)
    m = re.search(r"showing\s+(\d+)\s+of\s+(\d+)\s+fabric", text, re.IGNORECASE)
    return (int(m.group(2)), int(m.group(1))) if m else (0, 0)


def set_page_param(url: str, page: int) -> str:
    base = re.sub(r"[&?]page=\d+", "", url).rstrip("?&")
    sep  = "&" if "?" in base else "?"
    return f"{base}{sep}page={page}"


def crawl_search_results(search_url: str, delay: float = 1.5) -> list[str]:
    all_urls: list[str] = []
    page = 1
    while True:
        paged = set_page_param(search_url, page)
        print(f"    Page {page}: {paged}")
        soup  = fetch_soup(paged)
        total, _ = parse_totals(soup)
        found    = extract_fabric_urls(soup)
        new      = [u for u in found if u not in all_urls]
        all_urls.extend(new)
        print(f"      → {len(new)} new  (total {len(all_urls)}"
              + (f"/{total}" if total else "") + ")")
        if not new or (total and len(all_urls) >= total):
            break
        page += 1
        time.sleep(delay)
    return all_urls


# ──────────────────────────────────────────────────────────────────────────────
# HTML → dict
# ──────────────────────────────────────────────────────────────────────────────

TAB_MARKERS = {"object data", "fabric description", "archaeometry", "petrography"}


def parse_kv_table(table) -> dict:
    data, section = {}, None
    for row in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
        cells = [c for c in cells if c]
        if not cells:               continue
        if len(cells) == 1:         section = cells[0]; continue
        key, val = cells[0], cells[1]
        if not key:                 section = val; continue
        data[f"{section}.{key}" if section else key] = val or None
    return data


def find_content_tables(soup: BeautifulSoup) -> list:
    content = []
    for tbl in soup.find_all("table"):
        texts = {c.get_text(strip=True).lower() for c in tbl.find_all("td")}
        if TAB_MARKERS & texts:
            nxt = tbl.find_next_sibling("table")
            if nxt and nxt not in content:
                content.append(nxt)
    return content


def _clean_sample_number(raw: str | None) -> str | None:
    """Extract 'M 160/2' from 'M 160/2  Star2 representative sample for BNAP-A-11'."""
    if not raw:
        return None
    m = re.match(r"([A-Z]+\s+[\d/]+)", raw.strip())
    return m.group(1).strip() if m else raw.split("  ")[0].strip()


def extract_object_data(t) -> dict:
    r = parse_kv_table(t)
    return {
        "sample_number":             _clean_sample_number(r.get("sample no")),
        "fabric_type":               r.get("fabric type"),
        "production_site":           r.get("supposed site of production"),
        "sample_taken":              r.get("sample taken"),
        "discovery_site":            r.get("Context Data.site of discovery"),
        "context_registration_code": r.get("Context Data.registration code of context"),
        "context_chronology":        r.get("Context Data.chronology of context"),
        "context_interpretation":    r.get("Context Data.interpretation of context"),
        "object_registration_code":  r.get("Context Data.registration code of object"),
        "ware":                      r.get("Object.ware"),
        "object_form":               r.get("Object.object form"),
        "object_type":               r.get("Object.object type"),
        "chronology":                r.get("Object.chronology of object"),
        "fragmentation":             r.get("Object.fragmentation"),
        "surface_description":       r.get("Object.surface description"),
        "decoration":                r.get("Object.decoration"),
        "publication":               r.get("Object.publication"),
    }


def extract_fabric_description(t) -> dict:
    r    = parse_kv_table(t)
    VIS  = "Visual Examination of Fresh Break"
    BINO = "Observation Under the Binocular Stereoscope (all lengths in mm)"
    INCL = "Inclusions (all lengths in mm)"
    return {
        "visual_examination": r.get(f"{VIS}.visual examination of fresh break"),
        "munsell_colour":     r.get(f"{VIS}.colour of fresh break"),
        "texture":            r.get(f"{VIS}.texture of fresh break"),
        "hardness":           r.get(f"{VIS}.hardness"),
        "porosity":           r.get(f"{BINO}.porosity"),
        "inclusions_percent": r.get(f"{BINO}.inclusions"),
        "sorting":            r.get(f"{BINO}.sorting"),
        "inclusions":         {k[len(INCL)+1:]: v for k, v in r.items()
                               if k.startswith(INCL + ".") and v},
    }


def extract_archaeometry(t) -> dict:
    r = parse_kv_table(t)
    return {
        "method":            r.get("applied method for sample analysis"),
        "analyst":           r.get("analysis author"),
        "petrographic_type": r.get("petrographic mineralogical type"),
        "publication":       r.get("publication (of analysis)"),
    }


def extract_petrography(t) -> dict:
    r = parse_kv_table(t)
    return {
        "sample_number":       r.get("sample number"),
        "analyst":             r.get("name of author"),
        "analyst_institution": r.get("institution of author"),
    }


def scrape_facem(url: str) -> dict:
    soup   = fetch_soup(url)
    tables = find_content_tables(soup)

    def safe(fn, idx):
        try:    return fn(tables[idx]) if idx < len(tables) else {}
        except Exception as e: return {"_parse_error": str(e)}

    return {
        "_meta": {
            "source_url": url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "scraper":    "facem_pipeline.py v0.2",
            "tab_count":  len(tables),
        },
        "object_data":        safe(extract_object_data, 0),
        "fabric_description": safe(extract_fabric_description, 1),
        "archaeometry":       safe(extract_archaeometry, 2),
        "petrography":        safe(extract_petrography, 3) if len(tables) >= 4 else {},
    }


# ──────────────────────────────────────────────────────────────────────────────
# RDF builder
# ──────────────────────────────────────────────────────────────────────────────

def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def add_place(g: Graph, name: str) -> URIRef:
    uri = FACEM[f"entity/place_{slug(name)}"]
    g.add((uri, RDF.type,   CRM.E53_Place))
    g.add((uri, RDFS.label, Literal(name, lang="en")))
    return uri


def add_agent(g: Graph, name: str, institution: str | None = None) -> URIRef:
    uri = FACEM[f"entity/agent_{slug(name)}"]
    g.add((uri, RDF.type,   PROV.Person))
    g.add((uri, RDF.type,   CRM.E39_Actor))
    g.add((uri, RDFS.label, Literal(name)))
    if institution:
        inst = FACEM[f"entity/agent_{slug(institution)}"]
        g.add((inst, RDF.type,             PROV.Organization))
        g.add((inst, RDFS.label,           Literal(institution)))
        g.add((uri,  PROV.actedOnBehalfOf, inst))
    return uri


def new_graph() -> Graph:
    g = Graph()
    for p, n in [("crm", CRM), ("crmsci", CRMSCI), ("bfo", BFO), ("prov", PROV),
                 ("skos", SKOS), ("owl", OWL), ("facem", FACEM), ("facemv", FACEMV)]:
        g.bind(p, n, override=True)
    g.add((ONTOLOGY_URI, RDF.type, OWL.Ontology))
    return g


def record_to_rdf(record: dict, g: Graph | None = None) -> Graph:
    if g is None:
        g = new_graph()

    src_url    = record["_meta"]["source_url"]
    scraped_at = record["_meta"]["scraped_at"]
    od = record.get("object_data", {})
    fd = record.get("fabric_description", {})
    ar = record.get("archaeometry", {})
    pg = record.get("petrography", {})

    fabric_id  = od.get("fabric_type") or src_url.rstrip("/").split("/")[-1].upper()
    sample_no  = od.get("sample_number") or fabric_id + "-sample"

    fabric_uri = FACEM[f"entity/fabric_{slug(fabric_id)}"]
    object_uri = FACEM[f"entity/object_{slug(sample_no)}"]
    sample_uri = FACEM[f"entity/sample_{slug(sample_no)}"]
    page_uri   = URIRef(src_url)

    # Fabric type
    g.add((fabric_uri, RDF.type,      FACEMV.FabricType))
    g.add((fabric_uri, RDF.type,      CRM.E57_Material))
    g.add((fabric_uri, RDF.type,      BFO["BFO_0000040"]))
    g.add((fabric_uri, RDFS.label,    Literal(fabric_id)))
    g.add((fabric_uri, SKOS.notation, Literal(fabric_id)))
    g.add((fabric_uri, RDFS.seeAlso,  page_uri))

    # Ceramic object
    g.add((object_uri, RDF.type,             FACEMV.CeramicObject))
    g.add((object_uri, RDF.type,             CRM.E22_Human_Made_Object))
    g.add((object_uri, RDFS.label,           Literal(sample_no)))
    g.add((object_uri, FACEMV.hasFabricType, fabric_uri))
    g.add((object_uri, CRM.P45_consists_of,  fabric_uri))

    for field, pred in [
        ("ware",                      FACEMV.ware),
        ("object_form",               FACEMV.objectForm),
        ("object_type",               FACEMV.objectType),
        ("fragmentation",             FACEMV.fragmentation),
        ("surface_description",       FACEMV.surfaceDescription),
        ("decoration",                FACEMV.decoration),
        ("context_registration_code", FACEMV.contextRegistrationCode),
    ]:
        if od.get(field):
            g.add((object_uri, pred, Literal(od[field])))

    if od.get("object_registration_code"):
        g.add((object_uri, CRM.P1_is_identified_by,
               Literal(od["object_registration_code"])))
    if od.get("publication"):
        g.add((object_uri, CRM["P70i_is_documented_in"], Literal(od["publication"])))

    if od.get("chronology"):
        ts = FACEM[f"entity/timespan_{slug(od['chronology'])}"]
        g.add((ts, RDF.type,   CRM.E52_Time_Span))
        g.add((ts, RDFS.label, Literal(od["chronology"], lang="en")))
        g.add((object_uri, CRM.P4_has_time_span, ts))

    if od.get("context_chronology"):
        ts = FACEM[f"entity/timespan_{slug(od['context_chronology'])}"]
        g.add((ts, RDF.type,   CRM.E52_Time_Span))
        g.add((ts, RDFS.label, Literal(od["context_chronology"], lang="en")))

    if od.get("discovery_site"):
        place = add_place(g, od["discovery_site"])
        ev    = FACEM[f"entity/event_find_{slug(sample_no)}"]
        g.add((ev, RDF.type,                          CRM.E8_Acquisition))
        g.add((ev, RDFS.label,                        Literal(f"Discovery of {sample_no}")))
        g.add((ev, CRM.P26_moved_to,                  place))
        g.add((object_uri, CRM["P12i_was_present_at"], ev))

    if od.get("production_site"):
        place = add_place(g, od["production_site"])
        ev    = FACEM[f"entity/event_production_{slug(fabric_id)}"]
        g.add((ev, RDF.type,                       CRM.E12_Production))
        g.add((ev, RDFS.label,                     Literal(f"Production of {fabric_id}")))
        g.add((ev, CRM.P8_took_place_on_or_within, place))
        g.add((ev, CRM.P108_has_produced,          object_uri))

    # Sample
    g.add((sample_uri, RDF.type,                  FACEMV.CeramicSample))
    g.add((sample_uri, RDF.type,                  CRMSCI.S10_Material_Substantial))
    g.add((sample_uri, RDF.type,                  BFO["BFO_0000040"]))
    g.add((sample_uri, RDFS.label,                Literal(f"Sample {sample_no}")))
    g.add((sample_uri, CRM["P46i_forms_part_of"], object_uri))
    g.add((object_uri, FACEMV.hasSample,          sample_uri))

    if od.get("sample_taken"):
        ev = FACEM[f"entity/event_sampling_{slug(sample_no)}"]
        g.add((ev, RDF.type,              FACEMV.SamplingEvent))
        g.add((ev, RDF.type,              PROV.Activity))
        g.add((ev, RDFS.label,            Literal(f"Sampling of {sample_no}")))
        g.add((ev, SKOS.note,             Literal(od["sample_taken"])))
        g.add((ev, PROV.generated,        sample_uri))
        g.add((sample_uri, PROV.wasGeneratedBy, ev))

    # Visual examination
    if fd.get("visual_examination"):
        vis = FACEM[f"entity/observation_visual_{slug(sample_no)}"]
        g.add((vis, RDF.type,              FACEMV.VisualExamination))
        g.add((vis, RDF.type,              CRMSCI.S4_Observation))
        g.add((vis, RDFS.label,            Literal(f"Visual examination of {sample_no}")))
        g.add((vis, CRM.P3_has_note,       Literal(fd["visual_examination"])))
        g.add((vis, CRMSCI["O8_observed"], sample_uri))
        for field, pred in [("munsell_colour", FACEMV.munsellColour),
                             ("texture",        FACEMV.texture),
                             ("hardness",       FACEMV.hardness)]:
            if fd.get(field):
                g.add((vis, pred, Literal(fd[field])))

    # Binocular observation
    bino = FACEM[f"entity/observation_binocular_{slug(sample_no)}"]
    g.add((bino, RDF.type,              FACEMV.BinocularObservation))
    g.add((bino, RDF.type,              CRMSCI.S4_Observation))
    g.add((bino, RDFS.label,            Literal(f"Binocular stereoscope observation of {sample_no}")))
    g.add((bino, CRMSCI["O8_observed"], sample_uri))

    if fd.get("porosity"):
        m = FACEM[f"entity/measurement_porosity_{slug(sample_no)}"]
        g.add((m, RDF.type,                     FACEMV.PorosityMeasurement))
        g.add((m, RDF.type,                     CRMSCI.S21_Measurement))
        g.add((m, RDFS.label,                   Literal(f"Porosity of {sample_no}")))
        g.add((m, CRMSCI["O16_observed_value"], Literal(fd["porosity"])))
        g.add((m, FACEMV.measuredProperty,      FACEMV.porosity))
        g.add((bino, CRM["P9_consists_of"],     m))

    if fd.get("inclusions_percent"):
        m = FACEM[f"entity/measurement_inclusions_{slug(sample_no)}"]
        g.add((m, RDF.type,                     FACEMV.InclusionsMeasurement))
        g.add((m, RDF.type,                     CRMSCI.S21_Measurement))
        g.add((m, RDFS.label,                   Literal(f"Inclusions % of {sample_no}")))
        g.add((m, CRMSCI["O16_observed_value"], Literal(fd["inclusions_percent"])))
        g.add((m, FACEMV.measuredProperty,      FACEMV.inclusionsPercent))
        g.add((bino, CRM["P9_consists_of"],     m))

    if fd.get("sorting"):
        g.add((bino, FACEMV.sorting, Literal(fd["sorting"])))

    for incl_type, incl_val in fd.get("inclusions", {}).items():
        if incl_val:
            node = BNode()
            g.add((node, RDF.type,              FACEMV.Inclusion))
            g.add((node, FACEMV.inclusionType,  Literal(incl_type)))
            g.add((node, FACEMV.inclusionValue, Literal(incl_val)))
            g.add((bino, FACEMV.hasInclusion,   node))

    # Archaeometry
    if any(ar.values()):
        arch = FACEM[f"entity/analysis_archaeometry_{slug(sample_no)}"]
        g.add((arch, RDF.type,   FACEMV.ArchaeometricAnalysis))
        g.add((arch, RDF.type,   CRM.E13_Attribute_Assignment))
        g.add((arch, RDF.type,   PROV.Activity))
        g.add((arch, RDFS.label, Literal(f"Archaeometric analysis of {sample_no}")))
        g.add((arch, PROV.used,  sample_uri))
        if ar.get("method"):
            g.add((arch, FACEMV.analysisMethod, Literal(ar["method"])))
        if ar.get("analyst"):
            a = add_agent(g, ar["analyst"])
            g.add((arch, PROV.wasAssociatedWith,    a))
            g.add((arch, CRM["P14_carried_out_by"], a))
        if ar.get("petrographic_type"):
            pt = FACEM[f"entity/type_petrographic_{slug(ar['petrographic_type'])}"]
            g.add((pt, RDF.type,            FACEMV.PetrographicType))
            g.add((pt, RDF.type,            CRM.E55_Type))
            g.add((pt, RDFS.label,          Literal(ar["petrographic_type"])))
            g.add((arch, CRM.P141_assigned, pt))
            g.add((fabric_uri, CRM.P2_has_type, pt))
        if ar.get("publication"):
            g.add((arch, CRM["P70i_is_documented_in"], Literal(ar["publication"])))

    # Petrography
    if pg and any(pg.values()):
        petro = FACEM[f"entity/analysis_petrography_{slug(sample_no)}"]
        g.add((petro, RDF.type,   FACEMV.PetrographicAnalysis))
        g.add((petro, RDF.type,   CRMSCI.S4_Observation))
        g.add((petro, RDF.type,   PROV.Activity))
        g.add((petro, RDFS.label, Literal(f"Petrographic analysis of {sample_no}")))
        g.add((petro, PROV.used,  sample_uri))
        name = pg.get("analyst") or ar.get("analyst")
        if name:
            a = add_agent(g, name, pg.get("analyst_institution"))
            g.add((petro, PROV.wasAssociatedWith,    a))
            g.add((petro, CRM["P14_carried_out_by"], a))

    # Graph provenance
    gp = FACEM[f"entity/graph_{slug(fabric_id)}"]
    g.add((gp, RDF.type,             PROV.Entity))
    g.add((gp, PROV.wasDerivedFrom,  page_uri))
    g.add((gp, PROV.generatedAtTime, Literal(scraped_at, datatype=XSD.dateTime)))
    g.add((gp, RDFS.comment,
           Literal("Auto-generated by facem_pipeline.py from FACEM HTML")))

    return g


def run(urls: list[str], out_dir: Path,
        combined: bool = True, json_only: bool = False,
        delay: float = 1.5) -> tuple[list[dict], Graph | None]:
    """
    Scrape a list of URLs and write outputs to out_dir.
    Returns (all_records, combined_graph_or_None).
    Called by facem_run.py.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    all_records: list[dict] = []
    combined_g = new_graph() if (combined and not json_only) else None

    for i, url in enumerate(urls):
        fabric_id = url.rstrip("/").split("/")[-1].upper()
        print(f"  [{i+1:02d}/{len(urls):02d}] {fabric_id} … ", end="", flush=True)
        try:
            record = scrape_facem(url)
            all_records.append(record)
            if not combined:
                (out_dir / f"{fabric_id}.json").write_text(
                    json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
                print("JSON ✓", end="")
                if not json_only:
                    g = record_to_rdf(record)
                    (out_dir / f"{fabric_id}.ttl").write_text(
                        g.serialize(format="turtle"), encoding="utf-8")
                    print(f"  TTL ✓ ({len(g)} triples)", end="")
            else:
                if not json_only and combined_g is not None:
                    record_to_rdf(record, combined_g)
                print("✓", end="")
            print()
        except Exception as e:
            print(f"ERROR: {e}")

        if i < len(urls) - 1:
            time.sleep(delay)

    if combined and all_records:
        jp = out_dir / "facem_records.json"
        jp.write_text(json.dumps(all_records, indent=2, ensure_ascii=False),
                      encoding="utf-8")
        print(f"\n  JSON → {jp}  ({len(all_records)} records)")
        if not json_only and combined_g is not None:
            tp = out_dir / "facem.ttl"
            tp.write_text(combined_g.serialize(format="turtle"), encoding="utf-8")
            print(f"  TTL  → {tp}  ({len(combined_g)} triples)")

    return all_records, combined_g
