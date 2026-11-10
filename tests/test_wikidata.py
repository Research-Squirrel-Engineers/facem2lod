"""Offline tests for step_wikidata.py (no network)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "py"))

import step_wikidata as w


def entity(qid, label, lat=None, lon=None, geonames=None, osm=None):
    claims = {}
    if lat is not None:
        claims["P625"] = [{"rank": "normal", "mainsnak": {
            "snaktype": "value", "datavalue": {"value": {"latitude": lat, "longitude": lon}}}}]
    if geonames:
        claims["P1566"] = [{"rank": "normal", "mainsnak": {
            "snaktype": "value", "datavalue": {"value": geonames}}}]
    if osm:
        claims["P402"] = [{"rank": "deprecated", "mainsnak": {
            "snaktype": "value", "datavalue": {"value": osm}}}]
    claims["P31"] = [{"rank": "normal", "mainsnak": {
        "snaktype": "value", "datavalue": {"value": {"id": "Q839954"}}}}]
    return {"id": qid, "labels": {"en": {"value": label}},
            "descriptions": {"en": {"value": "test"}}, "claims": claims}


class TermTests(unittest.TestCase):
    def test_period_of(self):
        self.assertEqual(w.period_of("Roman-Late Antiquity c. 530–580 CE"), "Roman-Late Antiquity")
        self.assertEqual(w.period_of("Roman-Early Imperial second half of 5th century CE"),
                         "Roman-Early Imperial")
        self.assertEqual(w.period_of("Roman-Late Antiquity (end of 5th?)–6th century AD"),
                         "Roman-Late Antiquity")
        self.assertIsNone(w.period_of("c. 530–580 CE"))

    def test_place_names_split(self):
        self.assertEqual(w.queries_for_place("Oudna / Uthina"), ["Oudna", "Uthina"])
        self.assertEqual(w.queries_for_place("Bordj el Djerbi"), ["Bordj el Djerbi"])

    def test_period_queries_unique_and_include_parts(self):
        q = w.queries_for_period("Roman-Late Antiquity")
        self.assertEqual(q[0], "Roman Late Antiquity")
        self.assertIn("Late Antiquity", q)
        self.assertEqual(len(q), len({x.lower() for x in q}))

    def test_collect_terms(self):
        page = {"map_points": [{"label": "Oudna / Uthina", "lat": 36.6, "long": 10.1}],
                "sections": [{"fields": [{"key": "chronology of object",
                                          "value": "Roman-Late Antiquity c. 530–580 CE"}]}]}
        places, periods = w.collect_terms([page, page])
        self.assertEqual(places, {"Oudna / Uthina": (36.6, 10.1)})
        self.assertEqual(periods, {"Roman-Late Antiquity": 2})


class ReadTests(unittest.TestCase):
    def test_read_entity(self):
        info = w.read_entity(entity("Q1", "Oudna", 36.61, 10.17, geonames="2464026", osm="123"))
        self.assertEqual(info["geonames_id"], "2464026")
        self.assertEqual(info["osm_relation_id"], "")          # deprecated statement ignored
        self.assertEqual(info["instance_of"], "Q839954")
        self.assertAlmostEqual(info["lat"], 36.61)

    def test_haversine(self):
        self.assertAlmostEqual(w.haversine_km(0, 0, 0, 1), 111.19, places=1)
        self.assertEqual(w.haversine_km(36.6, 10.1, 36.6, 10.1), 0.0)

    def test_rank_places_by_distance(self):
        near = w.make_row("place", "Oudna / Uthina", "Oudna", w.read_entity(entity("Q1", "Oudna", 36.61, 10.17)), (36.6075, 10.1723))
        far = w.make_row("place", "Oudna / Uthina", "Oudna", w.read_entity(entity("Q2", "Oudna", 40.0, 10.0)), (36.6075, 10.1723))
        none = w.make_row("place", "Oudna / Uthina", "Oudna", w.read_entity(entity("Q3", "Oudna")), (36.6075, 10.1723))
        rows = [far, none, near]
        w.rank_rows(rows)
        self.assertEqual([r["rank"] for r in (near, far, none)], ["1", "2", "3"])

    def test_rank_periods_exact_label_first(self):
        a = w.make_row("period", "Roman-Late Antiquity", "q", w.read_entity(entity("Q9", "Late Antiquity")))
        b = w.make_row("period", "Roman-Late Antiquity", "q", w.read_entity(entity("Q1", "Roman Late Antiquity")))
        w.rank_rows([a, b])
        self.assertEqual((b["rank"], a["rank"]), ("1", "2"))


class MergeTests(unittest.TestCase):
    def test_merge_keeps_decisions(self):
        new = [w.make_row("place", "Oudna / Uthina", "Oudna", w.read_entity(entity("Q1", "Oudna", 36.61, 10.17)), (36.6075, 10.1723))]
        old = [{c: "" for c in w.COLUMNS} | {"kind": "place", "source_label": "Oudna / Uthina",
                                              "qid": "Q1", "confirmed": "ja", "note": "ok"},
               {c: "" for c in w.COLUMNS} | {"kind": "place", "source_label": "Oudna / Uthina",
                                              "qid": "Q7", "label": "gone", "confirmed": "nein"},
               {c: "" for c in w.COLUMNS} | {"kind": "place", "source_label": "Oudna / Uthina",
                                              "qid": "Q8", "label": "undecided"}]
        merged = w.merge_rows(new, old)
        by = {r["qid"]: r for r in merged}
        self.assertEqual(by["Q1"]["confirmed"], "ja")
        self.assertEqual(by["Q1"]["note"], "ok")
        self.assertIn("Q7", by)                  # decided rows survive
        self.assertNotIn("Q8", by)               # undecided rows that vanished are dropped

    def test_merge_is_stable(self):
        rows = [w.make_row("period", "Roman-Late Antiquity", "q", w.read_entity(entity(f"Q{i}", f"x{i}"))) for i in (3, 1, 2)]
        once = w.merge_rows([dict(r) for r in rows], [])
        twice = w.merge_rows([dict(r) for r in rows], [dict(r) for r in once])
        self.assertEqual(once, twice)


if __name__ == "__main__":
    unittest.main()
