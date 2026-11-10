"""Tests for step_rdf.py: ontology anchors, Turtle writer, instance graph, DCAT graph, SHACL (offline, real data)."""
import sys
import unittest
from pathlib import Path

import rdflib

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "py"))

import facem_model as model
import step_rdf as r
from ttl import Graph, I, L, XSD

CRM, SOSA = model.CRM, model.SOSA
FACEM = model.NS
HAVE_DATA = (r.VOCAB_JSON.exists() and any(r.DERIVED_JSON.glob("*.json")))


class Ontology(unittest.TestCase):
    def test_every_class_reaches_cidoc_crm(self):
        self.assertEqual(model.anchors_ok(), [])

    def test_ontology_reads_back(self):
        g = model.ontology_graph()
        parsed = rdflib.Graph()
        parsed.parse(data=g.serialize(), format="turtle")
        self.assertEqual(len(parsed), len(g))
        classes = set(parsed.subjects(rdflib.RDF.type, rdflib.OWL.Class))
        self.assertEqual(len(classes), len(model.CLASSES))

    def test_properties_have_domain_and_range(self):
        for name, (label, comment, dom, rng, kind, sup) in model.P.items():
            self.assertTrue(dom and rng, name)
            self.assertIn(kind, ("o", "d"), name)


class Writer(unittest.TestCase):
    def test_order_and_escaping(self):
        g = Graph({"ex": "http://e/", "xsd": XSD})
        g.add("http://e/b", "http://e/p", L('a "q"\nb', lang="en"))
        g.add("http://e/a", "http://e/p", L("1", dt=XSD + "decimal"))
        g.add("http://e/a", "http://e/p", L("2.5", dt=XSD + "decimal"))
        g.type("http://e/a", "http://e/C")
        text = g.serialize()
        self.assertLess(text.index("ex:a"), text.index("ex:b"))
        self.assertEqual(text, g.serialize())
        parsed = rdflib.Graph()
        parsed.parse(data=text, format="turtle")
        self.assertEqual(len(parsed), 4)
        values = {str(o.toPython()) for o in parsed.objects(rdflib.URIRef("http://e/a"), rdflib.URIRef("http://e/p"))}
        self.assertEqual(values, {"1", "2.5"})
        self.assertTrue(all(o.datatype == rdflib.XSD.decimal for o in parsed.objects(rdflib.URIRef("http://e/a"), rdflib.URIRef("http://e/p"))))


@unittest.skipUnless(HAVE_DATA, "needs data/derived (run main.py parse and vocab)")
class InstanceGraph(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import re
        from facem_utils import read_json
        pages = [read_json(p) for p in sorted(r.DERIVED_JSON.glob("*.json"))
                 if p.name != "index.json" and re.fullmatch(r"[a-z]{1,2}-(?:ars-)?\d+(?:-\d+)?", p.stem)]
        cls.b = r.Builder()
        cls.b.collect_places(pages)
        for p in pages:
            cls.b.sherd(p)
        cls.b.memberships()
        cls.dcat = cls.b.dcat(pages)
        cls.pages = pages

    def count(self, cls_iri):
        return sum(1 for s, p, o in ((s, p, o) for s, po in self.b.g.t.items() for p, os in po.items() for o in os)
                   if p.endswith("#type") and str(o) == cls_iri)

    def test_one_sherd_and_sample_per_page(self):
        self.assertEqual(self.count(FACEM + "Sherd"), len(self.pages))
        self.assertEqual(self.count(FACEM + "Sample"), len(self.pages))
        self.assertEqual(self.count(FACEM + "SampleTaking"), len(self.pages))
        self.assertEqual(self.count(FACEM + "SourceRecord"), len(self.pages))

    def test_no_warnings_on_the_release_data(self):
        self.assertEqual(self.b.warnings, [])

    def test_backwards_ranges_are_left_out(self):
        self.assertTrue(self.b.notes)
        text = self.b.g.serialize()
        self.assertIn("end smaller than start", text)

    def test_places_and_links(self):
        self.assertEqual(len(self.b.places), 4)
        g = self.b.g.t
        uthina = g[r.ent("place", "Oudna / Uthina")]
        self.assertIn(I(r.WD + "Q269424"), uthina[model.SKOS + "exactMatch"])
        self.assertNotIn(model.SKOS + "exactMatch", g[r.ent("place", "Bordj el Djerbi")])

    def test_two_runs_give_the_same_bytes(self):
        self.assertEqual(self.b.g.serialize(r.HEADER_GRAPH), self.b.g.serialize(r.HEADER_GRAPH))

    def test_every_sherd_belongs_to_a_dataset_of_the_catalogue(self):
        datasets = {s for s, po in self.dcat.t.items()
                    if any(str(o) == model.PREFIXES["dcat"] + "Dataset" for o in po.get(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", []))}
        for s, po in self.b.g.t.items():
            if FACEM + "Sherd" in {str(o) for o in po.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", [])}:
                (ds,) = po[model.DCT + "isPartOf"]
                self.assertIn(str(ds), datasets)


@unittest.skipUnless(HAVE_DATA and (r.OUT_GRAPH.exists()), "needs dist/facem.ttl (run main.py rdf)")
class Shacl(unittest.TestCase):
    def setUp(self):
        try:
            import pyshacl  # noqa: F401
        except ImportError:
            self.skipTest("pyshacl is not installed")

    def test_written_files_conform(self):
        self.assertEqual(r.shacl([r.OUT_GRAPH, r.OUT_DCAT, r.VOCAB_TTL, r.OUT_ONTOLOGY],
                                 sorted(r.SHAPES_DIR.glob("*.ttl"))), [])

    def test_a_broken_graph_is_caught(self):
        import tempfile
        text = r.OUT_GRAPH.read_text(encoding="utf-8")
        broken = text.replace("    facem:ware <http://w3id.org/facem/vocab/ware/african-red-slip-ware> ;\n", "", 1)
        self.assertNotEqual(text, broken)
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "broken.ttl"
            f.write_text(broken, encoding="utf-8")
            errs = r.shacl([f, r.OUT_DCAT, r.VOCAB_TTL, r.OUT_ONTOLOGY], sorted(r.SHAPES_DIR.glob("*.ttl")))
        self.assertTrue(any("ware" in e for e in errs), errs)


if __name__ == "__main__":
    unittest.main()
