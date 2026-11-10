"""Tests for step_vocab.py: object type parser, hierarchy, Turtle (offline)."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "py"))

import step_vocab as v


def codes(parsed):
    return [(a["typology"], a["code"], a["relation"]) for a in parsed["assignments"]]


class ParserTests(unittest.TestCase):
    def test_plain_with_literature(self):
        p = v.parse_object_type("Hayes 67C ( Hayes 1972 ; Bonifay 2004 )")
        self.assertEqual(codes(p), [("Hayes", "67C", "")])
        self.assertEqual(p["literature"], ["Hayes 1972", "Bonifay 2004"])

    def test_without_literature(self):
        self.assertEqual(codes(v.parse_object_type("Hayes 99B")), [("Hayes", "99B", "")])

    def test_slash_same_typology(self):
        self.assertEqual(codes(v.parse_object_type("Hayes 61A/B4 ( Hayes 1972 )")),
                         [("Hayes", "61A", ""), ("Hayes", "61B4", "alternative")])
        self.assertEqual(codes(v.parse_object_type("Hayes 87A/88")),
                         [("Hayes", "87A", ""), ("Hayes", "88", "alternative")])
        self.assertEqual(codes(v.parse_object_type("Atlante VIII A/C")),
                         [("Atlante", "VIII A", ""), ("Atlante", "VIII C", "alternative")])

    def test_slash_between_typologies(self):
        self.assertEqual(codes(v.parse_object_type("Hayes 76/EM 10.1")),
                         [("Hayes", "76", ""), ("El Mahrine", "10.1", "concordance")])
        self.assertEqual(codes(v.parse_object_type("Hayes 91/Atlante XLIX, 10 ( Hayes 1972 ; Atlante I )")),
                         [("Hayes", "91", ""), ("Atlante", "XLIX, 10", "concordance")])

    def test_certainty(self):
        self.assertEqual(v.parse_object_type("Hayes 67? ( Hayes 1972 )")["assignments"][0]["certainty"], "uncertain")
        self.assertEqual(v.parse_object_type("possibly Hayes 67")["assignments"][0]["certainty"], "possible")
        p = v.parse_object_type("sim. Fulford 27 ( Fulford 1984 )")
        self.assertEqual((p["assignments"][0]["typology"], p["assignments"][0]["certainty"]), ("Fulford", "similar"))

    def test_qualifiers_and_variants(self):
        a = v.parse_object_type("Hayes 61B3, late ( Hayes 1972 )")["assignments"][0]
        self.assertEqual((a["code"], a["qualifiers"]), ("61B3", ["late"]))
        a = v.parse_object_type("Hayes 86, no. 1")["assignments"][0]
        self.assertEqual((a["code"], a["qualifiers"]), ("86", ["no. 1"]))
        a = v.parse_object_type("Hayes 103, var. Sidi Khalifa ( Bonifay 2004 )")["assignments"][0]
        self.assertEqual((a["code"], a["variant"]), ("103", "Sidi Khalifa"))
        a = v.parse_object_type("Hayes 103, var. ( Hayes 1972 )")["assignments"][0]
        self.assertEqual((a["variant"], a["qualifiers"]), ("", ["var."]))

    def test_atlante_code_with_comma_and_trailing_note(self):
        p = v.parse_object_type("Atlante X, D2 ( Atlante I ; Bonifay 2004 ), rim motifs (after Bussière and Rivel 2015 )")
        self.assertEqual(codes(p), [("Atlante", "X, D2", "")])
        self.assertEqual(p["note"], "rim motifs after Bussière and Rivel 2015")
        self.assertEqual(p["literature"], ["Atlante I", "Bonifay 2004"])


class HierarchyTests(unittest.TestCase):
    def test_parent_code(self):
        self.assertEqual(v.parent_code("61B3"), "61B")
        self.assertEqual(v.parent_code("61B"), "61")
        self.assertIsNone(v.parent_code("61"))
        self.assertEqual(v.parent_code("104A1"), "104A")
        self.assertEqual(v.parent_code("XLIX, 10"), "XLIX")
        self.assertEqual(v.parent_code("VIII A"), "VIII")
        self.assertEqual(v.parent_code("18.2"), "18")

    def test_slug(self):
        self.assertEqual(v.slug("XLIX, 10"), "xlix-10")
        self.assertEqual(v.slug("88 var. PM 34.11"), "88-var.-pm-34.11")


class TurtleTests(unittest.TestCase):
    def test_turtle_is_stable_and_has_hierarchy(self):
        s = v.Scheme("hayes", "Hayes typology", "Hayes 1972")
        v.add_type_concept(s, "Hayes", "61B3", 1, "p1")
        text = v.to_turtle({"hayes": s})
        self.assertEqual(text, v.to_turtle({"hayes": s}))
        self.assertIn("skos:broader <http://w3id.org/facem/vocab/hayes/61b>", text)
        self.assertIn("skos:hasTopConcept <http://w3id.org/facem/vocab/hayes/61>", text)
        self.assertTrue(text.endswith(" .\n"))

    def test_literal_escaping(self):
        self.assertEqual(v.lit('a "b"\\c'), '"a \\"b\\"\\\\c"')


@unittest.skipUnless((ROOT / "data" / "derived" / "json" / "index.json").exists(), "no parsed data")
class RealDataTests(unittest.TestCase):
    def test_every_real_object_type_is_understood(self):
        pages = v.load_pages()
        self.assertGreaterEqual(len(pages), 12)
        for page in pages:
            for raw in v.field_values(page, "object data > Object", "object type"):
                p = v.parse_object_type(raw)
                self.assertTrue(p["assignments"], raw)
                for a in p["assignments"]:
                    self.assertIn(a["typology"], v.TYPOLOGIES)


if __name__ == "__main__":
    unittest.main()
