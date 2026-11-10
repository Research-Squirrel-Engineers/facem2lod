"""Offline tests for py/step_parse.py against a synthetic fixture.

    python -m unittest discover -s tests -v
"""

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "py"))

from step_parse import parse_html  # noqa: E402

FIXTURE = HERE / "fixtures" / "synthetic_fabric_page.html"


class ParseFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rec = parse_html(FIXTURE.read_bytes(), "https://facem.at/ou-ars-1",
                             "ou-ars-1", "fabric")

    def section(self, name):
        return next(s for s in self.rec["sections"] if s["name"] == name)

    def test_title_and_tabs(self):
        self.assertEqual(self.rec["title"], "Facem - OU-ARS-1")
        self.assertEqual(self.rec["tabs_found"], [
            "object data", "fabric description", "archaeometry", "petrography"])

    def test_sections_split_on_single_cell_rows(self):
        names = [s["name"] for s in self.rec["sections"]]
        self.assertIn("Context Data", names)
        self.assertIn("Object", names)
        self.assertIn("Inclusions (all lengths in mm)", names)

    def test_key_value_and_multi_value_rows(self):
        obj = self.section("Object")
        self.assertIn({"key": "object type", "value": "Hayes 99B"}, obj["fields"])
        incl = self.section("Inclusions (all lengths in mm)")
        quartz = next(f for f in incl["fields"] if f["key"] == "quartz")
        self.assertEqual(quartz["values"], ["frequent", "clear, grey, white", "up to 0.25"])

    def test_links_are_absolute_and_classified(self):
        kinds = {l["href"]: l["kind"] for l in self.rec["links"]}
        self.assertEqual(kinds["https://facem.at/map/production_site.php?id=7"], "site")
        self.assertEqual(kinds["https://facem.at/m-201-2"], "record")
        self.assertEqual(kinds["https://facem.at/papers/hasenzagl.pdf"], "pdf")

    def test_images_and_caption(self):
        img = self.rec["images"][0]
        self.assertEqual(img["src"], "https://facem.at/img/medium/ou-ars-1-4x.jpg")
        self.assertEqual(img["caption"], "thin section, 4x")

    def test_script_text_is_dropped(self):
        self.assertFalse(any("not part of the page text" in l for l in self.rec["text_lines"]))

    def test_deterministic(self):
        again = parse_html(FIXTURE.read_bytes(), "https://facem.at/ou-ars-1",
                           "ou-ars-1", "fabric")
        self.assertEqual(again, self.rec)


if __name__ == "__main__":
    unittest.main()
