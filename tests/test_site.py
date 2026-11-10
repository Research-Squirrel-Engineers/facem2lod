"""Tests for step_site.py: pages, search index, byte stability (offline, real graphs)."""
import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "py"))

import site_kit as kit
import step_site as site

HAVE = (site.DIST / "facem.ttl").exists() and site.VOCAB_JSON.exists()


class Kit(unittest.TestCase):
    def test_nav_marks_the_current_page(self):
        n = kit.nav_html("search.html")
        self.assertIn('<a class="on" href="search.html">', n)
        self.assertEqual(n.count('class="on"'), 1)

    def test_page_escapes_the_title(self):
        self.assertIn("&lt;b&gt;", kit.page("index.html", "<b>", "x", ""))


@unittest.skipUnless(HAVE, "needs dist/facem.ttl (run main.py)")
class Pages(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = site.Data()
        cls.recs = site.records(cls.d)

    def test_one_record_per_sherd_with_the_fields_the_search_needs(self):
        self.assertEqual(len(self.recs), 60)
        for r in self.recs:
            for k in ("no", "fabric", "form", "types", "from", "to", "place", "page", "img"):
                self.assertIn(k, r, r["id"])
            self.assertTrue(r["types"], r["id"])
            self.assertLessEqual(r["from"], r["to"])
            self.assertTrue(r["page"].startswith("https://facem.at/"))

    def test_every_fabric_has_a_card(self):
        self.assertEqual(len(site.fabric_cards(self.recs)), 12)

    def test_embedded_json_is_valid_and_cannot_close_the_script(self):
        text = site.search_page(self.recs)
        blob = re.search(r'<script id="data" type="application/json">(.*?)</script>', text, re.S).group(1)
        self.assertEqual(len(json.loads(blob)["records"]), 60)
        self.assertNotIn("</", blob)

    def test_pages_are_byte_stable(self):
        a = [site.index_page(self.d, self.recs), site.search_page(self.recs), site.model_page(), site.vocab_page(self.d),
             site.example_page(self.d), site.data_page()]
        b = [site.index_page(self.d, self.recs), site.search_page(self.recs), site.model_page(), site.vocab_page(self.d),
             site.example_page(self.d), site.data_page()]
        self.assertEqual(a, b)

    def test_example_shows_the_sherd_and_its_assignments(self):
        text = site.example_page(self.d)
        for needle in ("ent:sherd_m-200-10", "ent:sample_m-200-10", "ent:typeassignment_m-200-10-2", "ent:dating_m-200-10"):
            self.assertIn(needle, text)

    def test_every_nav_target_is_written(self):
        site.run()
        for f, _ in kit.PAGES:
            self.assertTrue((site.DOCS / f).exists(), f)


if __name__ == "__main__":
    unittest.main()
