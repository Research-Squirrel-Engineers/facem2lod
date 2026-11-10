import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "py"))

from values import (derive_interval, parse_dimensions, parse_inclusion_row, parse_inclusions, parse_porosity,
                    parse_range, split_chronology)


class Values(unittest.TestCase):
    def test_dimensions(self):
        d = parse_dimensions("diameter: 19cm (rim), 9cm (base); height: 5cm")
        self.assertEqual([(x["kind"], x["part"], x["value"], x["unit"]) for x in d],
                         [("diameter", "rim", 19, "cm"), ("diameter", "base", 9, "cm"), ("height", "", 5, "cm")])
        self.assertEqual(parse_dimensions("diameter: 6.6cm (base)")[0]["value"], 6.6)
        self.assertEqual(parse_dimensions("something odd"), [])

    def test_ranges(self):
        self.assertEqual(parse_range("2.5-7.5"), (2.5, 7.5))
        self.assertEqual(parse_range("0.5"), (0.5, 0.5))
        self.assertIsNone(parse_range("clayey"))

    def test_porosity(self):
        p = parse_porosity("2.5–7.5%; void form: channel;vughy; void length: 0.025-0.25")
        self.assertEqual(p, {"percent": (2.5, 7.5), "void_forms": ["channel", "vughy"], "void_length": (0.025, 0.25)})

    def test_inclusions(self):
        self.assertEqual(parse_inclusions("20–25% %, length: 0.025-0.75"), {"percent": (20, 25), "length": (0.025, 0.75)})

    def test_inclusion_row(self):
        r = parse_inclusion_row("infrequent, subangular;subrounded, elongate;subspherical, silver, 0.025-0.125")
        self.assertEqual(r["frequency"], ["infrequent"])
        self.assertEqual(r["roundness"], ["subangular", "subrounded"])
        self.assertEqual(r["colour"], ["silver"])
        self.assertEqual(r["size"], (0.025, 0.125))
        self.assertIsNone(parse_inclusion_row("singular, subangular, subspherical, clayey")["size"])

    def test_chronology_rules(self):
        cases = {
            "second half of 5th century CE": (450, 500, False),
            "c. 530–580 CE": (530, 580, False),
            "end of 4th–first half of 5th century CE": (375, 450, False),
            "320–mid-5th century CE": (320, 450, False),
            "4th/5th century CE": (300, 500, False),
            "6th century CE": (500, 600, False),
            "first third 5th century CE": (400, 433, False),
            "c. 320–410(+) CE": (320, 410, True),
            "(end of 5th?)–6th century AD": (475, 600, True),
            "end of 6th/early 7th century CE": (575, 625, False),
        }
        for text, want in cases.items():
            self.assertEqual(derive_interval(text), want, text)
        self.assertIsNone(derive_interval("sometime in the past"))
        self.assertIsNone(derive_interval(""))

    def test_split_chronology(self):
        labels = ["Roman-Late Antiquity", "Roman-Early Imperial"]
        self.assertEqual(split_chronology("Roman-Late Antiquity c. 320–375 CE", labels), ("Roman-Late Antiquity", "c. 320–375 CE"))
        self.assertEqual(split_chronology("unknown", labels), ("", "unknown"))


if __name__ == "__main__":
    unittest.main()
