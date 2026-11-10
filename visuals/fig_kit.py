"""Figure kit in the house style of GeoScience-FAIRification-LOD/visuals (copied, not imported).

1750x1000 SVG, Fira Sans named in the file (not embedded), PNG rendered with resvg. Text widths come from the bundled
Fira Sans (PIL, BASIC layout), so a figure looks the same on Windows and Linux; every text has a width limit and raises
instead of overflowing. No dates, no random numbers: running a figure twice gives the same bytes.
"""
from __future__ import annotations

import math
from pathlib import Path
from xml.sax.saxutils import escape

from PIL import ImageFont

HERE = Path(__file__).resolve().parent
REG, MED = HERE / "fonts" / "FiraSans-Regular.ttf", HERE / "fonts" / "FiraSans-Medium.ttf"

W, H = 1750, 1000
# (fill, stroke, text colour)
CLS = ("#9a3412", "#000000", "#ffffff")       # class of the facem ontology
REAL = ("#166534", "#000000", "#ffffff")      # object of the real world
TERM = ("#4c1d95", "#000000", "#ffffff")      # vocabulary term (SKOS)
OWL = ("#ffffff", "#000000", "#000000")       # class of another vocabulary
INST = ("#e2e8f0", "#000000", "#000000")
PROP = ("#fbbf24", "#000000", "#000000")      # property chip
DARK, MUTED, LINE = "#1f1f1d", "#55544f", "#55544f"


def tw(s: str, size: float, med: bool = False) -> float:
    font = ImageFont.truetype(str(MED if med else REG), 100, layout_engine=ImageFont.Layout.BASIC)
    return round(font.getlength(s) * size / 100, 1)


class Fig:
    def __init__(self, name, title, w=W, h=H, outdir=None):
        self.name, self.w, self.h, self.outdir = name, w, h, outdir
        self.o = [
            f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg" role="img"><title>{escape(title)}</title>',
            f'<rect width="{w}" height="{h}" fill="#ffffff"/>',
            f'<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1L9 5L1 9" fill="none" stroke="{LINE}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>']

    def text(self, x, y, t, size=22, med=False, color=DARK, anchor="middle", maxw=None):
        if maxw and tw(t, size, med) > maxw:
            raise ValueError(f"{self.name}: too wide ({tw(t, size, med):.0f}>{maxw}): {t}")
        wt = ' font-weight="500"' if med else ""
        self.o.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" dominant-baseline="central" font-family="Fira Sans" font-size="{size}"{wt} fill="{color}">{escape(t)}</text>')

    def box(self, x, y, w, h, title, sub=None, st=INST, ts=22, ss=17, rx=6, dash=False):
        f, b, c = st
        ds = ' stroke-dasharray="8 6" stroke-width="2.4"' if dash else ' stroke-width="1"'
        self.o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{f}" stroke="{b}"{ds}/>')
        cx = x + w / 2
        if sub:
            self.text(cx, y + h / 2 - ts * .55, title, ts, True, c, maxw=w - 14)
            self.text(cx, y + h / 2 + ts * .62, sub, ss, False, c, maxw=w - 14)
        else:
            self.text(cx, y + h / 2, title, ts, True, c, maxw=w - 14)

    def line(self, x1, y1, x2, y2, arrow=True, dash=False, w=1.6):
        d = ' stroke-dasharray="7 6"' if dash else ""
        mk = ' marker-end="url(#a)"' if arrow else ""
        self.o.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{LINE}" stroke-width="{w}"{d}{mk}/>')

    def path(self, pts, arrow=True, dash=False):
        """Right-angled polyline through the given points (x, y)."""
        d = "M" + " L".join(f"{x} {y}" for x, y in pts)
        ds = ' stroke-dasharray="7 6"' if dash else ""
        mk = ' marker-end="url(#a)"' if arrow else ""
        self.o.append(f'<path d="{d}" fill="none" stroke="{LINE}" stroke-width="1.6" stroke-linejoin="round"{ds}{mk}/>')

    def rect(self, x, y, w, h, fill, stroke="none", rx=0, sw=1, dash=False):
        ds = ' stroke-dasharray="7 6"' if dash else ""
        self.o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{ds}/>')

    def chip(self, x, y, t, size=16, anchor="middle"):
        w = tw(t, size) + 14
        x0 = x - w / 2 if anchor == "middle" else x
        self.rect(x0, y - 13, w, 26, PROP[0], "#000000", 4)
        self.text(x0 + w / 2, y, t, size, color="#000000")

    def legend(self, y, items, x):
        sizes = [34 + tw(t, 18) + 30 for _, t in items]
        for (st, t), sz in zip(items, sizes):
            self.rect(x, y - 11, 22, 22, st[0], "#000000", 3)
            self.text(x + 32, y, t, 18, color=MUTED, anchor="start")
            x += sz

    def save(self, fallback_dir: Path):
        import resvg_py
        d = self.outdir or fallback_dir
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"{self.name}.svg"
        p.write_text("\n".join(self.o + ["</svg>"]) + "\n", encoding="utf-8", newline="\n")
        (d / f"{self.name}.png").write_bytes(resvg_py.svg_to_bytes(
            svg_path=str(p), background="#ffffff", skip_system_fonts=True,
            font_files=[str(REG), str(MED)], zoom=2.0))
