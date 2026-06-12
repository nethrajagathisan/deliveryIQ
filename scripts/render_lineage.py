"""Render the Silver-layer dbt lineage graph to docs/lineage_silver.png.

Reads the real source->model->analysis edges from dbt's manifest.json so the
diagram reflects the actual compiled DAG (not a hand-drawn guess). Pure Pillow,
no browser required.
"""

from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "dbt" / "target" / "manifest.json"
OUT = ROOT / "docs" / "lineage_silver.png"

# colours per layer
C_SOURCE = (123, 97, 255)    # bronze sources (purple)
C_MODEL = (35, 153, 110)     # silver models (green)
C_ANALYSIS = (214, 137, 16)  # analysis (amber)
C_EDGE = (140, 150, 165)
C_BG = (250, 251, 253)
C_TEXT = (255, 255, 255)
C_TITLE = (40, 48, 60)


def load_lineage():
    m = json.loads(MANIFEST.read_text(encoding="utf-8"))
    models = {}
    analyses = {}
    for uid, node in m["nodes"].items():
        if "silver" in node.get("fqn", []) and node["resource_type"] == "model":
            srcs = [
                m["sources"][s]["name"]
                for s in node["depends_on"]["nodes"]
                if s.startswith("source.")
            ]
            models[node["name"]] = srcs
        elif node["resource_type"] == "analysis":
            refs = [
                m["nodes"][n]["name"]
                for n in node["depends_on"]["nodes"]
                if n.startswith("model.")
            ]
            analyses[node["name"]] = refs
    return models, analyses


def font(size: int):
    for name in ("segoeui.ttf", "arial.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def draw_box(d, x, y, w, h, label, colour, f):
    d.rounded_rectangle([x, y, x + w, y + h], radius=10, fill=colour)
    tb = d.textbbox((0, 0), label, font=f)
    tw, th = tb[2] - tb[0], tb[3] - tb[1]
    d.text((x + (w - tw) / 2, y + (h - th) / 2 - tb[1]), label, fill=C_TEXT, font=f)


def main():
    models, analyses = load_lineage()
    model_names = sorted(models)

    W, H = 1100, 130 + len(model_names) * 78 + 120
    img = Image.new("RGB", (W, H), C_BG)
    d = ImageDraw.Draw(img)

    f_title = font(28)
    f_node = font(16)
    f_legend = font(14)

    d.text((40, 28), "DeliveryIQ — Silver Layer Lineage", fill=C_TITLE, font=f_title)

    col_src_x, col_mdl_x = 60, 470
    bw, bh = 320, 46
    top = 100
    gap = 78
    centres = {}

    for i, mdl in enumerate(model_names):
        y = top + i * gap
        src = models[mdl][0] if models[mdl] else "?"
        # source box
        draw_box(d, col_src_x, y, bw, bh, f"bronze.{src}", C_SOURCE, f_node)
        # model box
        draw_box(d, col_mdl_x, y, bw, bh, f"silver.{mdl}", C_MODEL, f_node)
        # edge source -> model
        d.line([col_src_x + bw, y + bh / 2, col_mdl_x, y + bh / 2], fill=C_EDGE, width=3)
        d.polygon(
            [(col_mdl_x, y + bh / 2), (col_mdl_x - 10, y + bh / 2 - 5), (col_mdl_x - 10, y + bh / 2 + 5)],
            fill=C_EDGE,
        )
        centres[mdl] = (col_mdl_x + bw, y + bh / 2)

    # analysis column (fans in from its referenced models)
    col_an_x = 880
    for j, (an, refs) in enumerate(sorted(analyses.items())):
        ys = [centres[r][1] for r in refs if r in centres]
        ay = sum(ys) / len(ys) if ys else top
        draw_box(d, col_an_x, ay - bh / 2, 200, bh, f"{an}", C_ANALYSIS, f_node)
        for r in refs:
            if r in centres:
                sx, sy = centres[r]
                d.line([sx, sy, col_an_x, ay], fill=C_EDGE, width=2)
        d.polygon(
            [(col_an_x, ay), (col_an_x - 10, ay - 5), (col_an_x - 10, ay + 5)],
            fill=C_EDGE,
        )

    # legend
    ly = H - 70
    for k, (lbl, col) in enumerate(
        [("Bronze source", C_SOURCE), ("Silver model", C_MODEL), ("Analysis", C_ANALYSIS)]
    ):
        lx = 60 + k * 200
        d.rounded_rectangle([lx, ly, lx + 22, ly + 22], radius=5, fill=col)
        d.text((lx + 30, ly + 2), lbl, fill=C_TITLE, font=f_legend)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    img.save(OUT)
    print(f"Wrote {OUT}  ({W}x{H})")
    print(f"Models: {len(model_names)}  Analyses: {len(analyses)}")


if __name__ == "__main__":
    main()
