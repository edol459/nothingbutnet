"""
og_image.py — generate branded Open Graph share cards for public lists (Pillow).

Rendered server-side at /list/<id>/og.png so shared links show a rich preview in
iMessage/X/Discord (social scrapers don't run JS). Matches the site's paper theme.
"""

import os
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont

_FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "fonts")
_SERIF = os.path.join(_FONT_DIR, "DMSerifDisplay-Regular.ttf")
_MONO  = os.path.join(_FONT_DIR, "DMMono-Medium.ttf")
_SANS  = os.path.join(_FONT_DIR, "InstrumentSans.ttf")

W, H = 1200, 630
PAPER = (245, 240, 232)
INK   = (12, 12, 13)
INK3  = (107, 107, 120)
ORANGE = (232, 83, 10)
RULE  = (216, 213, 207)
PAD   = 72

_cache: dict = {}  # size -> ImageFont, so we don't reload TTFs every request


def _font(path, size):
    key = (path, size)
    if key not in _cache:
        _cache[key] = ImageFont.truetype(path, size)
    return _cache[key]


def _wrap(draw, text, font, max_w, max_lines):
    words = (text or "").split()
    lines, cur = [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        if draw.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = w
            if len(lines) == max_lines:
                break
    if cur and len(lines) < max_lines:
        lines.append(cur)
    # ellipsize if we truncated
    if lines and len(" ".join(lines)) < len(text or ""):
        while lines and draw.textlength(lines[-1] + "…", font=font) > max_w:
            lines[-1] = lines[-1].rsplit(" ", 1)[0] if " " in lines[-1] else lines[-1][:-1]
        lines[-1] = lines[-1] + "…"
    return lines


def render_list_card(title: str, subtitle: str, items: list[str], kicker: str = "LIST") -> bytes:
    img = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(img)
    d.rectangle([16, 16, W - 16, H - 16], outline=RULE, width=2)

    # kicker (top-left, mono, orange) + wordmark (top-right, serif)
    d.text((PAD, PAD), kicker, font=_font(_MONO, 26), fill=ORANGE)
    fw = _font(_SERIF, 36)
    word = "ydkball"
    d.text((W - PAD - d.textlength(word, font=fw), PAD - 8), word, font=fw, fill=INK)

    # title (serif, wrapped up to 3 lines)
    ft = _font(_SERIF, 78)
    y = PAD + 62
    for ln in _wrap(d, title, ft, W - 2 * PAD, 3):
        d.text((PAD, y), ln, font=ft, fill=INK)
        y += 90

    # subtitle
    y += 4
    d.text((PAD, y), subtitle, font=_font(_SANS, 30), fill=INK3)
    y += 52

    # accent rule
    d.rectangle([PAD, y, PAD + 96, y + 5], fill=ORANGE)
    y += 34

    # item preview (numbered) — distribute up to 5 evenly in the space above footer
    fi = _font(_SANS, 34)
    fn = _font(_MONO, 30)
    footer_y = H - PAD - 4
    n = min(len(items), 5)
    if n:
        step = min(52, max(44, (footer_y - 24 - y) // n))
        for i, it in enumerate(items[:n]):
            d.text((PAD, y), f"{i + 1}.", font=fn, fill=ORANGE)
            label = _wrap(d, it, fi, W - 2 * PAD - 64, 1)
            d.text((PAD + 60, y - 2), label[0] if label else it, font=fi, fill=INK)
            y += step

    # footer
    d.text((PAD, footer_y), "ydkball.net", font=_font(_MONO, 24), fill=INK3)

    buf = BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()
