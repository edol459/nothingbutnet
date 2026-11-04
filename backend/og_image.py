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

    # item preview (numbered) — as many as fit above the footer, evenly spaced.
    # How much room is left depends on how many lines the title wrapped to, so
    # the count is derived from the remaining space rather than fixed at 5:
    # forcing a minimum row height used to push the last rows off the card and
    # under the ydkball.net watermark.
    fi = _font(_SANS, 34)
    fn = _font(_MONO, 30)
    footer_y = H - PAD - 4
    ROW_H = 44                      # smallest legible row for the 34px item font
    avail = footer_y - 14 - y       # keeps a gap between the last row and the footer
    n = min(len(items), 5, max(0, avail // ROW_H))
    if n:
        step = min(52, avail // n)
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


# Verdict colours, only used once a ballot has been graded.
GREEN = (46, 125, 79)
RED   = (192, 57, 43)


def render_ballot_card(league: str, season: str, creator: str, slots: list[dict],
                       score: tuple | None = None) -> bytes:
    """An awards ballot as one image — every pick visible without scrolling.

    Typographic on purpose: headshots would mean fetching player art from the
    NBA CDN at request time, which is the exact call Akamai blocks from Railway
    (see docs/cdn-akamai-bot-manager.md). The award/name pairing carries the
    card on its own, and this keeps the endpoint dependency-free and fast.

    `slots` is [{short, label, name, team, correct}] in ballot order; `score` is
    (right, total) once results are in.
    """
    img = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(img)
    d.rectangle([16, 16, W - 16, H - 16], outline=RULE, width=2)

    kicker = f"{league.upper()} AWARDS" + (f"  ·  {season}" if season else "")
    d.text((PAD, PAD), kicker, font=_font(_MONO, 26), fill=ORANGE)
    fw = _font(_SERIF, 36)
    d.text((W - PAD - d.textlength("ydkball", font=fw), PAD - 8), "ydkball",
           font=fw, fill=INK)

    y = PAD + 66

    # The headline award gets the display size; the rest are set as a list, so
    # the hierarchy survives the drop from an interactive sheet to a flat image.
    if slots:
        hero = slots[0]
        d.text((PAD, y), hero.get("label", "").upper(), font=_font(_MONO, 22),
               fill=_verdict_fill(hero.get("correct")))
        y += 34
        fh = _font(_SERIF, 72)
        name = hero.get("name") or "No pick"
        line = _wrap(d, name, fh, W - 2 * PAD, 1)
        d.text((PAD, y), line[0] if line else name, font=fh, fill=INK)
        y += 84
        if hero.get("team"):
            d.text((PAD, y), hero["team"].upper(), font=_font(_MONO, 24), fill=INK3)
            y += 34

    d.rectangle([PAD, y, PAD + 96, y + 5], fill=ORANGE)
    y += 30

    # Remaining awards. Two columns fit five slots; an eight-slot ballot needs
    # three, or the rows run past the footer. Rows are spread across whatever
    # the hero left behind rather than stacked at a fixed pitch — a short pick
    # name otherwise leaves the bottom third of the card empty.
    rest = slots[1:]
    cols = 3 if len(rest) > 4 else 2
    col_w = (W - 2 * PAD) // cols
    fa = _font(_MONO, 22)
    fn = _font(_SANS, 32)
    footer_y = H - PAD - 4
    rows = (len(rest) + cols - 1) // cols
    step = max(70, (footer_y - 24 - y) // rows) if rows else 0
    for i, s in enumerate(rest):
        cx = PAD + (i % cols) * col_w
        row_y = y + (i // cols) * step
        d.text((cx, row_y), s.get("short", "").upper(), font=fa,
               fill=_verdict_fill(s.get("correct")))
        label = _wrap(d, s.get("name") or "No pick", fn, col_w - 20, 1)
        d.text((cx, row_y + 28), label[0] if label else "No pick", font=fn, fill=INK)

    d.text((PAD, footer_y), f"by {creator}  ·  ydkball.net", font=_font(_MONO, 24), fill=INK3)
    if score:
        right, total = score
        tag = f"{right}/{total} CALLED RIGHT"
        ft = _font(_MONO, 26)
        d.text((W - PAD - d.textlength(tag, font=ft), footer_y), tag, font=ft, fill=ORANGE)

    buf = BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def _verdict_fill(correct):
    if correct is True:
        return GREEN
    if correct is False:
        return RED
    return ORANGE


def render_chain_card(label: str, score: int, rounds: int, date_label: str,
                      endless: bool = False) -> bytes:
    """A Higher or Lower result as one image — the thing people actually post.

    Typographic for the same reason `render_ballot_card` is: pulling headshots from
    the NBA CDN at request time is the call Akamai blocks from Railway
    (docs/cdn-akamai-bot-manager.md), and a share endpoint is the worst place to
    inherit that flakiness. The score and the board carry it.

    `endless` renders a Pro run (a depth, with no denominator) instead of the daily.
    """
    img = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(img)
    d.rectangle([16, 16, W - 16, H - 16], outline=RULE, width=2)

    kicker = "HIGHER OR LOWER" + (f"  ·  {date_label}" if date_label else "")
    if endless:
        kicker = "HIGHER OR LOWER  ·  UNLIMITED"
    d.text((PAD, PAD), kicker, font=_font(_MONO, 26), fill=ORANGE)
    fw = _font(_SERIF, 36)
    d.text((W - PAD - d.textlength("ydkball", font=fw), PAD - 8), "ydkball", font=fw, fill=INK)

    # The number is the whole card. A depth with no ceiling gets no denominator.
    y = PAD + 74
    fs = _font(_SERIF, 188)
    big = str(score) if endless else f"{score}"
    d.text((PAD, y), big, font=fs, fill=INK)
    x = PAD + d.textlength(big, font=fs)
    if not endless:
        fd = _font(_SERIF, 92)
        d.text((x + 6, y + 86), f"/{rounds}", font=fd, fill=INK3)
        x += 6 + d.textlength(f"/{rounds}", font=fd)
    else:
        fd = _font(_MONO, 40)
        d.text((x + 18, y + 128), "DEEP", font=fd, fill=INK3)

    # The board line only claims vertical space when there is one — an endless run
    # with no label would otherwise leave a hole between the score and the pips.
    y += 214
    if label:
        fl = _font(_SERIF, 54)
        line = _wrap(d, label, fl, W - 2 * PAD, 1)
        d.text((PAD, y), line[0], font=fl, fill=INK)
        y += 86
    else:
        y += 24

    # Round pips — the on-page bar, so the image and the game look like each other.
    if not endless:
        bar_w, gap = (W - 2 * PAD - (rounds - 1) * 10) // max(1, rounds), 10
        for i in range(rounds):
            x0 = PAD + i * (bar_w + gap)
            if i < score:
                fill = GREEN
            elif i == score and score < rounds:
                fill = RED
            else:
                fill = RULE
            d.rounded_rectangle([x0, y, x0 + bar_w, y + 14], radius=7, fill=fill)
    else:
        bar_w = (W - 2 * PAD) // max(1, score + 1)
        for i in range(score):
            x0 = PAD + i * bar_w
            d.rounded_rectangle([x0, y, x0 + bar_w - 8, y + 14], radius=7, fill=GREEN)

    d.text((PAD, H - PAD - 4), "ydkball.net/survival", font=_font(_MONO, 24), fill=INK3)
    tag = "SUDDEN DEATH"
    ft = _font(_MONO, 24)
    d.text((W - PAD - d.textlength(tag, font=ft), H - PAD - 4), tag, font=ft, fill=ORANGE)

    buf = BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()
