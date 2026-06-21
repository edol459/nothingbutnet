"""
ydkball — Trivia question engine (NBA, standalone prototype)
============================================================

Spins random combinations of {stat × operator × season} against the
`player_seasons` table and produces validated trivia questions whose answer is a
player (or a set of players).  This is a *testing/refinement* harness — not wired
into the app yet.  Run it, eyeball the output, tune the config, repeat.

    python backend/games/question_engine.py                 # 10 sample questions
    python backend/games/question_engine.py --count 30      # more
    python backend/games/question_engine.py --seed 42        # reproducible
    python backend/games/question_engine.py --stat pts       # force a stat
    python backend/games/question_engine.py --op threshold   # force an operator
    python backend/games/question_engine.py --show-answers   # list every answer

Design notes
------------
* Thresholds are chosen FROM THE DATA distribution (nice round numbers that land
  the answer set in a "fun" size band), so we don't emit impossible ("> 40 PPG")
  or trivial ("> 0 PPG") questions.
* A minimum-games qualifier is applied so a 3-game sample can't "lead the league".
* Format follows the answer count: exactly 1 qualifier → "name the player";
  several → "name the set" (we tell the user N).
* Superlatives (most/least) are deliberately down-weighted — they have tiny,
  memorizable answer spaces.  Thresholds/ranges are the replayable core.
"""

import os
import re
import random
import difflib
import argparse
import unicodedata
import statistics
from dataclasses import dataclass, field

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — tune freely
# ─────────────────────────────────────────────────────────────────────────────

MIN_GP        = 40          # games-played qualifier for the regular season (~82 games)
PLAYOFF_MIN_GP = 5          # playoff qualifier — a full first-round series
PLAYOFF_PROB  = 0.20        # share of questions drawn from the playoffs
ANSWER_BAND   = (1, 5)      # cap at 5 — naming more than 5 players is a slog
SEASON_TYPE   = "Regular Season"


def min_gp_for(season_type):
    return PLAYOFF_MIN_GP if season_type == "Playoffs" else MIN_GP


def when(season, season_type):
    """Display label: '2015-16' for regular season, '2016 playoffs' for playoffs."""
    if season_type == "Playoffs":
        return f"{int(season[:4]) + 1} playoffs"
    return season

# Question formats. Top-N is the workhorse (no arbitrary numbers); thresholds only
# fire at curated milestones; superlative is the classic "led the league". Ranges
# are intentionally retired — "between 65 and 70 passes" feels random.
TOPN_SIZES = [3, 5]   # "name the top N"

# Stat menu.  `step` = the natural rounding granularity for thresholds.
# `pct` stats are stored as fractions (0–1) in some columns and 0–100 in others;
# we auto-detect at runtime.  `dir` = which end is "interesting" for superlatives.
#   label is filled into question templates: "averaged at least X {noun}".
@dataclass
class Stat:
    key:   str
    noun:  str           # e.g. "points per game"
    short: str           # e.g. "PPG"  (for compact answer display)
    step:  float
    pct:   bool = False
    high_is_good: bool = True    # False = lower is better (e.g. opp FG% at the rim)
    vol_col: str = None          # volume qualifier column (per-game)
    vol_min: float = 0           # e.g. fg3_pct only counts if fg3a >= vol_min
    vol_label: str = None        # human label for the volume floor, e.g. "3PA"
    allow_range: bool = True     # percentage/PPP ranges are weak — disable per stat
    high_end_only: bool = False  # only the upper end is interesting (no garbage-time low end)
    min_season: str = None       # earliest season this column is populated
    verb: str = None             # threshold/range action verb (default: posted/averaged)
    sup_verb: str = None         # superlative verb (default: "led the NBA in" / "had the fewest")
    superlative_only: bool = False  # only ask "who led…" (e.g. charges — tiny per-game values)
    milestones: list = None      # curated round thresholds (display units). None → top-N only.
    expr: str = None             # SQL expression for derived stats, e.g. "(ps.pts+ps.reb+ps.ast)"
    tier: int = 1                # familiarity: 0 common (PPG), 1 medium, 2 obscure (deflections)
    integer: bool = False        # display as a whole number (season totals, e.g. charges)
    topn_label: str = None       # clearer top-N phrasing, e.g. "rim protectors" (for low-is-good)

# ── Tier 1: populated for all 30 seasons (1996-97 →) ──
STAT_POOL = [
    Stat("pts",          "points per game",          "PPG", 5, high_end_only=True),
    Stat("ast",          "assists per game",         "APG", 1, high_end_only=True),
    Stat("reb",          "rebounds per game",        "RPG", 1, high_end_only=True),
    Stat("stl",          "steals per game",          "SPG", 0.5, high_end_only=True),
    Stat("blk",          "blocks per game",          "BPG", 0.5, high_end_only=True),
    Stat("fg3m",         "made threes per game",     "3PM", 0.5, high_end_only=True),
    Stat("tov",          "turnovers per game",       "TOV", 1, high_end_only=True),
    Stat("plus_minus",   "plus-minus per game",      "+/-", 1, high_end_only=True),
    Stat("pra",          "P+R+A per game",           "PRA", 5, high_end_only=True,
         expr="(ps.pts + ps.reb + ps.ast)"),
    Stat("fgm_25ft_pg",  "made FGs from 25+ feet per game", "25FT", 0.5, high_end_only=True),
    # Rate stats: require real volume so a 1-for-1 shooter can't "lead" at 100%.
    Stat("ts_pct",  "true shooting %", "TS%", 0.02, pct=True, vol_col="fga",  vol_min=8.0, vol_label="FGA", allow_range=False),
    Stat("fg3_pct", "three-point %",   "3P%", 0.02, pct=True, vol_col="fg3a", vol_min=3.0, vol_label="3PA", allow_range=False),
    Stat("fg_pct",  "field-goal %",    "FG%", 0.02, pct=True, vol_col="fga",  vol_min=8.0, vol_label="FGA", allow_range=False),
    Stat("ft_pct",  "free-throw %",    "FT%", 0.02, pct=True, vol_col="fta",  vol_min=2.0, vol_label="FTA", allow_range=False),

    # ── Tier 2: tracking / hustle / play-type — only populated 2020-21 → present ──
    # Hustle
    Stat("deflections",     "deflections per game",       "DEF",  0.5, high_end_only=True, min_season="2020-21"),
    Stat("contested_shots", "contested shots per game",   "CON",  2,   high_end_only=True, min_season="2020-21"),
    Stat("charges_total",   "charges",                    "CHG",  1, min_season="2020-21",
         superlative_only=True, sup_verb="drew the most",
         expr="(ps.charges_drawn * ps.gp)", integer=True),   # per-game × games = season total
    # Defense
    Stat("def_rim_fg_pct", "opponent FG% at the rim", "RIM", 0.02, pct=True, high_is_good=False,
         vol_col="def_rim_fga", vol_min=4.0, vol_label="rim att/g", allow_range=False,
         min_season="2020-21", verb="allowed", sup_verb="allowed the lowest",
         topn_label="rim protectors (lowest opponent FG% at the rim)"),
    # Tracking
    Stat("drives",          "drives per game",            "DRV",  2,   high_end_only=True, min_season="2020-21"),
    Stat("passes_made",     "passes per game",            "PASS", 5,   high_end_only=True, min_season="2020-21"),
    Stat("ast_pts_created", "assist points created per game", "APC", 2,  high_end_only=True, min_season="2020-21"),
    Stat("post_touch_pts",  "post-up points per game",    "POST", 1,   high_end_only=True, min_season="2020-21"),
    Stat("potential_ast",   "potential assists per game", "PAST", 2,   high_end_only=True, min_season="2020-21"),
    # Play-type efficiency (points per possession) — each needs a possessions floor
    Stat("iso_ppp",        "points per possession on isolations", "ISO", 0.1, vol_col="iso_fga",        vol_min=4.0, vol_label="isos/g",       allow_range=False, high_end_only=True, min_season="2020-21"),
    Stat("pnr_bh_ppp",     "PPP as the pick-and-roll ball-handler", "PNR", 0.1, vol_col="pnr_bh_fga",     vol_min=5.0, vol_label="PnR poss/g",   allow_range=False, high_end_only=True, min_season="2020-21"),
    Stat("pnr_roll_ppp",   "PPP as the roll man",          "ROLL", 0.1, vol_col="pnr_roll_poss", vol_min=3.0, vol_label="roll poss/g",  allow_range=False, high_end_only=True, min_season="2020-21"),
    Stat("post_ppp",       "points per possession in the post", "POSTUP", 0.1, vol_col="post_poss",      vol_min=3.0, vol_label="post-ups/g",  allow_range=False, high_end_only=True, min_season="2020-21"),
]

# Curated "achievement club" thresholds, in display units. Only stats with culturally
# recognized round numbers get thresholds; everything else is top-N only.
_MILESTONES = {
    "pts": [20, 25, 30],   "ast": [8, 10],     "reb": [10, 12, 15],
    "stl": [2],            "blk": [2, 3],      "fg3m": [3, 4],
    "tov": [4],            "pra": [45, 50],
    "fgm_25ft_pg": [2, 3],   # the "deep shooter" club: 2+/3+ makes from 25+ ft a game
    "ts_pct": [60, 65],    "fg3_pct": [40, 45],"fg_pct": [50, 55, 60],
    "ft_pct": [90],
    "def_rim_fg_pct": [50, 52, 54],   # "allowed no more than 50% at the rim"
}
for _s in STAT_POOL:
    _s.milestones = _MILESTONES.get(_s.key)

# Familiarity tiers (for difficulty + picking good team-question stats):
#   0 = common box stat (PPG, RPG…)               → easy/medium by count
#   1 = medium, but the leader is a known star     → single-answer can be easy
#         (shooting %s, +/-, drives, passes, potential ast, post-up pts, 25-ft makes)
#   2 = obscure even for the leader                → auto-hard
#         (play-type PPP, rim defense, charges, contested shots, deflections)
_TIER0 = {"pts", "reb", "ast", "stl", "blk", "fg3m", "tov", "pra"}
_TIER2 = {"iso_ppp", "pnr_bh_ppp", "pnr_roll_ppp", "post_ppp",
          "def_rim_fg_pct", "charges_total", "contested_shots", "deflections"}
for _s in STAT_POOL:
    _s.tier = 0 if _s.key in _TIER0 else 2 if _s.key in _TIER2 else 1

# Team abbr → full name for the current 30 franchises; historical abbrs fall back to the abbr.
TEAM_NAMES = {
    "ATL": "Hawks", "BOS": "Celtics", "BKN": "Nets", "CHA": "Hornets", "CHI": "Bulls",
    "CLE": "Cavaliers", "DAL": "Mavericks", "DEN": "Nuggets", "DET": "Pistons",
    "GSW": "Warriors", "HOU": "Rockets", "IND": "Pacers", "LAC": "Clippers", "LAL": "Lakers",
    "MEM": "Grizzlies", "MIA": "Heat", "MIL": "Bucks", "MIN": "Timberwolves",
    "NOP": "Pelicans", "NYK": "Knicks", "OKC": "Thunder", "ORL": "Magic", "PHI": "76ers",
    "PHX": "Suns", "POR": "Trail Blazers", "SAC": "Kings", "SAS": "Spurs", "TOR": "Raptors",
    "UTA": "Jazz", "WAS": "Wizards",
    # Historical franchises/relocations (full names for clarity in older questions)
    "NJN": "New Jersey Nets", "SEA": "Seattle SuperSonics", "VAN": "Vancouver Grizzlies",
    "CHH": "Charlotte Hornets", "NOH": "New Orleans Hornets",
    "NOK": "New Orleans/Oklahoma City Hornets",
}

def team_name(abbr):
    return TEAM_NAMES.get(abbr, abbr)

# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


@dataclass
class Answer:
    player_id: int
    name: str
    value: float
    display: str = None   # optional custom display (e.g. a 50/40/90 slash line)


@dataclass
class Question:
    text: str
    stat: Stat
    season: str
    operator: str
    answers: list = field(default_factory=list)   # list[Answer]
    fmt: str = "single"                            # "single" | "set"
    difficulty: str = "medium"
    season_type: str = "Regular Season"            # or "Playoffs"
    team: str = None                               # team abbr (team questions only) — for dedup
    options: list = None                           # this-or-that: the two players to pick between

    @property
    def n(self):
        return len(self.answers)


# ── data access ──────────────────────────────────────────────────────────────

# Leaderboards for a (stat, season) don't change, so cache them in-process — the
# generation retry loop re-queries the same boards constantly. (A long-running server
# would add a TTL for the in-progress current season; fine as-is for a fresh process.)
_QUALIFIED_CACHE = {}

def load_qualified(conn, stat: Stat, season: str, team: str = None, season_type: str = "Regular Season"):
    """Return [(player_id, name, value)] for qualified players, sorted desc by value.
    Pass `team` (abbr) to scope to a single roster; `season_type` for playoffs."""
    ckey = (stat.key, season, season_type, team)
    if ckey in _QUALIFIED_CACHE:
        return _QUALIFIED_CACHE[ckey]
    col = stat.expr or f"ps.{stat.key}"     # derived stats (e.g. P+R+A) use an expression
    vol_clause = ""
    params = [season, season_type, min_gp_for(season_type)]
    if stat.vol_col and stat.vol_min:
        vol_clause = f"AND ps.{stat.vol_col} >= %s"
        params.append(stat.vol_min)
    team_clause = ""
    if team:
        team_clause = "AND ps.team_abbr = %s"
        params.append(team)
    sql = f"""
        SELECT ps.player_id, p.player_name AS name, {col} AS val
        FROM   player_seasons ps
        JOIN   players p ON p.player_id = ps.player_id
        WHERE  ps.season = %s
          AND  ps.season_type = %s
          AND  ps.gp >= %s
          {vol_clause}
          {team_clause}
          AND  {col} IS NOT NULL
        ORDER BY {col} DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = [(r["player_id"], r["name"], float(r["val"])) for r in cur.fetchall()]
    _QUALIFIED_CACHE[ckey] = rows
    return rows


def list_seasons(conn, season_type="Regular Season"):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT season FROM player_seasons WHERE season_type=%s ORDER BY season",
            (season_type,),
        )
        return [r["season"] for r in cur.fetchall()]


_PO_SEASONS = None
def playoff_seasons(conn):
    global _PO_SEASONS
    if _PO_SEASONS is None:
        _PO_SEASONS = list_seasons(conn, "Playoffs")
    return _PO_SEASONS


# ── formatting helpers ───────────────────────────────────────────────────────

def detect_pct_scale(values):
    """pct columns are either 0–1 or 0–100; return a multiplier to display as %."""
    return 100.0 if (values and max(values) <= 1.5) else 1.0


def fmt_val(stat: Stat, v: float, scale: float = 1.0):
    if stat.pct:
        return f"{v * scale:.1f}%"
    if stat.integer:
        return f"{v:.0f}"
    return f"{v:.1f}"


def fmt_threshold(stat: Stat, t: float, scale: float = 1.0):
    if stat.pct:
        return f"{t * scale:.0f}%" if (t * scale) % 1 == 0 else f"{t * scale:.1f}%"
    if stat.step >= 1:
        return f"{t:.0f}"
    return f"{t:.1f}"


def _ans_val(q, a, scale):
    """The value portion for an answer (custom display, e.g. a 50/40/90 slash, or the stat value)."""
    return a.display or fmt_val(q.stat, a.value, scale)


def _ans_label(q, a, scale):
    """Full label: value + stat short code (or just the custom display for clubs)."""
    return a.display or f"{fmt_val(q.stat, a.value, scale)} {q.stat.short}"


# ── name matching (so "Jokic" matches "Nikola Jokić") ────────────────────────

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

def normalize_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z\s]", " ", s.lower())
    toks = [t for t in s.split() if t not in _SUFFIXES]
    return " ".join(toks)

def last_name(s: str) -> str:
    toks = normalize_name(s).split()
    return toks[-1] if toks else ""

def match_guess(guess: str, answers):
    """Return the matching Answer, or None. Accepts full name or an unambiguous last name."""
    g = normalize_name(guess)
    if not g:
        return None
    for a in answers:                       # exact full-name match
        if normalize_name(a.name) == g:
            return a
    hits = [a for a in answers if last_name(a.name) == g]   # unique last-name match
    return hits[0] if len(hits) == 1 else None


# Common nicknames the fuzzy matcher won't catch on its own.
# Keys are normalized at load time, so "cp3"→"cp", "pg13"→"pg" still resolve.
NICKNAMES = {
    "shaq": "shaquille oneal",
    "kd": "kevin durant",
    "cp3": "chris paul",
    "kg": "kevin garnett",
    "the beard": "james harden",
    "steph": "stephen curry",
    "dame": "damian lillard",
    "melo": "carmelo anthony",
    "ad": "anthony davis",
    "pg13": "paul george",
    "greek freak": "giannis antetokounmpo",
    "manu": "manu ginobili",
    "t mac": "tracy mcgrady",
    "tmac": "tracy mcgrady",
    "d wade": "dwyane wade",
}
_NICK_NORM = {normalize_name(k): v for k, v in NICKNAMES.items()}


def load_player_bank(conn):
    """Build a name→player index over all players, for autocomplete-style resolution.

    Buckets (last/first name) are ranked by career games so an ambiguous surname like
    "garnett" resolves to the prominent player (Kevin), not a journeyman."""
    with conn.cursor() as cur:
        cur.execute("SELECT player_id, player_name FROM players WHERE player_name IS NOT NULL")
        people = cur.fetchall()
        cur.execute("SELECT player_id, COALESCE(SUM(gp),0) AS g FROM player_seasons GROUP BY player_id")
        prom = {r["player_id"]: float(r["g"] or 0) for r in cur.fetchall()}

    full, last, first = {}, {}, {}
    for r in people:
        pid, name = r["player_id"], r["player_name"]
        nf = normalize_name(name)
        if not nf:
            continue
        full.setdefault(nf, (pid, name))
        last.setdefault(last_name(name), []).append((pid, name))
        first.setdefault(nf.split()[0], []).append((pid, name))

    by_prom = lambda t: prom.get(t[0], 0)
    for bucket in (last, first):
        for k in bucket:
            bucket[k].sort(key=by_prom, reverse=True)   # most prominent first
    return {"full": full, "last": last, "first": first, "norms": list(full.keys())}


def resolve_player(text: str, bank):
    """Resolve typed text to a real (player_id, name) — tolerant of spelling & nicknames.

    Mirrors how the app's autocomplete dropdown will work: you don't need perfect
    spelling, but you still have to know *who* you mean. Returns (id, name) or None."""
    n = normalize_name(text)
    if not n:
        return None
    n = _NICK_NORM.get(n, n)
    if n in bank["full"]:
        return bank["full"][n]
    if n in bank["last"]:               # surname (prominence-ranked)
        return bank["last"][n][0]
    if n in bank["first"]:              # iconic first name (Dirk, Giannis, Luka…)
        return bank["first"][n][0]
    m = difflib.get_close_matches(n, bank["norms"], n=1, cutoff=0.84)   # fuzzy full name
    if m:
        return bank["full"][m[0]]
    m = difflib.get_close_matches(n, list(bank["last"].keys()), n=1, cutoff=0.82)  # fuzzy surname
    if m:
        return bank["last"][m[0]][0]
    return None


# ── threshold picking (data-driven) ──────────────────────────────────────────

def nice_levels(values, step):
    """Round candidate thresholds spanning the data range, by `step`."""
    lo = (min(values) // step) * step
    hi = (max(values) // step + 1) * step
    out, x = [], lo
    while x <= hi:
        out.append(round(x, 4))
        x += step
    return out


def count_at_least(values, t):
    return sum(1 for v in values if v >= t)


def count_at_most(values, t):
    return sum(1 for v in values if v <= t)


def count_between(values, lo, hi):
    return sum(1 for v in values if lo <= v <= hi)


# ── generators ───────────────────────────────────────────────────────────────

def _subject(n):
    """Singular vs plural framing based on how many players qualify."""
    return "Who" if n == 1 else "Which players"


def _verb(stat):
    return stat.verb or ("posted" if stat.pct else "averaged")


def _qualifier(stat, season_type="Regular Season"):
    """Trailing '(min …)' note — shows games and any volume floor."""
    if not (stat.pct or stat.vol_col):
        return ""                              # counting stats: keep it clean
    parts = [f"min {min_gp_for(season_type)} games"]
    if stat.vol_col and stat.vol_min:
        parts.append(f"{stat.vol_min:g} {stat.vol_label or stat.vol_col}")
    return f" ({', '.join(parts)})"


def gen_topn(stat, season, rows, season_type="Regular Season"):
    """'Name the top N in X.' The workhorse — no arbitrary thresholds."""
    if len(rows) < min(TOPN_SIZES):
        return None
    ordered = rows if stat.high_is_good else list(reversed(rows))   # best-first
    n = random.choice([s for s in TOPN_SIZES if s <= len(ordered)] or [len(ordered)])
    cutoff = ordered[n - 1][2]
    # include players tied at the boundary value so the cutoff isn't arbitrary
    cmp = (lambda v: v >= cutoff) if stat.high_is_good else (lambda v: v <= cutoff)
    top = [r for r in ordered if cmp(r[2])]
    if len(top) > n + 2:        # a big tie pile-up — just take the clean N
        top = ordered[:n]
    answers = [Answer(pid, nm, v) for pid, nm, v in top]
    tail = stat.topn_label or f"in {stat.noun}"
    text = f"Name the top {len(answers)} {tail} in {when(season, season_type)}{_qualifier(stat, season_type)}."
    return Question(text, stat, season, "topn", answers, season_type=season_type)


def gen_threshold(stat, season, rows, season_type="Regular Season"):
    """Only fires at curated milestones (25 PPG, 90% FT…) — never an arbitrary number."""
    if not stat.milestones:
        return None
    values = [v for _, _, v in rows]
    scale = detect_pct_scale(values)
    lo_n = max(2, ANSWER_BAND[0])   # 2+ answers; a single-answer club is just a superlative
    cands = []
    for m in stat.milestones:
        t = m / scale               # display units → stored units (60 → 0.60 for pct)
        c = count_at_least(values, t) if stat.high_is_good else count_at_most(values, t)
        if lo_n <= c <= ANSWER_BAND[1]:
            cands.append(t)
    if not cands:
        return None
    t = random.choice(cands)
    if stat.high_is_good:
        answers = [Answer(pid, nm, v) for pid, nm, v in rows if v >= t]
        bound = f"at least {fmt_threshold(stat, t, scale)}"
    else:
        answers = [Answer(pid, nm, v) for pid, nm, v in rows if v <= t]
        bound = f"no more than {fmt_threshold(stat, t, scale)}"
    subj = _subject(len(answers))
    text = f"{subj} {_verb(stat)} {bound} {stat.noun} in {when(season, season_type)}{_qualifier(stat, season_type)}?"
    return Question(text, stat, season, "threshold", answers, season_type=season_type)


def gen_range(stat, season, rows, season_type="Regular Season"):
    if not stat.allow_range:
        return None
    values = [v for _, _, v in rows]
    levels = nice_levels(values, stat.step)
    floor = statistics.median(values) if stat.high_end_only else float("-inf")
    cands = []
    for i in range(len(levels) - 1):
        lo = levels[i]
        hi = round(lo + stat.step, 4)   # a one-step-wide band
        if lo < floor:
            continue   # skip un-fun low-end bands (e.g. "2–4 MPG")
        if ANSWER_BAND[0] <= count_between(values, lo, hi) <= ANSWER_BAND[1]:
            cands.append((lo, hi))
    if not cands:
        return None
    lo, hi = random.choice(cands)
    scale = detect_pct_scale(values)
    answers = [Answer(pid, nm, v) for pid, nm, v in rows if lo <= v <= hi]
    subj = _subject(len(answers))
    text = (f"{subj} {_verb(stat)} between {fmt_threshold(stat, lo, scale)} and "
            f"{fmt_threshold(stat, hi, scale)} {stat.noun} in {when(season, season_type)}{_qualifier(stat, season_type)}?")
    return Question(text, stat, season, "range", answers, season_type=season_type)


def gen_superlative(stat, season, rows, season_type="Regular Season"):
    if not rows:
        return None
    high = stat.high_is_good
    ordered = rows if high else list(reversed(rows))
    top_val = ordered[0][2]
    answers = [Answer(pid, nm, v) for pid, nm, v in ordered if v == top_val]   # include ties
    if len(answers) > ANSWER_BAND[1]:
        return None
    label = when(season, season_type)
    qual = _qualifier(stat, season_type)
    if stat.sup_verb:                          # custom verb (charges, rim defense)
        text = f"Who {stat.sup_verb} {stat.noun} in {label}{qual}?"
    elif high and season_type == "Playoffs":   # avoid "led the playoffs … in 2025 playoffs"
        text = f"Who led the {label} in {stat.noun}{qual}?"
    elif high:
        text = f"Who led the NBA in {stat.noun} in {label}{qual}?"
    else:
        text = f"Who had the fewest {stat.noun} in {label}{qual}?"
    return Question(text, stat, season, "superlative", answers, season_type=season_type)


def gen_thisorthat(stat, season, rows, season_type="Regular Season"):
    """'Who averaged more X — A or B?' Pick 2 from the top of the leaderboard so both
    players are recognizable; the answer is whoever's higher (single answer)."""
    if len(rows) < 6:
        return None
    pool = rows[:min(15, len(rows))]            # top of the board → both notable
    a, b = random.sample(pool, 2)
    if a[2] == b[2]:
        return None
    winner = a if a[2] > b[2] else b            # stats here are all high-is-good (tier ≤ 1)
    comp = "had a higher" if stat.pct else "averaged more"
    # The two players are shown as headshot buttons in the UI, so they're carried in
    # `options` (shuffled) instead of being baked into the question text.
    text = f"Who {comp} {stat.noun} in {when(season, season_type)}?"
    opts = [Answer(a[0], a[1], a[2]), Answer(b[0], b[1], b[2])]
    random.shuffle(opts)
    return Question(text, stat, season, "thisorthat",
                    [Answer(winner[0], winner[1], winner[2])], season_type=season_type, options=opts)


def gen_any(stat, season, rows, season_type="Regular Season"):
    """'Name a player who averaged 25+ PPG…' — correct if you name ANY qualifier.
    Single-guess-friendly version of a club/threshold."""
    if not stat.milestones:
        return None
    values = [v for _, _, v in rows]
    scale = detect_pct_scale(values)
    cands = [m / scale for m in stat.milestones
             if 2 <= (count_at_least(values, m / scale) if stat.high_is_good
                      else count_at_most(values, m / scale)) <= 25]
    if not cands:
        return None
    t = random.choice(cands)
    if stat.high_is_good:
        answers = [Answer(pid, nm, v) for pid, nm, v in rows if v >= t]
        bound = f"at least {fmt_threshold(stat, t, scale)}"
    else:
        answers = [Answer(pid, nm, v) for pid, nm, v in rows if v <= t]
        bound = f"no more than {fmt_threshold(stat, t, scale)}"
    text = (f"Name a player who {_verb(stat)} {bound} {stat.noun} "
            f"in {when(season, season_type)}{_qualifier(stat, season_type)}.")
    return Question(text, stat, season, "any", answers, season_type=season_type)


GENERATORS = {
    "topn":        gen_topn,
    "threshold":   gen_threshold,
    "range":       gen_range,   # retired from rotation, kept for --op testing
    "superlative": gen_superlative,
    "thisorthat":  gen_thisorthat,
    "any":         gen_any,
}


# ── "club" questions: multi-condition membership (e.g. 50/40/90) ──────────────

@dataclass
class Club:
    key:    str
    label:  str          # rare clubs: "a 50/40/90 shooting season"; any-clubs: "20+ PPG and 10+ RPG"
    where:  str          # SQL conditions on player_seasons (alias ps)
    short:  str
    is_any: bool = False  # True → many qualifiers, framed as "name a player who {label}" (operator "any")
    select: str = ""      # rare clubs: extra cols for the slash-line display
    display: "callable" = None
    min_season: str = None

CLUBS = [
    Club(
        key="503090",
        label="a 50/40/90 shooting season",
        where=("ps.fg_pct>=0.5 AND ps.fg3_pct>=0.4 AND ps.ft_pct>=0.9 "
               "AND ps.fga>=8 AND ps.fg3a>=3 AND ps.fta>=2"),
        short="50/40/90",
        select="ps.fg_pct, ps.fg3_pct, ps.ft_pct",
        display=lambda r: f"{r['fg_pct']*100:.0f}/{r['fg3_pct']*100:.0f}/{r['ft_pct']*100:.0f}",
    ),
    Club(
        key="triple_double",
        label="a triple-double average",
        where="ps.pts>=10 AND ps.reb>=10 AND ps.ast>=10",
        short="TD avg",
        select="ps.pts, ps.reb, ps.ast",
        display=lambda r: f"{r['pts']:.1f}/{r['reb']:.1f}/{r['ast']:.1f}",
    ),
    # "name a player who …" clubs — many qualifiers, real answer variety
    Club(key="20_10",  label="averaged 20+ points and 10+ rebounds", short="20/10",
         where="ps.pts>=20 AND ps.reb>=10", is_any=True),
    Club(key="20_5_5", label="averaged 20+ points, 5+ rebounds and 5+ assists", short="20/5/5",
         where="ps.pts>=20 AND ps.reb>=5 AND ps.ast>=5", is_any=True),
    Club(key="25_5_5", label="averaged 25+ points, 5+ rebounds and 5+ assists", short="25/5/5",
         where="ps.pts>=25 AND ps.reb>=5 AND ps.ast>=5", is_any=True),
]
SINGLE_CLUBS = [c for c in CLUBS if not c.is_any]
ANY_CLUBS    = [c for c in CLUBS if c.is_any]


def gen_club(conn, club: Club, seasons, lo=None, hi=None):
    """Find a season where players are in the club; frame as 'who had…' (rare, single)
    or 'name a player who…' (an any-club with many qualifiers). `lo`/`hi` limit the era."""
    pool = [s for s in seasons if (club.min_season is None or s >= club.min_season)
            and (lo is None or int(s[:4]) >= lo) and (hi is None or int(s[:4]) < hi)]
    random.shuffle(pool)
    sel = (", " + club.select) if club.select else ""
    for season in pool:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT ps.player_id, p.player_name AS name{sel}
                FROM   player_seasons ps
                JOIN   players p ON p.player_id = ps.player_id
                WHERE  ps.season = %s AND ps.season_type = %s AND ps.gp >= %s
                  AND  {club.where}
            """, (season, SEASON_TYPE, MIN_GP))
            rows = cur.fetchall()
        cstat = Stat(club.key, club.label, club.short, 1)   # synthetic, for display
        if club.is_any:
            if 2 <= len(rows) <= 25:
                answers = [Answer(r["player_id"], r["name"], 0.0) for r in rows]
                text = f"Name a player who {club.label} in {season}?"
                return Question(text, cstat, season, "any", answers, season_type="Regular Season")
        elif 1 <= len(rows) <= ANSWER_BAND[1]:
            answers = [Answer(r["player_id"], r["name"], 0.0,
                              display=club.display(r) if club.display else None) for r in rows]
            subj = _subject(len(answers))
            text = f"{subj} had {club.label} in {season}?"
            return Question(text, cstat, season, "club", answers)
    return None


# ── team-scoped questions: "Who led the [team] in [stat]?" ────────────────────

def _teams_for(conn, season, min_players=5):
    """Team abbrs that had >= min_players qualified players that season."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT team_abbr FROM player_seasons
            WHERE season = %s AND season_type = %s AND gp >= %s AND team_abbr IS NOT NULL
            GROUP BY team_abbr HAVING COUNT(*) >= %s
        """, (season, SEASON_TYPE, MIN_GP, min_players))
        return [r["team_abbr"] for r in cur.fetchall()]


# Stats you'd actually know a team's leader in — marquee box stats only.
# (Turnovers excluded: "led the team in turnovers" is an unsatisfying, un-guessable fact.)
# Stats for team-leader questions. pts/reb/ast/P+R+A have a memorable leader (the star);
# steals/blocks/made-threes leaders are often obscure role players, so those are floored to
# at least medium (see score_difficulty) — fun deep cuts, but never gimmes.
_TEAM_STATS = {"pts", "reb", "ast", "pra", "fg3m", "stl", "blk"}
_TEAM_SECONDARY = {"stl", "blk", "fg3m"}

def gen_team(conn, seasons, force_stat=None, lo=None, hi=None):
    """A team-scoped single-answer question: 'Who led the Warriors in PPG in 2016-17?'
    `lo`/`hi` constrain the season's start year (e.g. hi=2010 for old-roster questions)."""
    candidates = [s for s in STAT_POOL if s.key in _TEAM_STATS and s.high_is_good and not s.superlative_only]
    stat = next((s for s in STAT_POOL if s.key == force_stat), None) or random.choice(candidates)
    pool = [s for s in seasons if (stat.min_season is None or s >= stat.min_season)
            and (lo is None or int(s[:4]) >= lo) and (hi is None or int(s[:4]) < hi)]
    random.shuffle(pool)
    for season in pool:
        teams = _teams_for(conn, season)
        random.shuffle(teams)
        for team in teams[:6]:
            rows = load_qualified(conn, stat, season, team=team)
            if not rows:
                continue
            top_val = rows[0][2]
            answers = [Answer(*r) for r in rows if r[2] == top_val]
            if len(answers) == 1:                              # clean single leader (skip ties)
                text = f"Who led the {team_name(team)} in {stat.noun} in {season}?"
                return Question(text, stat, season, "team", answers, team=team)
    return None


# ── award questions: "Who won MVP in 2015-16?" (from the awards[] column) ─────

@dataclass
class Award:
    key:    str     # label stored in player_seasons.awards
    name:   str     # phrasing in the question
    single: bool    # one winner ("who won…") vs many ("name an All-Star")

AWARDS = [
    Award("MVP",        "MVP",                           True),
    Award("ROTY",       "Rookie of the Year",            True),
    Award("DPOY",       "Defensive Player of the Year",  True),
    Award("6MOY",       "Sixth Man of the Year",         True),
    Award("MIP",        "Most Improved Player",          True),
    Award("Finals MVP", "Finals MVP",                    True),
    Award("All-Star",   "an All-Star",                   False),
]


def gen_award(conn, seasons, only=None, lo=None, hi=None):
    """Pick an award and a season it was given, and ask about the winner(s).
    `only`: restrict to these award keys; `lo`/`hi`: restrict the season's start year."""
    pool = [a for a in AWARDS if only is None or a.key in only]
    random.shuffle(pool)
    for award in pool:
        with conn.cursor() as cur:
            cur.execute("""SELECT DISTINCT season FROM player_seasons
                           WHERE season_type='Regular Season' AND %s = ANY(awards)""", (award.key,))
            avail = [r["season"] for r in cur.fetchall()]
            avail = [s for s in avail if (lo is None or int(s[:4]) >= lo)
                     and (hi is None or int(s[:4]) < hi)]
            if not avail:
                continue
            season = random.choice(avail)
            cur.execute("""SELECT ps.player_id, p.player_name AS name
                           FROM player_seasons ps JOIN players p ON p.player_id = ps.player_id
                           WHERE ps.season=%s AND ps.season_type='Regular Season' AND %s = ANY(ps.awards)""",
                        (season, award.key))
            rows = cur.fetchall()
        if not rows:
            continue
        answers = [Answer(r["player_id"], r["name"], 0.0, display=award.key) for r in rows]
        if award.single:
            answers = answers[:1]
            text = f"Who won {award.name} in {season}?"
        else:
            text = f"Name {award.name} from {season}?"
        cstat = Stat(f"award_{award.key}", award.name, award.key, 1)  # synthetic, for display
        return Question(text, cstat, season, "award", answers, season_type="Regular Season")
    return None


# ── This-or-that generation (the whole game) ─────────────────────────────────
# Every Survival question is "Who [did more X] — A or B?": two players, tap one. Far
# more variety than superlatives (pairs × axes × seasons) and a judgment, not recall.
# Three axes: a single-season stat, a career total, and an accolade count.
#
# Career/accolade axes are restricted to players who DEBUTED in 1997-98+ — our data
# starts at 1996-97, so anyone earlier has a truncated total/count (wrong answers).

_DEBUT_FLOOR  = "1997-98"
# Season axis uses only intuitive, recognizable stats (box score + shooting %s) — not
# plus-minus or tracking metrics, which make for noisy/coinflip "who averaged more" calls.
_TOOT_SEASON  = {"pts", "reb", "ast", "stl", "blk", "fg3m", "pra",
                 "ts_pct", "fg3_pct", "fg_pct", "ft_pct"}
_CAREER_STATS = [("pts", "career points"), ("reb", "career rebounds"),
                 ("ast", "career assists"), ("stl", "career steals"),
                 ("blk", "career blocks"), ("fg3m", "career made threes")]
_ACCOLADES    = [("All-Star", "made more", "All-Star teams"),
                 ("MVP", "won more", "MVPs"),
                 ("DPOY", "won more", "Defensive Player of the Year awards")]
_CAREER_CACHE   = {}    # col   -> [(player_id, name, total)] desc, complete-career players
_ACCOLADE_CACHE = {}    # award -> [(player_id, name, count)] desc


def _career_board(conn, col):
    if col not in _CAREER_CACHE:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT ps.player_id, p.player_name AS name, SUM(ps.{col} * ps.gp) AS total
                FROM   player_seasons ps JOIN players p ON p.player_id = ps.player_id
                WHERE  ps.season_type = 'Regular Season' AND ps.{col} IS NOT NULL
                GROUP BY ps.player_id, p.player_name
                HAVING SUM(ps.gp) >= 200 AND MIN(ps.season) >= %s
                ORDER BY total DESC LIMIT 150
            """, (_DEBUT_FLOOR,))
            _CAREER_CACHE[col] = [(r["player_id"], r["name"], float(r["total"])) for r in cur.fetchall()]
    return _CAREER_CACHE[col]


def _accolade_board(conn, award):
    if award not in _ACCOLADE_CACHE:
        with conn.cursor() as cur:
            cur.execute("""
                WITH counts AS (
                    SELECT player_id, COUNT(*) AS n FROM player_seasons
                    WHERE season_type='Regular Season' AND %s = ANY(awards) GROUP BY player_id),
                debut AS (
                    SELECT player_id, MIN(season) AS d FROM player_seasons
                    WHERE season_type='Regular Season' GROUP BY player_id)
                SELECT c.player_id, p.player_name AS name, c.n
                FROM counts c JOIN debut d ON d.player_id = c.player_id
                             JOIN players p ON p.player_id = c.player_id
                WHERE d.d >= %s
                ORDER BY c.n DESC
            """, (award, _DEBUT_FLOOR))
            _ACCOLADE_CACHE[award] = [(r["player_id"], r["name"], int(r["n"])) for r in cur.fetchall()]
    return _ACCOLADE_CACHE[award]


def _toot(text, statkey, a, b, season="career", season_type="Career"):
    """Build a this-or-that Question from two (id, name, value) rows."""
    winner = a if a[2] > b[2] else b
    opts = [Answer(a[0], a[1], a[2]), Answer(b[0], b[1], b[2])]
    random.shuffle(opts)
    return Question(text, Stat(statkey, "", "", 1), season, "thisorthat",
                    [Answer(winner[0], winner[1], winner[2])], season_type=season_type, options=opts)


def _toot_season(conn, seasons):
    cands = [s for s in STAT_POOL if s.key in _TOOT_SEASON]
    for stat in random.sample(cands, k=len(cands)):
        elig = [s for s in seasons if not stat.min_season or s >= stat.min_season]
        if not elig:
            continue
        season = random.choice(elig)
        rows = load_qualified(conn, stat, season)
        if len(rows) < 8:
            continue
        a, b = random.sample(rows[:15], 2)          # top of the board → both recognizable
        if a[2] == b[2]:
            continue
        comp = "had a higher" if stat.pct else "averaged more"
        return _toot(f"Who {comp} {stat.noun} in {when(season, 'Regular Season')}?",
                     stat.key, a, b, season, "Regular Season")
    return None


def _toot_career(conn):
    col, noun = random.choice(_CAREER_STATS)
    rows = _career_board(conn, col)
    if len(rows) < 30:
        return None
    i = random.randrange(0, len(rows) - 1)
    j = random.randrange(i + 1, min(i + 40, len(rows)))   # nearby ranks = a closer, fairer call
    a, b = rows[i], rows[j]
    return None if a[2] == b[2] else _toot(f"Who has more {noun}?", "career_" + col, a, b)


def _toot_accolade(conn):
    award, verb, noun = random.choice(_ACCOLADES + [("All-Star", "made more", "All-Star teams")] * 2)
    rows = [r for r in _accolade_board(conn, award) if r[2] >= 1]
    if len(rows) < 6:
        return None
    for _ in range(12):
        a, b = random.sample(rows, 2)
        if a[2] != b[2]:
            return _toot(f"Who {verb} {noun}?", "acc_" + award, a, b)
    return None


def generate_thisorthat(conn, seasons, exclude=None, tries=40):
    """A this-or-that question whose winner isn't in `exclude` (answer player_ids already
    used this run). Picks an axis at random: ~50% season stat, ~30% career, ~20% accolade."""
    exclude = exclude or set()
    for _ in range(tries):
        r = random.random()
        q = _toot_season(conn, seasons) if r < 0.50 else _toot_career(conn) if r < 0.80 else _toot_accolade(conn)
        if q and q.answers[0].player_id not in exclude:
            return q
    return _toot_season(conn, seasons)


# ── difficulty model ─────────────────────────────────────────────────────────

def score_difficulty(q):
    """Return 'easy' | 'medium' | 'hard' from the question's STRUCTURE only —
    how many players you must name (question type) and how niche the stat is.
    Deterministic; no per-answer lookups."""
    n = q.n
    # Accessible-by-construction formats:
    if q.operator == "thisorthat":             # a 2-way pick (50/50 floor)
        return "easy" if int(q.season[:4]) >= 2010 else "medium"
    if q.operator == "any":                    # name ANY qualifier — easier with more options
        return "easy" if n >= 4 else "medium"
    if q.operator == "award":                  # awards: name-any (All-Star) easy; winners by era
        if q.stat.key == "award_MIP":          # Most Improved is obscure regardless of era
            return "hard"
        if n > 1:
            return "easy"
        year = int(q.season[:4])
        return "easy" if year >= 2015 else "medium" if year >= 2008 else "hard"
    # Advanced/efficiency stats (play-type PPP, rim defense) are hard even for a single
    # leader — knowing the transition-PPP leader is deep knowledge, unlike "led in PPG".
    if getattr(q.stat, "tier", 1) >= 2:
        return "hard"
    # Full leaderboards take deep recall → hard:
    #   • naming the top 4–5 league-wide (the 4th/5th name is brutal), or
    #   • a specific team's 2nd/3rd (top 3 on a roster for a given year).
    if n >= 4 or (q.operator == "team" and n >= 3):
        return "hard"

    # (1) how many players you must name — one is easy, a few is medium-ish
    s = {1: -2, 2: 0, 3: 1}[n]

    # (2) stat niche level — common box stat (0), medium (1), advanced/play-type (2)
    s += getattr(q.stat, "tier", 1)

    # (3) older seasons are harder to recall
    year = int(q.season[:4])
    if   year < 2005: s += 2
    elif year < 2014: s += 1

    result = "easy" if s <= 0 else "hard" if s >= 4 else "medium"
    if q.operator == "club" and result == "easy":
        result = "medium"                  # club membership (50/40/90…) is a knowledge flex
    if q.operator == "team":
        secondary = q.stat.key in _TEAM_SECONDARY
        if secondary and year < 2010:
            return "hard"                  # obscure stat + obscure old roster — a real deep cut
        if result == "easy" and (secondary or year < 2010):
            result = "medium"              # one of the two → not a gimme
    return result


# ── top-level generation ─────────────────────────────────────────────────────

def generate_question(conn, seasons, force_stat=None, force_op=None, max_tries=40):
    # Special "club" questions (50/40/90, etc.) — occasional, or forced for testing.
    club = next((c for c in CLUBS if c.key == force_stat), None)
    if club or (not force_stat and not force_op and CLUBS and random.random() < 0.04):
        q = gen_club(conn, club or random.choice(CLUBS), seasons)
        if q:
            q.fmt = "single" if q.n == 1 else "set"
            q.difficulty = score_difficulty(q)
            return q
        if club:
            return None

    # Team-scoped questions ("Who led the Lakers in PPG…") — occasional, or forced.
    if force_op == "team" or (not force_stat and not force_op and random.random() < 0.20):
        q = gen_team(conn, seasons)
        if q:
            q.fmt = "single" if q.n == 1 else "set"
            q.difficulty = score_difficulty(q)
            return q
        if force_op == "team":
            return None

    # Award questions ("Who won MVP in 2015-16?") — occasional, or forced. (No-op until
    # the awards[] column is backfilled, then it lights up automatically.)
    if force_op == "award" or (not force_stat and not force_op and random.random() < 0.12):
        q = gen_award(conn, seasons)
        if q:
            q.fmt = "single" if q.n == 1 else "set"
            q.difficulty = score_difficulty(q)
            return q
        if force_op == "award":
            return None

    for _ in range(max_tries):
        stat = next((s for s in STAT_POOL if s.key == force_stat), None) if force_stat else random.choice(STAT_POOL)
        if stat is None:
            return None
        # Tracking/hustle stats only exist for recent seasons — don't spin them for 2003-04.
        reg_pool = [s for s in seasons if stat.min_season is None or s >= stat.min_season]
        po_pool  = [s for s in playoff_seasons(conn) if stat.min_season is None or s >= stat.min_season]
        if po_pool and random.random() < PLAYOFF_PROB:
            season, stype = random.choice(po_pool), "Playoffs"
        elif reg_pool:
            season, stype = random.choice(reg_pool), "Regular Season"
        else:
            continue
        # Single-answer formats only (the game uses one-guess questions). top-N and
        # threshold ("name all who…") are excluded — kept callable via --op for testing.
        if stat.superlative_only:
            choices = [("superlative", 1.0)]
        else:
            choices = [("superlative", 0.45)]
            if stat.tier <= 1:                         # intuitive stats only
                choices.append(("thisorthat", 0.35))   # "who had more — A or B?"
                if stat.milestones:
                    choices.append(("any", 0.30))      # "name a player who 25+ PPG…"
        op = force_op or random.choices([c for c, _ in choices], weights=[w for _, w in choices])[0]
        rows = load_qualified(conn, stat, season, season_type=stype)
        if len(rows) < ANSWER_BAND[0]:
            continue
        q = GENERATORS[op](stat, season, rows, season_type=stype)
        if q is None:
            continue
        # "any" answer sets can be large (you only name one); others stay within the band.
        if q.operator == "any":
            if q.n < 2:
                continue
        elif not (ANSWER_BAND[0] <= q.n <= ANSWER_BAND[1]):
            continue
        q.fmt = "single" if q.n == 1 else "set"
        q.difficulty = score_difficulty(q)
        return q
    return None


def generate_targeted(conn, seasons, difficulty, safe=False, asked=None,
                      avoid_stats=None, avoid_answers=None, avoid_teams=None, tries=40):
    """Construct a single-answer question OF a target difficulty directly — pick inputs
    known to produce it, instead of generating randomly and filtering. Used by Survival's
    difficulty ramp so there's no guess-and-check loop. `avoid_stats`/`avoid_answers` keep
    a run from repeating a stat/club type or the same answer player."""
    po = playoff_seasons(conn)
    t0 = [s for s in STAT_POOL if s.tier == 0]
    t1 = [s for s in STAT_POOL if s.tier == 1]

    def season_for(stat, lo=None, hi=None, allow_po=True):
        reg = [s for s in seasons if (stat.min_season is None or s >= stat.min_season)
               and (lo is None or int(s[:4]) >= lo) and (hi is None or int(s[:4]) < hi)]
        pos = [s for s in po if stat.min_season is None or s >= stat.min_season] if allow_po else []
        opts = [(s, "Regular Season") for s in reg] + [(s, "Playoffs") for s in pos]
        return random.choice(opts) if opts else None

    def build(stat, gen, lo=None, hi=None, allow_po=True):
        sel = season_for(stat, lo, hi, allow_po)
        if not sel:
            return None
        season, stype = sel
        rows = load_qualified(conn, stat, season, season_type=stype)
        return gen(stat, season, rows, stype) if len(rows) >= 2 else None

    fallback = None
    for _ in range(tries):
        if safe:                                              # gimmes — varied but all easy & recent
            r = random.random()
            if   r < 0.42:                                     # recent league leader (marquee stat)
                q = build(random.choice([s for s in t0 if s.key in ("pts", "reb", "ast", "fg3m")]),
                          gen_superlative, lo=2018, allow_po=False)
            elif r < 0.64:                                     # this-or-that between two recent stars
                q = build(random.choice([s for s in t0 if s.key in ("pts", "reb", "ast")]),
                          gen_thisorthat, lo=2018, allow_po=False)
            elif r < 0.82:                                     # recent MVP / All-Star
                q = gen_award(conn, seasons, only=["MVP", "All-Star"], lo=2018)
            else:                                             # "name a recent 20/10 / 20-5-5 player"
                q = gen_club(conn, random.choice(ANY_CLUBS), seasons, lo=2015)
        elif difficulty == "hard":
            r = random.random()
            if   r < 0.30:                                     # play-type PPP — hard but star-led
                q = build(random.choice([s for s in STAT_POOL
                          if s.key in ("iso_ppp", "pnr_bh_ppp", "pnr_roll_ppp", "post_ppp")]), gen_superlative)
            elif r < 0.50:                                     # famous old award, or MIP (any era)
                q = (gen_award(conn, seasons, only=["MIP"]) if random.random() < 0.3
                     else gen_award(conn, seasons, hi=2008))
            elif r < 0.75:                                     # old-roster deep cut (steals/blocks/threes)
                q = gen_team(conn, seasons, force_stat=random.choice(["stl", "blk", "fg3m"]), hi=2010)
            else:                                             # the brutal obscure-leader stats
                q = build(random.choice([s for s in STAT_POOL
                          if s.key in ("charges_total", "contested_shots", "deflections", "def_rim_fg_pct")]),
                          gen_superlative)
        elif difficulty == "easy":                            # tier-0 = always easy
            r = random.random()
            if   r < 0.35: q = build(random.choice(t0), gen_superlative)
            elif r < 0.55: q = gen_team(conn, seasons)
            elif r < 0.70: q = gen_award(conn, seasons)
            elif r < 0.85: q = build(random.choice(t0 + t1), gen_thisorthat, lo=2010)
            else:          q = gen_club(conn, random.choice(ANY_CLUBS), seasons)   # "name a 20/10 player"
        else:                                                 # medium — lean on the deep pool
            r = random.random()
            if   r < 0.08: q = gen_club(conn, random.choice(SINGLE_CLUBS), seasons) # 50/40/90, triple-double
            elif r < 0.25: q = build(random.choice(t1), gen_superlative, hi=2005)   # 90s shooting (narrow)
            elif r < 0.55: q = gen_award(conn, seasons)                             # awards (varied)
            else:          q = gen_team(conn, seasons, hi=2010)                     # old team leaders (huge)
        if q is None or (q.n != 1 and q.operator not in ("any", "award")):
            continue
        if (asked and q.text in asked) or (avoid_stats and (q.operator, q.stat.key) in avoid_stats):
            continue                           # block repeating a format+stat (e.g. two 50/40/90)
        if avoid_answers and q.n == 1 and q.answers[0].player_id in avoid_answers:
            continue                           # don't ask back-to-back about the same player
        if avoid_teams and q.team and q.team in avoid_teams:
            continue                           # at most one question per team in a run
        q.fmt = "single" if q.n == 1 else "set"
        q.difficulty = score_difficulty(q)
        if q.difficulty == difficulty or (safe and q.difficulty == "easy"):
            return q
        fallback = fallback or q
    return fallback


# ── interactive play mode ────────────────────────────────────────────────────

def play(conn, seasons, args):
    print("\n🏀  ydkball trivia — prototype")
    print("    Type a player's name to answer. Commands: 'reveal', 'skip', 'quit'\n")
    bank = load_player_bank(conn)
    rounds = correct = possible = 0
    try:
        while True:
            q = generate_question(conn, seasons, force_stat=args.stat, force_op=args.op)
            if q is None:
                continue
            rounds += 1
            possible += q.n
            scale = detect_pct_scale([a.value for a in q.answers])
            answer_ids = {a.player_id for a in q.answers}
            ans_by_id = {a.player_id: a for a in q.answers}
            print("─" * 64)
            hint = "name the player" if q.fmt == "single" else f"name all {q.n}"
            print(f"Q{rounds}  ({q.difficulty}, {hint})")
            print(f"  {q.text}\n")

            found, stop = set(), False
            while len(found) < q.n and not stop:
                prog = "" if q.fmt == "single" else f"[{len(found)}/{q.n}] "
                try:
                    guess = input(f"  {prog}> ").strip()
                except EOFError:
                    stop = True; break
                cmd = guess.lower()
                if cmd in ("quit", "q", "exit"):
                    print_summary(rounds - 1, correct, possible - q.n); conn.close(); return
                if cmd in ("skip", "reveal", "r", "s", ""):
                    stop = True; break
                res = resolve_player(guess, bank)
                if res is None:
                    print("    ?  no player found — try a fuller name")
                    continue
                rid, rname = res
                if rid not in answer_ids:
                    print(f"    ✗  {rname} — not in the set")
                elif rid in found:
                    print(f"    •  already got {rname}")
                else:
                    found.add(rid)
                    correct += 1
                    a = ans_by_id[rid]
                    print(f"    ✓  {rname}  ({_ans_label(q, a, scale)})")

            missed = [a for a in q.answers if a.player_id not in found]
            if missed:
                names = ", ".join(f"{a.name} ({_ans_val(q, a, scale)})"
                                  for a in sorted(missed, key=lambda x: -x.value))
                print(f"    missed: {names}")
            print(f"    score: {len(found)}/{q.n}\n")
            try:
                cont = input("  [enter] next  ·  [q] quit > ").strip().lower()
            except EOFError:
                break
            if cont in ("q", "quit", "exit"):
                break
    finally:
        print_summary(rounds, correct, possible)
        conn.close()


def print_summary(rounds, correct, possible):
    print("═" * 64)
    pct = f"{100*correct/possible:.0f}%" if possible else "—"
    print(f"  Played {rounds} questions · {correct}/{possible} answers ({pct})")
    print("═" * 64)


def print_stat_pool():
    print(f"\nStat pool — {len(STAT_POOL)} stats currently in rotation:\n")
    print(f"  {'':<6}{'column':<16}{'phrasing':<26}{'era':<12}notes")
    for s in STAT_POOL:
        notes = []
        if s.vol_col: notes.append(f"needs ≥{s.vol_min:g} {s.vol_col}")
        if not s.allow_range: notes.append("no ranges")
        era = f"{s.min_season}+" if s.min_season else "all 30 yrs"
        print(f"  {s.short:<6}{s.key:<16}{s.noun:<26}{era:<12}{' · '.join(notes)}")
    print(f"\n  Available to add: standard advanced (off/def/net rating, PIE, AST%, REB%) span")
    print(f"  all 30 yrs; more tracking/hustle/play-type (box-outs, charges, ISO/PnR PPP,")
    print(f"  LEBRON, WAR) exist 2020-21+. Note: touches, darko, ws columns are empty.\n")


# ── difficulty audit ─────────────────────────────────────────────────────────

def audit(conn, seasons, n):
    """Generate n questions and show the easy/medium/hard split + examples each."""
    buckets = {"easy": [], "medium": [], "hard": []}
    for _ in range(n):
        q = generate_question(conn, seasons)
        if q:
            buckets[q.difficulty].append(q)
    total = sum(len(v) for v in buckets.values())
    print(f"\nDifficulty distribution over {total} questions:\n")
    for d in ("easy", "medium", "hard"):
        qs = buckets[d]
        pct = 100 * len(qs) / total if total else 0
        bar = "█" * round(pct / 2)
        print(f"  {d:<7} {len(qs):>4} ({pct:4.0f}%)  {bar}")
        for q in qs[:4]:
            print(f"            • {q.text}")
        print()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", action="store_true", help="interactive play mode")
    ap.add_argument("--list-stats", action="store_true", help="show the stat pool and exit")
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--stat", type=str, default=None)
    ap.add_argument("--op", type=str, default=None, choices=list(GENERATORS) + ["team", "award"])
    ap.add_argument("--show-answers", action="store_true")
    ap.add_argument("--audit", type=int, default=0, metavar="N",
                    help="generate N questions and report the difficulty distribution")
    args = ap.parse_args()

    if args.list_stats:
        print_stat_pool()
        return

    if args.seed is not None:
        random.seed(args.seed)

    conn = get_conn()
    seasons = list_seasons(conn)

    if args.play:
        play(conn, seasons, args)
        return

    if args.audit:
        audit(conn, seasons, args.audit)
        return

    print(f"seasons available: {len(seasons)} ({seasons[0]}…{seasons[-1]})  "
          f"min_gp={MIN_GP}  band={ANSWER_BAND}\n")

    made = 0
    while made < args.count:
        q = generate_question(conn, seasons, force_stat=args.stat, force_op=args.op)
        if q is None:
            print("  (no valid question this spin)")
            continue
        made += 1
        tag = "NAME ONE " if q.fmt == "single" else f"NAME {q.n:>2}"
        print(f"[{tag} · {q.difficulty:<6} · {q.operator}] {q.text}")
        if args.show_answers:
            scale = detect_pct_scale([a.value for a in q.answers])
            for a in sorted(q.answers, key=lambda x: -x.value):
                print(f"      • {a.name:<24} {_ans_label(q, a, scale)}")
        else:
            top = sorted(q.answers, key=lambda x: -x.value)[0]
            scale = detect_pct_scale([a.value for a in q.answers])
            preview = f"e.g. {top.name} ({_ans_val(q, top, scale)})" if q.fmt == "set" else f"{top.name} ({_ans_val(q, top, scale)})"
            print(f"      → {preview}")
        print()

    conn.close()


if __name__ == "__main__":
    main()
