"""
Estimate how many distinct questions exist per difficulty.

Counts the concrete, countable question types (superlative, team leader, award, club,
"name any" milestone) exactly-ish, and reports the combinatorial "this or that" pool
separately (it's effectively unbounded — pairs drawn from each leaderboard).

    python backend/games/count_pool.py
"""

from collections import Counter, defaultdict
import question_engine as qe

conn = qe.get_conn()
REG = qe.list_seasons(conn, "Regular Season")
PO  = qe.playoff_seasons(conn)

diff   = Counter()                  # difficulty -> count
bytype = defaultdict(Counter)       # type -> {difficulty: count}


def _q(op, stat, season, stype="Regular Season", n=1):
    return qe.Question("", stat, season, op, [qe.Answer(0, "", 0.0)] * n, season_type=stype)

def tally(label, op, stat, season, stype="Regular Season", n=1, mult=1):
    d = qe.score_difficulty(_q(op, stat, season, stype, n))
    diff[d] += mult
    bytype[label][d] += mult


# 1) Superlatives — every stat × every eligible season (regular + playoffs)
for s in qe.STAT_POOL:
    for season in (x for x in REG if not s.min_season or x >= s.min_season):
        tally("superlative", "superlative", s, season)
    for season in (x for x in PO if not s.min_season or x >= s.min_season):
        tally("superlative", "superlative", s, season, "Playoffs")

# 2) Team leaders — team-stats × season × (teams that season)
with conn.cursor() as cur:
    cur.execute("""SELECT season, COUNT(DISTINCT team_abbr) n FROM player_seasons
                   WHERE season_type='Regular Season' AND gp>=%s GROUP BY season""", (qe.MIN_GP,))
    teams_per = {r["season"]: r["n"] for r in cur.fetchall()}
for s in (st for st in qe.STAT_POOL if st.key in qe._TEAM_STATS):
    for season in REG:
        tally("team leader", "team", s, season, mult=teams_per.get(season, 30))

# 3) Awards — each award × the seasons it was given
for award in qe.AWARDS:
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT season FROM player_seasons
                       WHERE season_type='Regular Season' AND %s = ANY(awards)""", (award.key,))
        seasons = [r["season"] for r in cur.fetchall()]
    cstat = qe.Stat(f"award_{award.key}", award.name, award.key, 1)
    for season in seasons:
        tally("award", "award", cstat, season, n=(1 if award.single else 10))

# 4) Clubs — each club × the seasons with a valid set
for club in qe.CLUBS:
    with conn.cursor() as cur:
        cur.execute(f"""SELECT season, COUNT(*) c FROM player_seasons ps
                        WHERE season_type='Regular Season' AND gp>=%s AND {club.where}
                        GROUP BY season""", (qe.MIN_GP,))
        rows = cur.fetchall()
    cstat = qe.Stat(club.key, club.label, club.short, 1)
    for r in rows:
        c = r["c"]
        if club.is_any and 2 <= c <= 25:
            tally("club", "any", cstat, r["season"], n=c)
        elif not club.is_any and 1 <= c <= qe.ANSWER_BAND[1]:
            tally("club", "club", cstat, r["season"], n=c)

# 5) "name any" milestone questions — milestone stat × season × valid milestones
milestone_stats = [s for s in qe.STAT_POOL if s.milestones and s.tier <= 1]
for s in milestone_stats:
    seasons = [x for x in REG if not s.min_season or x >= s.min_season]
    for season in seasons:
        rows = qe.load_qualified(conn, s, season)
        vals = [v for _, _, v in rows]
        if not vals:
            continue
        scale = qe.detect_pct_scale(vals)
        for m in s.milestones:
            t = m / scale
            c = sum(1 for v in vals if v >= t)
            if 2 <= c <= 25:
                tally("name-any", "any", s, season, n=c)

# ── report ───────────────────────────────────────────────────────────────────
total = sum(diff.values())
print("\nDISTINCT QUESTIONS BY DIFFICULTY (countable types):\n")
for d in ("easy", "medium", "hard"):
    bar = "█" * round(40 * diff[d] / total)
    print(f"  {d:<7} {diff[d]:>6,}  ({100*diff[d]/total:4.0f}%)  {bar}")
print(f"  {'TOTAL':<7} {total:>6,}")

print("\nby question type:")
for label, c in sorted(bytype.items(), key=lambda kv: -sum(kv[1].values())):
    tot = sum(c.values())
    print(f"  {label:<13} {tot:>6,}   (easy {c['easy']:,} · med {c['medium']:,} · hard {c['hard']:,})")

# this-or-that is combinatorial — report separately
tot_pairs_pools = sum(
    1 for s in qe.STAT_POOL if s.tier <= 1
    for season in REG if not s.min_season or season >= s.min_season
)
print(f"\nplus 'this or that': ~{tot_pairs_pools:,} stat-season leaderboards, each yielding")
print(f"  ~100 distinct A-vs-B pairings → tens of thousands more (mostly easy/medium).")
conn.close()
