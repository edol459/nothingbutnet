"""
health_check.py — Daily data-integrity verifier + morning report.
=================================================================

Connects to the production DB (DATABASE_URL) and verifies that the daily
pipelines actually did their job, that today's games are generated, and that
nothing looks anomalous versus yesterday. Produces a human-readable morning
report with a prioritized to-do list.

    python backend/ingest/health_check.py            # run checks, print + save report
    python backend/ingest/health_check.py --no-snapshot   # don't write today's snapshot
    python backend/ingest/health_check.py --quiet    # only print the report, no progress

Exit code: 0 if no FAILs (WARN is allowed), 1 if any FAIL — so a cron/agent can
detect a bad day from the exit status alone.

Design notes
------------
- Strictly read-only against your data tables. The ONLY thing it writes is its
  own `health_snapshots` table (additive, CREATE TABLE IF NOT EXISTS), used to
  detect day-over-day anomalies (e.g. a fetch that half-failed and dropped rows).
- Column-existence is introspected at runtime, so freshness checks light up
  automatically as tables gain/lose timestamp columns. `player_seasons` has no
  updated_at today, so it is covered by snapshot anomaly detection instead.
- Season-aware: NBA freshness is only enforced in-season; WNBA likewise.
- Daily puzzles (Higher-or-Lower + Guess Who) are LAZILY generated on first play
  — there is no puzzle_gen cron. So "no puzzle yet today" before anyone has played
  is normal, not a failure; the puzzle checks are informational, and an EMPTY
  payload (generator ran but produced nothing) is the only real breakage.
"""

import os
import sys
import json
import argparse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def _db_url():
    """Railway exposes DATABASE_URL (internal) and DATABASE_PUBLIC_URL (proxy).
    Prefer the internal one; fall back to public so a service that only has the
    public var (or an empty DATABASE_URL) still connects."""
    return os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")


# ── Severity levels ───────────────────────────────────────────────────────────
OK, WARN, FAIL, INFO = "OK", "WARN", "FAIL", "INFO"
ICON = {OK: "✅", WARN: "🟡", FAIL: "🔴", INFO: "ℹ️"}

# ── Tables whose row counts we snapshot daily for anomaly detection ───────────
# A sudden drop in any of these means a fetch step half-failed.
SNAPSHOT_TABLES = [
    "players", "player_seasons", "player_gamelogs", "player_pctiles",
    "player_matchups", "team_seasons", "team_rosters", "wowy_lineups",
    "games", "wnba_player_game_stats", "survival_daily", "poeltl_daily",
    "users", "game_reviews",
]

# A drop larger than this fraction of yesterday's count is a FAIL (data loss).
ANOMALY_DROP_FRAC = 0.05


class Health:
    def __init__(self, conn, today=None, quiet=False):
        self.conn = conn
        self.today = today or date.today()
        self.quiet = quiet
        self.results = []          # list of dicts: {section, status, name, detail}
        self.metrics = {}          # today's snapshot metrics

    # ── result helpers ────────────────────────────────────────────────────────
    def add(self, section, status, name, detail=""):
        self.results.append({"section": section, "status": status,
                             "name": name, "detail": detail})
        if not self.quiet:
            print(f"  {ICON[status]} [{section}] {name} — {detail}")

    # ── introspection helpers ─────────────────────────────────────────────────
    def _scalar(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return row[0] if row else None

    def table_exists(self, table):
        return bool(self._scalar(
            "SELECT to_regclass(%s)", (f"public.{table}",)))

    def column_exists(self, table, col):
        return bool(self._scalar(
            """SELECT 1 FROM information_schema.columns
               WHERE table_name=%s AND column_name=%s""", (table, col)))

    def count(self, table, where="", params=None):
        if not self.table_exists(table):
            return None
        sql = f"SELECT COUNT(*) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self._scalar(sql, params)

    # ── season context ─────────────────────────────────────────────────────────
    def nba_in_season(self):
        m = self.today.month
        # NBA: late Oct → mid June (incl. playoffs)
        return m in (10, 11, 12, 1, 2, 3, 4, 5) or (m == 6 and self.today.day <= 20)

    def wnba_in_season(self):
        m = self.today.month
        return m in (5, 6, 7, 8, 9, 10)

    # ──────────────────────────────────────────────────────────────────────────
    #  CHECK 1 — pipeline liveness / freshness (via available timestamps)
    # ──────────────────────────────────────────────────────────────────────────
    def check_freshness(self):
        sec = "Pipeline freshness"
        now = datetime.now(timezone.utc)

        # Cloud Railway cron → fetch_players writes players.updated_at daily.
        if self.column_exists("players", "updated_at"):
            ts = self._scalar("SELECT MAX(updated_at) FROM players")
            self._freshness_verdict(
                sec, "Cloud cron (players sync)", ts, now,
                warn_h=30, fail_h=54,
                ctx="runs daily on Railway — works year-round")

        # NB: no puzzle-generator freshness check — Higher-or-Lower & Guess Who
        # are lazily generated on first play (no cron), so a table's newest
        # created_at reflects the last time someone *played*, not a job's health.
        # See check_daily_puzzles for the (informational) puzzle status.

        # Wholesale-recompute aggregate tables have no natural date column, so an
        # `updated_at` is stamped on every write by a DB trigger (installed via
        # add_freshness_tracking.py). Reading MAX(updated_at) tells us the daily
        # pipeline actually refreshed the table — not just that it exited 0.
        # Guarded by column_exists so this stays dormant until the migration runs.
        #   (table, label, enforce-now?, context)
        agg = [
            ("player_seasons", "Season stats (aggregates/DARKO/LEBRON)", True,
             "cloud + local both write daily — expected year-round"),
            ("team_seasons",   "Team W-L records", True,
             "cloud writes daily — expected year-round"),
            ("player_matchups", "Matchup defense", self.nba_in_season(),
             "local pipeline, NBA in-season"),
            ("team_rosters",    "Rosters (WoWY)",  self.nba_in_season(),
             "local pipeline, NBA in-season"),
            ("wowy_lineups",    "WoWY lineups",    self.nba_in_season(),
             "local pipeline, NBA in-season"),
            ("player_pctiles",  "Percentiles (Builder)", self.nba_in_season(),
             "local pipeline, NBA in-season"),
        ]
        for tbl, label, enforce, ctx in agg:
            if not (self.table_exists(tbl) and self.column_exists(tbl, "updated_at")):
                continue
            if not enforce:
                self.add(sec, INFO, label, f"offseason — not refreshed now ({ctx})")
                continue
            ts = self._scalar(f"SELECT MAX(updated_at) FROM {tbl}")
            self._freshness_verdict(sec, label, ts, now,
                                    warn_h=30, fail_h=54, ctx=ctx)

        # Local Windows pipeline writes player_seasons (no timestamp col) — the
        # best timestamp proxy is games.updated_at during NBA season.
        if self.nba_in_season() and self.column_exists("games", "updated_at"):
            ts = self._scalar(
                "SELECT MAX(updated_at) FROM games WHERE season_type IS NOT NULL")
            self._freshness_verdict(
                sec, "Games table (NBA in-season)", ts, now,
                warn_h=30, fail_h=54, ctx="new finals should land daily")

    def _freshness_verdict(self, sec, name, ts, now, warn_h, fail_h, ctx=""):
        if ts is None:
            self.add(sec, WARN, name, f"never updated / empty ({ctx})")
            return
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_h = (now - ts).total_seconds() / 3600
        when = f"last update {age_h:.0f}h ago ({ts:%Y-%m-%d %H:%M} UTC)"
        if age_h >= fail_h:
            self.add(sec, FAIL, name, f"STALE — {when}. {ctx}")
        elif age_h >= warn_h:
            self.add(sec, WARN, name, f"aging — {when}. {ctx}")
        else:
            self.add(sec, OK, name, when)

    # ──────────────────────────────────────────────────────────────────────────
    #  CHECK 1b — pipeline run-tracking (definitive "did it actually run?")
    # ──────────────────────────────────────────────────────────────────────────
    def check_pipeline_runs(self):
        sec = "Pipeline runs"
        if not self.table_exists("pipeline_runs"):
            self.add(sec, INFO, "Run-tracking",
                     "pipeline_runs table not created yet — populates after the "
                     "next instrumented pipeline run")
            return

        # most recent run per pipeline
        rows = {}
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (pipeline)
                       pipeline, status, failed_steps,
                       EXTRACT(EPOCH FROM (NOW() - started_at))/3600 AS age_h,
                       started_at
                FROM pipeline_runs
                ORDER BY pipeline, started_at DESC""")
            for r in cur.fetchall():
                rows[r[0]] = {"status": r[1], "failed": r[2], "age_h": float(r[3]),
                             "started": r[4]}

        # (key, label, critical-when-missing?)
        # No puzzle_gen entry — puzzles are lazily generated on first play, so
        # there is no cron that records runs here (see check_daily_puzzles).
        expected = [
            ("cloud_daily", "Cloud daily update", True),
            # local pipeline only feeds NBA stats — non-critical in the offseason
            ("local_daily", "Local daily update (Windows PC)", self.nba_in_season()),
        ]
        for key, label, critical in expected:
            r = rows.get(key)
            if r is None:
                sev = FAIL if critical else WARN
                self.add(sec, sev, label, "NEVER recorded a run (job not wired up or never fired)")
                continue
            age = r["age_h"]
            stale = age >= 30  # daily job: >30h means it missed a day
            if r["status"] == "failed":
                self.add(sec, FAIL, label,
                         f"last run FAILED {age:.0f}h ago ({r['started']:%Y-%m-%d %H:%M})")
            elif stale:
                sev = FAIL if critical else WARN
                self.add(sec, sev, label,
                         f"DID NOT RUN — last run {age:.0f}h ago "
                         f"({r['started']:%Y-%m-%d %H:%M}). Job may be down"
                         f"{' (PC asleep?)' if key == 'local_daily' else ''}")
            elif r["status"] == "partial":
                fs = ", ".join(r["failed"] or []) or "some steps"
                self.add(sec, WARN, label, f"ran {age:.0f}h ago but {len(r['failed'] or [])} step(s) failed: {fs}")
            elif r["status"] == "running":
                self.add(sec, WARN if age < 3 else FAIL, label,
                         f"still marked 'running' after {age:.1f}h "
                         f"({'in progress' if age < 3 else 'likely crashed mid-run'})")
            else:  # success
                self.add(sec, OK, label, f"ran successfully {age:.0f}h ago")

    # ──────────────────────────────────────────────────────────────────────────
    #  CHECK 2 — daily puzzles exist for today (and a few days ahead)
    # ──────────────────────────────────────────────────────────────────────────
    def check_daily_puzzles(self):
        # Both games are lazily generated on first play (no cron). So the only
        # real breakage detectable here is a row that exists but has an EMPTY
        # payload (generator ran and produced nothing). A missing row just means
        # nobody has played yet today — informational, not a failure.
        sec = "Daily puzzles"
        for tbl, label in (("survival_daily", "Higher or Lower"),
                           ("poeltl_daily", "Guess Who")):
            if not self.table_exists(tbl):
                self.add(sec, FAIL, label, f"table {tbl} missing")
                continue
            # today present + non-empty payload?
            row = None
            with self.conn.cursor() as cur:
                cur.execute(
                    f"SELECT payload IS NOT NULL AND payload::text <> '{{}}' "
                    f"FROM {tbl} WHERE date = %s", (self.today,))
                row = cur.fetchone()
            if row is None:
                self.add(sec, INFO, f"{label} — today",
                         f"not generated yet for {self.today} "
                         "(lazy — created on first play)")
            elif not row[0]:
                self.add(sec, FAIL, f"{label} — today",
                         f"puzzle row exists for {self.today} but payload is EMPTY "
                         "— generator ran but produced nothing")
            else:
                self.add(sec, OK, f"{label} — today",
                         f"generated for {self.today} (someone has played)")

    # ──────────────────────────────────────────────────────────────────────────
    #  CHECK 2b — data completeness: did the per-game data actually LAND?
    #  Run-tracking proves a pipeline *ran* (exit 0); anomaly detection proves
    #  counts didn't drop. NEITHER proves the rows that SHOULD exist do — a fetch
    #  can return an empty/stale response and still exit 0. This anchors on ground
    #  truth (the finished-games schedule) and asserts the local pipeline's
    #  per-game output has caught up to it. Self-calibrating by season: with no
    #  recent finals (offseason) nothing is due, so it stays quiet.
    # ──────────────────────────────────────────────────────────────────────────
    def check_data_completeness(self):
        sec = "Data completeness"
        if not self.table_exists("games"):
            return

        # The `games` table holds BOTH leagues; NBA seasons are hyphenated
        # ("2025-26"), WNBA are single-year ("2026"). Scope each arm to its league
        # so an in-season WNBA slate doesn't get mistaken for an NBA schedule.

        # ── NBA: player_gamelogs (local pipeline) vs the NBA schedule ──
        if self.table_exists("player_gamelogs"):
            last_final = self._scalar(
                "SELECT MAX(game_date) FROM games "
                "WHERE status='Final' AND season LIKE '%%-%%'")
            last_landed = self._scalar("SELECT MAX(game_date) FROM player_gamelogs")
            self._completeness_arm(
                sec, "NBA game logs", last_final, last_landed,
                landed_noun="gamelogs",
                empty_msg="player_gamelogs is EMPTY — local pipeline not landing data")

        # ── WNBA: box scores (cloud pipeline) vs the WNBA schedule ──
        # wnba_player_game_stats has no date column, so join to games via game_id.
        if self.table_exists("wnba_player_game_stats"):
            last_final = self._scalar(
                "SELECT MAX(game_date) FROM games "
                "WHERE status='Final' AND season NOT LIKE '%%-%%'")
            last_landed = self._scalar(
                "SELECT MAX(g.game_date) FROM games g "
                "JOIN wnba_player_game_stats w ON w.game_id = g.game_id "
                "WHERE g.status='Final' AND g.season NOT LIKE '%%-%%'")
            self._completeness_arm(
                sec, "WNBA box scores", last_final, last_landed,
                landed_noun="box scores",
                empty_msg="no WNBA box scores landed yet — pipeline not landing data")

    def _completeness_arm(self, sec, label, last_final, last_landed,
                          landed_noun, empty_msg):
        """Compare a league's landed per-game data against its finished-game
        schedule. Self-calibrating: only enforced while the league is actively
        playing (a final in the last few days); otherwise it's the offseason and
        nothing is due, so we stay quiet."""
        if last_final is None:
            self.add(sec, INFO, label, "no finished games on record yet")
            return
        days_since = (self.today - last_final).days
        if days_since > 3:
            self.add(sec, INFO, label,
                     f"last final {last_final} ({days_since}d ago) — offseason, nothing due")
            return
        if last_landed is None:
            self.add(sec, FAIL, label,
                     f"schedule has finals through {last_final} but {empty_msg}")
            return
        behind = (last_final - last_landed).days
        detail = f"{landed_noun} through {last_landed}, schedule through {last_final}"
        if behind <= 1:            # data lands the morning after — 1 day is normal
            self.add(sec, OK, label, f"current — {detail}")
        elif behind == 2:
            self.add(sec, WARN, label, f"1 day past the normal overnight lag — {detail}")
        else:
            self.add(sec, FAIL, label,
                     f"{behind} days behind schedule — pipeline ran but isn't "
                     f"landing data ({detail})")

    # ──────────────────────────────────────────────────────────────────────────
    #  CHECK 3 — day-over-day anomaly detection (snapshot compare)
    # ──────────────────────────────────────────────────────────────────────────
    def gather_metrics(self):
        for t in SNAPSHOT_TABLES:
            self.metrics[t] = self.count(t)

    def check_anomalies(self):
        sec = "Anomaly detection"
        prev = self._prev_snapshot()
        if not prev:
            self.add(sec, INFO, "Baseline",
                     "no prior snapshot yet — today becomes the baseline; "
                     "anomaly detection starts tomorrow")
            return
        prev_date = prev["snapshot_date"]
        prev_m = prev["metrics"]
        for t, now_c in self.metrics.items():
            if now_c is None:
                continue
            was = prev_m.get(t)
            if was is None or was == 0:
                continue
            delta = now_c - was
            if delta < 0 and abs(delta) >= max(1, ANOMALY_DROP_FRAC * was):
                self.add(sec, FAIL, t,
                         f"row count DROPPED {was:,} → {now_c:,} ({delta:+,}) "
                         f"since {prev_date} — likely a failed/partial fetch")
            elif delta < 0:
                self.add(sec, WARN, t,
                         f"slight drop {was:,} → {now_c:,} ({delta:+,}) since {prev_date}")
            elif delta > 0:
                self.add(sec, OK, t, f"{was:,} → {now_c:,} ({delta:+,})")
            else:
                self.add(sec, OK, t, f"unchanged ({now_c:,})")

    def _prev_snapshot(self):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT snapshot_date, metrics FROM health_snapshots
                   WHERE snapshot_date < %s ORDER BY snapshot_date DESC LIMIT 1""",
                (self.today,))
            return cur.fetchone()

    def ensure_snapshot_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS health_snapshots (
                    snapshot_date DATE PRIMARY KEY,
                    metrics       JSONB NOT NULL,
                    created_at    TIMESTAMPTZ DEFAULT NOW()
                )""")
        self.conn.commit()

    def write_snapshot(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO health_snapshots (snapshot_date, metrics)
                VALUES (%s, %s)
                ON CONFLICT (snapshot_date)
                DO UPDATE SET metrics = EXCLUDED.metrics, created_at = NOW()""",
                (self.today, json.dumps(self.metrics)))
        self.conn.commit()

    # ──────────────────────────────────────────────────────────────────────────
    #  CHECK 4 — standing structural gaps (from DATABASE_MAP) — verified live
    # ──────────────────────────────────────────────────────────────────────────
    def check_known_gaps(self):
        sec = "Structural gaps"
        cur_season = self._current_season_str()

        # PVA missing for current season
        c = self.count("player_pva_season", "season = %s", (cur_season,))
        if c is not None and c == 0:
            self.add(sec, WARN, "player_pva_season",
                     f"no rows for {cur_season} — compute_pva.py not in any pipeline")

        # Adjusted WoWY missing for current season
        c = self.count("player_adjusted_wowy", "season = %s", (cur_season,))
        if c is not None and c == 0:
            self.add(sec, WARN, "player_adjusted_wowy",
                     f"no rows for {cur_season} — compute script not in any pipeline")

        # players.position coverage
        total = self.count("players")
        if total:
            filled = self.count("players", "position IS NOT NULL AND position <> ''")
            pct = 100 * filled / total
            status = WARN if pct < 60 else OK
            self.add(sec, status, "players.position coverage",
                     f"{filled:,}/{total:,} ({pct:.0f}%) have a position")

        # pctiles present for current season (both types)
        for stype in ("Regular Season", "Playoffs"):
            c = self.count("player_pctiles", "season = %s AND season_type = %s",
                          (cur_season, stype))
            if c is not None and c == 0 and not (stype == "Playoffs" and self.nba_in_season()):
                self.add(sec, WARN, f"player_pctiles {cur_season} {stype}",
                         "missing — Builder tool degraded for this season/type")

    def _current_season_str(self):
        # NBA season label e.g. "2025-26". Season starts in October.
        y = self.today.year
        if self.today.month >= 10:
            return f"{y}-{str(y + 1)[2:]}"
        return f"{y - 1}-{str(y)[2:]}"

    # ──────────────────────────────────────────────────────────────────────────
    #  Report rendering
    # ──────────────────────────────────────────────────────────────────────────
    def overall(self):
        statuses = [r["status"] for r in self.results]
        if FAIL in statuses:
            return FAIL
        if WARN in statuses:
            return WARN
        return OK

    def render(self):
        ov = self.overall()
        n_fail = sum(r["status"] == FAIL for r in self.results)
        n_warn = sum(r["status"] == WARN for r in self.results)
        headline = {
            OK:   "All systems healthy — data is fresh and complete.",
            WARN: f"Mostly healthy — {n_warn} thing(s) to keep an eye on.",
            FAIL: f"ATTENTION NEEDED — {n_fail} failure(s) and {n_warn} warning(s).",
        }[ov]

        L = []
        L.append(f"# 🏀 ydkball — Daily Data Report")
        L.append(f"**{self.today:%A, %B %d, %Y}**  ·  {ICON[ov]} **{headline}**")
        L.append("")
        L.append(f"Context: NBA {'in-season' if self.nba_in_season() else 'offseason'} · "
                 f"WNBA {'in-season' if self.wnba_in_season() else 'offseason'}")
        L.append("")

        # ── To-do list first: what needs attention, prioritized ──
        todos = [r for r in self.results if r["status"] in (FAIL, WARN)]
        todos.sort(key=lambda r: 0 if r["status"] == FAIL else 1)
        L.append("## ✅ To-Do / Needs Attention")
        if not todos:
            L.append("Nothing — everything passed. ✨")
        else:
            for r in todos:
                L.append(f"- {ICON[r['status']]} **[{r['section']}] {r['name']}** — {r['detail']}")
        L.append("")

        # ── Full results by section ──
        L.append("## 📋 Full Check Results")
        sections = []
        for r in self.results:
            if r["section"] not in sections:
                sections.append(r["section"])
        for s in sections:
            L.append(f"\n### {s}")
            for r in [r for r in self.results if r["section"] == s]:
                L.append(f"- {ICON[r['status']]} {r['name']} — {r['detail']}")
        L.append("")
        L.append(f"---\n*Generated {datetime.now():%Y-%m-%d %H:%M:%S} by health_check.py*")
        return "\n".join(L)

    def render_html(self):
        ov = self.overall()
        n_fail = sum(r["status"] == FAIL for r in self.results)
        n_warn = sum(r["status"] == WARN for r in self.results)
        banner_bg = {OK: "#eafaef", WARN: "#fdf6e3", FAIL: "#fdecea"}[ov]
        banner_fg = {OK: "#1a7a36", WARN: "#9a7508", FAIL: "#c0392b"}[ov]
        headline = {
            OK:   "All systems healthy — data is fresh and complete.",
            WARN: f"Mostly healthy — {n_warn} item(s) to watch.",
            FAIL: f"Attention needed — {n_fail} failure(s), {n_warn} warning(s).",
        }[ov]
        pill_bg = {OK: "#eafaef", WARN: "#fdf6e3", FAIL: "#fdecea", INFO: "#eee"}
        pill_fg = {OK: "#1a7a36", WARN: "#9a7508", FAIL: "#c0392b", INFO: "#666"}

        def pill(s):
            return (f"<span style='background:{pill_bg[s]};color:{pill_fg[s]};"
                    f"font:600 10px monospace;padding:2px 7px;border-radius:3px'>{s}</span>")

        h = ['<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;'
             'max-width:640px;margin:0 auto;color:#1a1a1a">']
        h.append(f"<h2 style='margin:0 0 4px'>🏀 ydkball — Daily Data Report</h2>")
        h.append(f"<div style='color:#777;font-size:13px'>{self.today:%A, %B %d, %Y}</div>")
        h.append(f"<div style='background:{banner_bg};color:{banner_fg};padding:12px 16px;"
                 f"border-radius:6px;margin:14px 0;font-weight:600'>{headline}</div>")

        todos = sorted([r for r in self.results if r["status"] in (FAIL, WARN)],
                       key=lambda r: 0 if r["status"] == FAIL else 1)
        h.append("<h3 style='margin:18px 0 8px'>To-Do / Needs Attention</h3>")
        if not todos:
            h.append("<div style='color:#777'>Nothing — everything passed ✨</div>")
        for r in todos:
            h.append(f"<div style='padding:8px 0;border-bottom:1px solid #eee'>{pill(r['status'])} "
                     f"<b>{r['section']}: {r['name']}</b><br>"
                     f"<span style='color:#777;font-size:13px'>{r['detail']}</span></div>")

        h.append("<h3 style='margin:22px 0 8px'>Full Check Results</h3>")
        seen = []
        for r in self.results:
            if r["section"] not in seen:
                seen.append(r["section"])
                h.append(f"<div style='font:600 11px monospace;color:#777;"
                         f"text-transform:uppercase;margin:14px 0 6px'>{r['section']}</div>")
            h.append(f"<div style='padding:5px 0;font-size:14px'>{pill(r['status'])} "
                     f"<b>{r['name']}</b> — <span style='color:#777'>{r['detail']}</span></div>")
        h.append(f"<div style='color:#aaa;font-size:12px;margin-top:24px'>"
                 f"Generated {datetime.now():%Y-%m-%d %H:%M:%S} · "
                 f"open the <a href='/admin'>admin dashboard</a> for live numbers.</div>")
        h.append("</div>")
        return "\n".join(h)


def send_email_report(health, threshold=FAIL):
    """Email the report if overall status meets/exceeds threshold.

    Delivery: prefers the Resend HTTP API when RESEND_API_KEY is set (required on
    Railway, which blocks SMTP); otherwise falls back to Gmail SMTP (local dev).
    Env: RESEND_API_KEY (+ optional RESEND_FROM) for HTTP; GMAIL_ADDRESS/
    GMAIL_APP_PASSWORD for SMTP; REPORT_TO for the recipient (defaults to
    GMAIL_ADDRESS). Returns (sent: bool, message: str).
    """
    order = {OK: 0, WARN: 1, FAIL: 2}
    ov = health.overall()
    if order[ov] < order[threshold]:
        return False, f"status {ov} below threshold {threshold} — no email sent"

    recipient = os.getenv("REPORT_TO") or os.getenv("GMAIL_ADDRESS")
    if not recipient:
        return False, "REPORT_TO / GMAIL_ADDRESS not set — no recipient"

    icon = ICON[ov]
    n_fail = sum(r["status"] == FAIL for r in health.results)
    n_warn = sum(r["status"] == WARN for r in health.results)
    if ov == FAIL:
        subject = f"{icon} ydkball data report — {n_fail} failure(s) ({health.today})"
    elif ov == WARN:
        subject = f"{icon} ydkball data report — {n_warn} warning(s) ({health.today})"
    else:
        subject = f"{icon} ydkball data report — all healthy ({health.today})"

    html = health.render_html()
    text = health.render()

    # Railway blocks outbound SMTP on ALL ports (25/465/587) — SMTP just times out
    # there. So prefer an HTTP email API (Resend) over port 443, which is open.
    # SMTP stays as a fallback for local/off-Railway runs.
    resend_key = os.getenv("RESEND_API_KEY")
    if resend_key:
        import requests
        from_addr = os.getenv("RESEND_FROM", "ydkball <onboarding@resend.dev>")
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {resend_key}",
                         "Content-Type": "application/json"},
                json={"from": from_addr, "to": [recipient],
                      "subject": subject, "html": html, "text": text},
                timeout=30)
        except Exception as e:
            return False, f"Resend request failed: {e}"
        if resp.status_code >= 300:
            return False, f"Resend error {resp.status_code}: {resp.text[:200]}"
        return True, f"emailed {recipient} via Resend (subject: {subject})"

    # SMTP fallback (works off Railway, e.g. local dev)
    sender = os.getenv("GMAIL_ADDRESS")
    app_pw = os.getenv("GMAIL_APP_PASSWORD")
    if not sender or not app_pw:
        return False, "no RESEND_API_KEY, and GMAIL_ADDRESS/GMAIL_APP_PASSWORD not set — cannot send"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    import socket as _socket
    import ssl as _ssl
    _orig_gai = _socket.getaddrinfo

    def _ipv4_only(*args, **kwargs):
        res = _orig_gai(*args, **kwargs)
        v4 = [r for r in res if r[0] == _socket.AF_INET]
        return v4 or res

    _socket.getaddrinfo = _ipv4_only
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as s:
            s.ehlo()
            s.starttls(context=_ssl.create_default_context())
            s.ehlo()
            s.login(sender, app_pw)
            s.sendmail(sender, [recipient], msg.as_string())
    finally:
        _socket.getaddrinfo = _orig_gai
    return True, f"emailed {recipient} via Gmail SMTP (subject: {subject})"


def collect(conn, today=None, write_snapshot=True):
    """Run all checks against an open connection and return the Health object.

    Importable entry point for the admin dashboard / email cron — no printing.
    Pass write_snapshot=False for read-only callers (e.g. a dashboard page load)
    so only the scheduled run records the day's baseline.
    """
    h = Health(conn, today=today, quiet=True)
    h.ensure_snapshot_table()
    h.gather_metrics()
    h.check_freshness()
    h.check_pipeline_runs()
    h.check_daily_puzzles()
    h.check_data_completeness()
    h.check_anomalies()
    h.check_known_gaps()
    if write_snapshot:
        h.write_snapshot()
    return h


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-snapshot", action="store_true",
                    help="don't record today's snapshot (read-only mode)")
    ap.add_argument("--quiet", action="store_true",
                    help="suppress per-check progress; print only the report")
    ap.add_argument("--date", default=None, help="run as if today were YYYY-MM-DD")
    ap.add_argument("--email", action="store_true",
                    help="email the report via Gmail SMTP (see --email-on)")
    ap.add_argument("--email-on", choices=["fail", "warn", "always"], default="fail",
                    help="email only when status is at least this severe (default: fail)")
    args = ap.parse_args()

    db_url = _db_url()
    if not db_url:
        print("ERROR: neither DATABASE_URL nor DATABASE_PUBLIC_URL is set "
              "(check .env / Railway service variables)", file=sys.stderr)
        sys.exit(2)

    today = (datetime.strptime(args.date, "%Y-%m-%d").date()
             if args.date else date.today())

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    h = Health(conn, today=today, quiet=args.quiet)

    if not args.quiet:
        print("Running data health checks...\n")
    h.ensure_snapshot_table()
    h.gather_metrics()
    h.check_freshness()
    h.check_pipeline_runs()
    h.check_daily_puzzles()
    h.check_data_completeness()
    h.check_anomalies()
    h.check_known_gaps()
    if not args.no_snapshot:
        h.write_snapshot()

    report = h.render()

    # save to logs/
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    out_path = os.path.join(log_dir, f"health_report_{today:%Y-%m-%d}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)

    print("\n" + "=" * 70)
    print(report)
    print("=" * 70)
    print(f"\nReport saved to {out_path}")

    email_failed = False
    if args.email:
        threshold = {"fail": FAIL, "warn": WARN, "always": OK}[args.email_on]
        try:
            sent, info = send_email_report(h, threshold=threshold)
        except Exception as e:
            sent, info = False, f"exception: {e}"
        if sent:
            print("📧 " + info)
        elif info.startswith("status "):     # below-threshold skip — not a failure
            print("✉️  skipped: " + info)
        else:
            print("📧 email FAILED: " + info, file=sys.stderr)
            email_failed = True

    conn.close()
    # In --email mode the job's success is DELIVERING the report, not the health
    # status (which lives in the email). Exit non-zero only if the email itself
    # failed — otherwise Railway flags the service "crashed" on every FAIL day.
    if args.email:
        sys.exit(1 if email_failed else 0)
    sys.exit(1 if h.overall() == FAIL else 0)


if __name__ == "__main__":
    main()
