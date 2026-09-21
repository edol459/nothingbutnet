# Jerseys — LockerVision ingest

**Status:** automated in `cloud_daily` as of 2026-09-21. Nothing to remember.

## The short version

NBA and WNBA jersey art comes from the LockerVision CDN via
`backend/ingest/ingest_lockervision_jerseys.py` and
`ingest_lockervision_wnba_jerseys.py`. Both now run daily in
`backend/ingest/daily_update.py` and are self-limiting: they cost one DB query
once a season's art exists, and they decline to write anything before it does.

So a new season's kits appear on their own, within a day of the NBA publishing
them. You should not need to touch this.

To check by hand:

```bash
python backend/ingest/ingest_lockervision_jerseys.py --season 2026 --dry-run
```

## The trap this exists to avoid

**LockerVision answers `200 OK` for a season it has not published**, serving one
shared placeholder rather than a 404.

Measured 2026-09-21, probing 2026-27: every URL returned the same 3,958-byte
image — four different teams, three different editions, one identical sha256.
The 2025-26 control returned genuinely distinct images per team and edition.

Both scripts previously trusted the URL pattern alone. `ingest_lockervision_jerseys.py`
even had a `check_url()` helper that was **never called**, and the WNBA script had
no check at all. Running either against an unpublished season would have written
120 (NBA) or 48 (WNBA) rows pointing at that placeholder — and because
`/api/jerseys/seasons` is driven by the `jerseys` table, the app's picker would
have started defaulting to a season of blank jerseys.

## How verification works

A real jersey image is unique to its team and edition, so **any image whose hash
appears more than once is a placeholder.** That test needs no hardcoded hash and
keeps working if the NBA swaps the placeholder art.

Two details that are easy to get wrong:

- **Fetch through `server._cdn_get`, not `urllib`.** The CDN fallback-serves plain
  clients after a handful of requests — the Akamai behaviour in
  [cdn-akamai-bot-manager.md](cdn-akamai-bot-manager.md). Mid-probe, even known-good
  2025 URLs started returning the placeholder. A verifier built on `urllib` would
  reject real art as fake.
- **Key the season to `roster_season()`, not `current_season()`.** Jerseys lead the
  season the way rosters do — the art lands around media day, weeks before a game
  is played. `current_season()` reads played games and still said 2025-26 in late
  September, which combined with `--skip-if-present` would have made the pipeline
  step short-circuit on jerseys we already had and never look for the new season's.
  Same rule CLAUDE.md states for rosters: *stats resolve from played games,
  membership resolves from the schedule.*

## Flags

| Flag | Purpose |
|---|---|
| `--season YYYY` | Season start year. NBA `2026` = 2026-27; WNBA `2026` = 2026. |
| `--dry-run` | Verify and report, write nothing. |
| `--skip-if-present` | Exit 0 immediately if this season already has rows — no CDN calls. |
| `--ok-if-unpublished` | Exit 0 rather than 1 when the art isn't out yet. |
| `--no-verify` | Skip the placeholder check. Only for a CDN change that breaks the hash test. |

Exit codes: **0** wrote rows / already had them / unpublished-and-told-that's-fine.
**1** unpublished on a manual run. The pipeline passes both flags, so the weeks
before the art lands don't show up as `partial` runs in the health report.

## Still manual

- **Expansion and relocated teams.** The `TEAMS` list in each script is
  hand-maintained, as are the abbr and logo maps. The WNBA is actively expanding —
  a new team's jerseys will not appear until it's added to that list.
