# ydkball (nothingbutnet)

NBA & WNBA scores, stats, game reviews, and daily games. Flask backend + static
frontend, Postgres (Railway), plus an iOS app that shares the same API.
Public site: ydkball.net. Code/repo name is "nothingbutnet"; product is "ydkball".

## Run it

```bash
# backend (serves the frontend too)
python backend/server.py            # local dev
# prod: Procfile → gunicorn server:app   (Railway)
```
`backend/server.py` is a Flask app whose static_folder is `frontend/`, so `/`
serves `frontend/index.html` and every page is a static HTML file in `frontend/`.

Config comes from `.env` (DATABASE_URL, Google/Apple OAuth, RevenueCat,
ADMIN_GOOGLE_IDS, GMAIL_* for the health-report email). `.venv/` is the local
virtualenv.

## Naming gotchas (important)

- **"Higher or Lower"** is the user-facing name; the code says **`survival`**
  (`backend/games/survival_api.py`, `survival_daily` table). See `docs/trivia-game.md`.
- **"Guess Who"** is user-facing; the code/routes say **`poeltl`**
  (`backend/games/poeltl_api.py`, `poeltl_daily` table).
- Repo is "nothingbutnet", product is "ydkball".

## Layout

- `backend/server.py` — the monolith: all API routes, page routes, scoreboards,
  games endpoints, admin, RevenueCat webhook. Large; grep by route.
- `backend/auth.py` — Google + Apple sign-in, sessions, mobile Bearer tokens
  (`current_user()`, `login_required`). Admin gate is in server.py:
  `_is_admin` / `_admin_required`, allowlisted by `ADMIN_GOOGLE_IDS` env.
- `backend/games/` — daily games (survival/poeltl) + generators.
- `backend/ingest/` — all data fetch/compute scripts + the daily pipelines.
- `backend/schema*.py` — table DDL (schema.py, schema_additions.py, schema_v3..v5).
- `frontend/` — one static HTML file per page; `/admin` is `admin.html`.

## Frontend conventions

- **Canonical header**: every page uses the same `<header class="site-nav">` markup +
  `/site-nav.css` + `/site-nav.js`. Never add a per-page nav.
- **Design tokens** (the "paper" theme) live in each page's `:root`: `--paper`,
  `--bg-card`, `--ink`/`--ink2..4`, `--orange`, `--gold`, `--green`, `--red`,
  fonts `--ff-serif` (DM Serif Display), `--ff-sans` (Instrument Sans),
  `--ff-mono` (DM Mono).
- **Night mode**: `html.night-mode` overrides the tokens; boot script at the top of
  `<head>` reads `localStorage.ydkball_night`. New pages should support it by using
  the tokens (not hardcoded colors).

## iOS app (separate repo, shared backend)

Native **SwiftUI** app (iOS 17+; bundle id `net.ydkball.ydkball`, team `RHB7DB5Q97`)
living in its **own git repo at `/Users/ethandolder/projects/ydkball/`** — *not* in
this repo. This Flask server is its backend, and the app hits the **production** API
directly (`API.swift` base = `https://ydkball.net`, even in dev). So changing shared
endpoints, auth, or JSON shapes can silently break the app — keep the contract below stable.

- **App layout:** flat — all Swift in `ydkball/ydkball/`. Entry `ydkballApp.swift` (@main).
  Networking in `API.swift`; auth in `AuthManager.swift`; Pro/RevenueCat in `ProManager.swift`.
  Views mirror the web: `ScoresView`, `FeedView` (reviews), `GamesView`/`BrowseGamesView`,
  `DiscoverView`, `ProfileView`, `FriendsView`, plus games `Survival*`/`Poeltl*`, `ProPaywallView`.
- **Auth = Bearer token, not cookies.** Token is kept in `UserDefaults` (key `ydkball.mobileToken`),
  sent as `Authorization: Bearer <token>`; backend `current_user()` (auth.py) accepts a web
  session cookie *or* that token (resolved against `users.mobile_token`).
  - Google: app opens `/auth/google/login?mobile=1` in a web auth session and catches the
    `ydkball://auth-complete?token=…` redirect.
  - Apple (native Sign in with Apple): `POST /auth/apple {identity_token, full_name}` → `{token}`.
- **Pro / subscriptions = RevenueCat** (entitlement id `"pro"`). App calls `Purchases.logIn("<users.id>")`,
  so RevenueCat's `app_user_id` **is** `users.id`; it also trusts backend `is_pro` from `/auth/me`.
  Webhook `POST /api/webhooks/revenuecat` (authed via `REVENUECAT_WEBHOOK_SECRET` header) flips
  `users.is_pro` by event type and logs to `revenue_events`.
- **Deep links:** custom scheme `ydkball://` (auth callback); Universal Links via
  `/.well-known/apple-app-site-association` (appID `RHB7DB5Q97.net.ydkball.ydkball`, only `/profile/*`).

## Data pipelines (keep the DB fresh)

Three jobs, see `DATABASE_MAP.md` for the full table-by-table map.
- `backend/ingest/daily_update.py` — **Railway cron** (`cloud_daily`): steps that
  work from a datacenter IP (team records, players sync, DARKO/LEBRON/Net-Pts).
- `backend/ingest/daily_update_local.py` — **Windows Task Scheduler** (`local_daily`):
  steps that need a residential IP because stats.nba.com blocks Railway
  (season stats, PBP, gamelogs, lineups, pctiles). Writes straight to Railway PG.
- `backend/games/generate_daily.py` — **Railway cron** (`puzzle_gen`): pre-generates
  the Higher-or-Lower daily. Guess Who is generated lazily on first request (no cron).

All NBA/WNBA CDN calls go through `_cdn_get()` with curl_cffi Chrome impersonation
to defeat Akamai TLS fingerprinting on Railway IPs — see `docs/cdn-akamai-bot-manager.md`.

## Monitoring (admin + health)

- `/admin` (admin-gated) → **Insights** dashboard + **Moderation**. Insights are
  served by `/api/admin/dashboard`.
- `backend/ingest/health_check.py` — verifies pipeline freshness/run-tracking,
  today's daily puzzles, row-count anomalies, and structural gaps. Standalone it
  prints/saves a report (exit 1 on FAIL); `--email` sends it via Gmail SMTP.
  Importable via `collect(conn, ...)` (needs a PLAIN psycopg2 conn).
- `backend/ingest/pipeline_status.py` + `pipeline_runs` table — the three pipelines
  record start/finish/per-step status so the health check knows "did it run today?".

## Conventions

- DB access: `get_conn()` in server.py returns a thread-local `RealDictCursor`
  connection (dict rows). `health_check.py` uses positional rows — give it its own
  plain connection.
- `datetime` the class is NOT imported at module scope in server.py (only `date`,
  `timedelta`); import it locally as needed.
- Commit/push only when asked; branch off `master`. Railway auto-deploys `master`.
