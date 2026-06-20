# NBA/WNBA CDN — Akamai Bot Manager & the `curl_cffi` fix

## Symptom
Live NBA/WNBA data silently breaks **on Railway (or any data-center host)** while
working fine when you run `python backend/server.py` from home. Typical signs:

- WNBA games show as "upcoming" instead of live, or boxscore/PBP return
  "unavailable".
- The server's CDN requests come back as `Content-Type: text/html` (a challenge
  page titled "WNBA.com" / "NBA.com") instead of JSON, or `403`.
- A Railway redeploy *temporarily* fixes it (new container = different egress IP).

## Root cause
`cdn.nba.com` and `cdn.wnba.com` are both served by **Akamai**, and Akamai
**Bot Manager** is now enabled on these JSON endpoints. Bot Manager scores each
request primarily on the client's **TLS fingerprint (JA3)** plus IP reputation.

- Plain `requests` / `urllib3` / `curl` have fingerprints Akamai flags → they get
  the JS challenge page and a `_abck=...~-1~...` cookie (`-1` = "not verified as
  human"). A real browser passes because it runs Akamai's JavaScript sensor.
- Confirm it's Bot Manager by the response setting `_abck` and `bm_sz` cookies and
  `server: AkamaiNetStorage`. A normal static JSON CDN does **not** set those.
- Adding browser-like headers (User-Agent, Referer, `sec-ch-ua`, etc.) does **not**
  help — the gate is the TLS handshake, not the headers. It also blocks from a
  residential IP, so it is not purely a Railway/IP issue.

## The fix (keeps the official NBA/WNBA APIs — no ESPN)
Make every `cdn.nba.com` / `cdn.wnba.com` request with **`curl_cffi`** using
`impersonate="chrome"`. curl_cffi replicates Chrome's exact TLS + HTTP/2
fingerprint, so Akamai treats it as a real browser and returns JSON.

In `backend/server.py`:

```python
from curl_cffi import requests as _cffi_requests

def _cdn_get(url, headers=None, timeout=10, impersonate="chrome"):
    # curl_cffi Response is API-compatible with requests.Response
    # (.json(), .text, .status_code, .headers, .raise_for_status()).
    return _cffi_requests.get(url, headers=headers, timeout=timeout, impersonate=impersonate)
```

All CDN calls go through `_cdn_get()` (scoreboards, schedules, boxscores, PBP,
the parallel fetchers, and the background ingest). Non-CDN calls (ESPN historical
fallback, etc.) still use plain `_requests`. `requirements.txt` pins
`curl_cffi==0.15.0`.

Notes:
- The scoreboard endpoint returns valid JSON but with `Content-Type: text/plain`
  — call `.json()` regardless of content-type (don't gate on the header).
- NBA endpoints need the `Referer: https://www.nba.com/` header; WNBA needs
  `Referer: https://www.wnba.com/`. Keep passing the existing `_CDN_HEADERS` /
  `_WNBA_CDN_HEADERS` alongside `impersonate`.

## If it breaks again despite curl_cffi
Akamai also weighs **IP reputation**, so a flagged data-center IP can still get
challenged even with a perfect TLS fingerprint. Next escalation: route `_cdn_get`
through a **residential proxy** (one-line change since everything funnels through
that one helper). Bumping `impersonate` to a newer Chrome build (e.g. `chrome124`)
can also help if Akamai starts flagging the pinned fingerprint.

## Quick diagnosis snippet
```python
from curl_cffi import requests as cffi
r = cffi.get("https://cdn.wnba.com/static/json/liveData/scoreboard/todaysScoreboard_10.json",
             impersonate="chrome", timeout=12)
print(r.status_code, r.headers.get("content-type"), r.json()["scoreboard"]["gameDate"])
```
If that returns JSON but plain `requests` returns HTML, it's the Bot Manager / TLS
issue described here.
