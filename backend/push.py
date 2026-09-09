"""
ydkball — APNs sender
=============================================================
Token-based (.p8) push to Apple. Nothing here knows *why* a notification is being
sent; it takes device tokens and a message and reports, per token, what Apple said.

Config (Railway env, mirrored into .env for local testing):
    APNS_KEY_ID     10-char Key ID from Apple Developer → Keys
    APNS_TEAM_ID    RHB7DB5Q97
    APNS_BUNDLE_ID  net.ydkball.ydkball
    APNS_KEY_P8     contents of AuthKey_XXXXXXXXXX.p8, including the BEGIN/END lines

Two details that cause most APNs mysteries:

1. HTTP/2 is mandatory. `requests` is HTTP/1.1 only and will not work — hence httpx
   with the h2 extra.

2. The environment comes from the TOKEN, not the key. One .p8 signs for both hosts,
   but a token minted by a development build is only valid against the sandbox host
   and vice versa. Sending to the wrong one returns `BadDeviceToken`, which reads
   exactly like a corrupt token. `device_tokens.environment` records which kind it is
   and `send()` groups by it.

Run directly to check the configuration without sending anything:

    python backend/push.py --validate
    python backend/push.py --send <device-token> [--environment sandbox]
"""
import os, sys, time, json
from dotenv import load_dotenv

load_dotenv()

APNS_KEY_ID    = os.getenv("APNS_KEY_ID")
APNS_TEAM_ID   = os.getenv("APNS_TEAM_ID")
APNS_BUNDLE_ID = os.getenv("APNS_BUNDLE_ID") or "net.ydkball.ydkball"
_RAW_P8        = os.getenv("APNS_KEY_P8") or ""

# Railway multi-line values arrive with real newlines; a single-line paste arrives with
# literal backslash-n. Accept both rather than making the operator care.
APNS_KEY_P8 = _RAW_P8.replace("\\n", "\n").strip()

HOSTS = {
    "production": "https://api.push.apple.com",
    "sandbox":    "https://api.sandbox.push.apple.com",
}

# Apple rejects a provider token older than 1 hour and refuses to mint a new one more
# than once every 20 minutes, so this is cached rather than signed per send.
_cached = {"jwt": None, "iat": 0.0}


def is_configured() -> bool:
    return bool(APNS_KEY_ID and APNS_TEAM_ID and APNS_KEY_P8)


def _auth_token() -> str:
    import jwt  # imported late so an unconfigured deploy never pays for it
    now = time.time()
    if _cached["jwt"] and (now - _cached["iat"]) < 50 * 60:
        return _cached["jwt"]
    token = jwt.encode(
        {"iss": APNS_TEAM_ID, "iat": int(now)},
        APNS_KEY_P8,
        algorithm="ES256",
        headers={"kid": APNS_KEY_ID},
    )
    _cached.update(jwt=token, iat=now)
    return token


def send(tokens, title: str, body: str, data: dict = None,
         environment: str = "production", collapse_id: str = None,
         thread_id: str = None) -> list:
    """Push one message to many tokens in ONE environment.

    Returns [{token, status, reason, dead}] — `dead` marks a token Apple says will
    never work again, which the caller should retire rather than retry.
    """
    if not is_configured():
        return [{"token": t, "status": 0, "reason": "not_configured", "dead": False}
                for t in tokens]
    if not tokens:
        return []

    import httpx

    payload = {"aps": {"alert": {"title": title, "body": body},
                       "sound": "default"}}
    if thread_id:
        payload["aps"]["thread-id"] = thread_id
    if data:
        payload.update(data)
    encoded = json.dumps(payload).encode()

    headers = {
        "authorization":   f"bearer {_auth_token()}",
        "apns-topic":      APNS_BUNDLE_ID,
        "apns-push-type":  "alert",
        # 10 = deliver immediately. 5 would let Apple hold it to save power, which is
        # wrong for "the game just ended".
        "apns-priority":   "10",
    }
    if collapse_id:
        # Same id replaces an undelivered earlier one instead of stacking. Used so a
        # phone that was off all evening shows one digest, not four.
        headers["apns-collapse-id"] = collapse_id[:64]

    host = HOSTS.get(environment, HOSTS["production"])
    out = []
    # One HTTP/2 connection carries every send — that is the entire reason APNs
    # requires it, and opening one per token would be dramatically slower.
    with httpx.Client(http2=True, base_url=host, timeout=10.0) as client:
        for tok in tokens:
            try:
                r = client.post(f"/3/device/{tok}", content=encoded, headers=headers)
                reason = ""
                if r.status_code != 200:
                    try:    reason = (r.json() or {}).get("reason", "")
                    except Exception: reason = r.text[:120]
                out.append({
                    "token": tok,
                    "status": r.status_code,
                    "reason": reason,
                    # 410 Unregistered = app deleted. BadDeviceToken = wrong environment
                    # or malformed. Neither is worth retrying.
                    "dead": r.status_code == 410 or reason in
                            ("BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"),
                })
            except Exception as e:
                out.append({"token": tok, "status": 0, "reason": str(e)[:120], "dead": False})
    return out


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--validate" in args or not args:
        print("APNS_KEY_ID   :", APNS_KEY_ID or "MISSING")
        print("APNS_TEAM_ID  :", APNS_TEAM_ID or "MISSING")
        print("APNS_BUNDLE_ID:", APNS_BUNDLE_ID)
        print("APNS_KEY_P8   :", f"{len(APNS_KEY_P8)} chars" if APNS_KEY_P8 else "MISSING")
        if not is_configured():
            print("\n❌ not configured — set the variables above in .env for local runs")
            sys.exit(1)
        try:
            t = _auth_token()
            print(f"\n✅ key parses and signs — provider JWT is {len(t)} chars")
            print("   (this proves the .p8, Key ID and Team ID are usable;")
            print("    it does NOT prove the key is enabled for this bundle id)")
        except Exception as e:
            print(f"\n❌ could not sign: {e}"); sys.exit(1)
    if "--send" in args:
        i = args.index("--send")
        tok = args[i + 1]
        env = args[args.index("--environment") + 1] if "--environment" in args else "sandbox"
        res = send([tok], "ydkball", "Test push — you can ignore this.",
                   environment=env, data={"kind": "test"})
        print(json.dumps(res, indent=2))
