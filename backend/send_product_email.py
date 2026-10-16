"""
ydkball — product email sender
=============================================================
  python backend/send_product_email.py                      # dry run (default)
  python backend/send_product_email.py --only-me            # real send, just you
  python backend/send_product_email.py --send               # the real thing

Sends one campaign to everyone who has not opted out. Dry run is the default
and you must pass --send to actually mail anyone.

What protects you here:

  * email_sends (schema_v21) is a dedupe ledger whose PRIMARY KEY is the
    "never twice" rule. The row is written BEFORE the API call, so a run that
    dies at recipient 200 resumes at 201 instead of re-mailing the first 200.
    There is no unsend.
  * Every message carries a per-user unsubscribe link and a List-Unsubscribe
    header, and anyone with email_opt_out set is never selected.
  * A postal address is required by CAN-SPAM. The script REFUSES to send
    while POSTAL_ADDRESS is unset rather than mailing a non-compliant footer.
  * Sending is batched with a pause. ydkball.net was registered in April 2026
    and has no sending reputation; 346 messages in one burst is what trips
    spam filters.

Env (.env): RESEND_API_KEY, RESEND_FROM, POSTAL_ADDRESS
"""
import os, sys, time, argparse, json
from dotenv import load_dotenv
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

DATABASE_URL  = os.getenv("DATABASE_URL")
RESEND_KEY    = os.getenv("RESEND_API_KEY")
RESEND_FROM   = os.getenv("RESEND_FROM", "ydkball <hello@ydkball.net>")
POSTAL        = os.getenv("POSTAL_ADDRESS", "").strip()
SECRET        = os.getenv("SECRET_KEY") or ""

CAMPAIGN   = "season_opener_2026"
SUBJECT    = "The NBA season starts Tuesday"
TEMPLATES  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "emails")

BATCH_SIZE = 25
BATCH_PAUSE = 20.0      # seconds between batches
SEND_PAUSE  = 0.6       # seconds between individual sends (Resend allows ~2/s)


def unsub_url(user_id) -> str:
    """Must match _unsub_token in server.py — same secret, same string."""
    import hmac, hashlib
    secret = SECRET.encode() or b"ydkball-dev"
    tok = hmac.new(secret, f"unsub:{user_id}".encode(), hashlib.sha256).hexdigest()[:32]
    return f"https://ydkball.net/unsubscribe?u={user_id}&t={tok}"


def render(name, user_id):
    out = []
    for ext in ("html", "txt"):
        with open(os.path.join(TEMPLATES, f"{name}.{ext}")) as f:
            body = f.read()
        body = body.replace("{{unsubscribe_url}}", unsub_url(user_id))
        body = body.replace("{{postal_address}}", POSTAL)
        out.append(body)
    return out[0], out[1]


def recipients(cur, only_me=False, include_relay=True):
    sql = """
        SELECT u.id, u.email, u.display_name,
               (u.email ILIKE '%%privaterelay.appleid.com') AS relay
          FROM users u
         WHERE u.email IS NOT NULL AND u.email <> ''
           AND u.email_opt_out = FALSE
           AND NOT EXISTS (SELECT 1 FROM email_sends e
                            WHERE e.campaign = %s AND e.user_id = u.id)
    """
    params = [CAMPAIGN]
    if only_me:
        sql += " AND u.email = %s"
        params.append(os.getenv("TEST_EMAIL", "ydkbball@gmail.com"))
    if not include_relay:
        sql += " AND u.email NOT ILIKE '%%privaterelay.appleid.com'"
    sql += " ORDER BY u.id"
    cur.execute(sql, params)
    return cur.fetchall()


def send_one(to_email, html, text, user_id):
    r = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {RESEND_KEY}",
                 "Content-Type": "application/json"},
        data=json.dumps({
            "from": RESEND_FROM,
            "to": [to_email],
            "subject": SUBJECT,
            "html": html,
            "text": text,
            # One-click unsubscribe. Gmail and Yahoo expect these on bulk mail,
            # and without them a campaign is far likelier to land in spam.
            "headers": {
                "List-Unsubscribe": f"<{unsub_url(user_id)}>",
                "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
            },
        }),
        timeout=30,
    )
    if r.status_code >= 300:
        return None, f"{r.status_code} {r.text[:160]}"
    return r.json().get("id"), None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="actually send (default is a dry run)")
    ap.add_argument("--only-me", action="store_true", help="send only to TEST_EMAIL")
    ap.add_argument("--skip-relay", action="store_true",
                    help="skip Apple Private Relay addresses")
    ap.add_argument("--limit", type=int, help="cap the number of recipients")
    a = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    people = recipients(cur, only_me=a.only_me, include_relay=not a.skip_relay)
    if a.limit:
        people = people[:a.limit]

    relay_n = sum(1 for p in people if p["relay"])
    print(f"campaign : {CAMPAIGN}")
    print(f"subject  : {SUBJECT}")
    print(f"from     : {RESEND_FROM}")
    print(f"postal   : {POSTAL or '*** NOT SET ***'}")
    print(f"recipients: {len(people)}  (Apple relay: {relay_n})")

    cur.execute("SELECT COUNT(*) AS n FROM users WHERE email_opt_out")
    print(f"opted out : {cur.fetchone()['n']} (excluded)")
    cur.execute("SELECT COUNT(*) AS n FROM email_sends WHERE campaign=%s", (CAMPAIGN,))
    print(f"already sent: {cur.fetchone()['n']} (excluded)")

    if not a.send:
        print("\n── DRY RUN — nothing sent. Pass --send to mail these people. ──")
        for p in people[:10]:
            print(f"   {p['id']:>5}  {p['email']}{'  [apple relay]' if p['relay'] else ''}")
        if len(people) > 10:
            print(f"   … and {len(people)-10} more")
        html, text = render("season_opener", people[0]["id"] if people else 0)
        print(f"\n   html {len(html)} bytes · text {len(text)} bytes")
        print(f"   unsubscribe example: {unsub_url(people[0]['id']) if people else 'n/a'}")
        conn.rollback(); conn.close()
        return 0

    # ── guardrails before anything leaves ──
    if not RESEND_KEY:
        print("\n❌ RESEND_API_KEY is not set."); return 1
    if not POSTAL:
        print("\n❌ POSTAL_ADDRESS is not set.")
        print("   CAN-SPAM requires a physical postal address in commercial email.")
        print("   Add POSTAL_ADDRESS to .env, e.g.")
        print('   POSTAL_ADDRESS="ydkball llc · 123 Example St, City, ST 00000"')
        return 1
    if not people:
        print("\nNothing to send — everyone has already received it or opted out.")
        return 0

    print(f"\nSending to {len(people)} people in batches of {BATCH_SIZE}…\n")
    sent = failed = 0
    for i, p in enumerate(people, 1):
        # Claim the recipient FIRST. If this raises, someone else (or an earlier
        # run) already has them, and we must not send again.
        try:
            cur.execute("""INSERT INTO email_sends (campaign, user_id, email, status)
                           VALUES (%s,%s,%s,'sending')""",
                        (CAMPAIGN, p["id"], p["email"]))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            print(f"  [{i}/{len(people)}] {p['email']} — already sent, skipping")
            continue

        html, text = render("season_opener", p["id"])
        pid, err = send_one(p["email"], html, text, p["id"])
        if err:
            failed += 1
            cur.execute("""UPDATE email_sends SET status=%s WHERE campaign=%s AND user_id=%s""",
                        (f"failed:{err[:80]}", CAMPAIGN, p["id"]))
            print(f"  [{i}/{len(people)}] {p['email']} — FAILED {err}")
        else:
            sent += 1
            cur.execute("""UPDATE email_sends SET status='sent', provider_id=%s
                            WHERE campaign=%s AND user_id=%s""", (pid, CAMPAIGN, p["id"]))
            print(f"  [{i}/{len(people)}] {p['email']} — ok")
        conn.commit()

        time.sleep(SEND_PAUSE)
        if i % BATCH_SIZE == 0 and i < len(people):
            print(f"  … batch done, pausing {BATCH_PAUSE:.0f}s")
            time.sleep(BATCH_PAUSE)

    print(f"\nDone. sent={sent} failed={failed}")
    conn.close()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
