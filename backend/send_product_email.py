"""
ydkball — product email sender (CLI)
=============================================================
  python backend/send_product_email.py                 # dry run (default)
  python backend/send_product_email.py --only-me       # real send, just you
  python backend/send_product_email.py --send          # the real thing

A thin wrapper over backend/emailer.py, which the /admin Email tab also uses —
one send path, so the protections cannot be sidestepped by picking a different
entry point:

  * anyone with email_opt_out is never selected
  * email_sends (campaign, user_id) is claimed BEFORE the API call, so a run
    that dies partway resumes rather than re-mailing everyone it reached
  * every message carries an unsubscribe link + List-Unsubscribe headers
  * refuses to send without POSTAL_ADDRESS (CAN-SPAM)

This CLI sends the prewritten backend/emails/season_opener.* campaign. For
ad-hoc mail, use the Email tab on /admin.

Env (.env): RESEND_API_KEY, RESEND_FROM, POSTAL_ADDRESS, SECRET_KEY
"""
import os, sys, argparse
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()
import emailer  # noqa: E402

CAMPAIGN = "season_opener_2026"
SUBJECT  = "The NBA season is underway"


def body_text():
    # The day-2 copy, not season_opener.txt. The first send went out on opening
    # night and said so; everyone left is being reached after tip-off, and a
    # mail that opens "Opening night is Tuesday" on the Thursday of week one
    # reads as a mistake. emailer.strip_footer handles any footer in the file.
    with open(os.path.join(emailer.TEMPLATES, "season_opener_body_day2.txt")) as f:
        return f.read().rstrip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="actually send (default is a dry run)")
    ap.add_argument("--only-me", action="store_true", help="send only to TEST_EMAIL")
    ap.add_argument("--skip-relay", action="store_true", help="skip Apple Private Relay addresses")
    ap.add_argument("--limit", type=int, help="cap the number of recipients")
    a = ap.parse_args()

    only = os.getenv("TEST_EMAIL", "ydkbball@gmail.com") if a.only_me else None
    postal = (os.getenv("POSTAL_ADDRESS") or "").strip()

    conn = psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)
    cur = conn.cursor()
    people = emailer.audience(cur, CAMPAIGN, only, include_relay=not a.skip_relay)
    if a.limit:
        people = people[:a.limit]

    print(f"campaign  : {CAMPAIGN}")
    print(f"subject   : {SUBJECT}")
    print(f"from      : {os.getenv('RESEND_FROM', '(unset)')}")
    print(f"postal    : {postal or '*** NOT SET ***'}")
    print(f"recipients: {len(people)}  (Apple relay: {sum(1 for p in people if p['relay'])})")
    cur.execute("SELECT COUNT(*) AS n FROM users WHERE email_opt_out")
    print(f"opted out : {cur.fetchone()['n']} (excluded)")
    cur.execute("SELECT COUNT(*) AS n FROM email_sends WHERE campaign=%s", (CAMPAIGN,))
    print(f"already   : {cur.fetchone()['n']} (excluded)")

    if not a.send and not a.only_me:
        print("\n── DRY RUN — nothing sent. Pass --send to mail these people. ──")
        for p in people[:10]:
            print(f"   {p['id']:>5}  {p['email']}{'  [apple relay]' if p['relay'] else ''}")
        if len(people) > 10:
            print(f"   … and {len(people)-10} more")
        if people:
            html, text = emailer.render(SUBJECT, emailer.body_to_html(body_text()),
                                        people[0]["id"], postal)
            print(f"\n   html {len(html)} bytes · text {len(text)} bytes")
            print(f"   unsubscribe example: {emailer.unsub_url(people[0]['id'])}")
        conn.rollback(); conn.close()
        return 0

    conn.rollback(); conn.close()
    print(f"\nSending to {len(people)}…\n")
    res = emailer.send_campaign(
        CAMPAIGN, SUBJECT, body_text(), only_email=only,
        include_relay=not a.skip_relay, limit=a.limit,
        on_progress=lambda p: print(f"  [{p['done']}/{p['total']}] "
                                    f"sent={p['sent']} failed={p['failed']}", flush=True))
    if not res.get("ok"):
        print("\n❌ " + "\n❌ ".join(res.get("problems", ["unknown error"])))
        return 1
    print(f"\nDone. sent={res['sent']} failed={res['failed']} skipped={res['skipped']}")
    return 0 if res["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
