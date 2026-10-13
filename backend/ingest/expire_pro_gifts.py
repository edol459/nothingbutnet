"""
ydkball — expire gifted Pro
=============================================================
python backend/ingest/expire_pro_gifts.py [--apply]

Takes back Pro from users whose gift has run out. Runs daily from
daily_update.py (cloud_daily).

Why a sweep and not a computed read:
    `users.is_pro` is consulted in 44 places. Making every one of them also
    weigh `pro_until` would mean 44 chances to miss one and leave a grant
    half-honoured. A gift is not a payment, so a few hours of overrun costs
    nothing, and one nightly pass keeps the boolean honest.

The case this exists to get right:
    Someone is gifted a month, likes it, and subscribes before it lapses. Their
    `pro_until` is still set, so a naive sweep would revoke a paying customer.
    Before revoking anyone, this checks `revenue_events` for a live subscription
    and, finding one, clears `pro_until` instead — the gift stops governing and
    the webhook takes over.

Dry run by default. Pass --apply to write.
"""
import os, sys, argparse
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Kept in step with the webhook's sets in server.py. If those change, change these.
ACTIVE_EVENTS   = ("INITIAL_PURCHASE", "RENEWAL", "REACTIVATION",
                   "PRODUCT_CHANGE", "TRANSFER")
INACTIVE_EVENTS = ("EXPIRATION", "BILLING_ISSUE")


def _has_live_subscription(cur, user_id) -> bool:
    """True if this user's most recent RevenueCat event means 'subscribed'.

    Ordered by event_at rather than insertion: webhooks can arrive out of order,
    and the newest row is not always the newest event. Events we do not classify
    (CANCELLATION, SUBSCRIBER_ALIAS…) are skipped rather than treated as a
    revocation — CANCELLATION only means auto-renew is off, not that access ended.
    """
    cur.execute("""
        SELECT event_type
          FROM revenue_events
         WHERE user_id = %s
           AND event_type = ANY(%s)
         ORDER BY event_at DESC NULLS LAST, id DESC
         LIMIT 1
    """, (user_id, list(ACTIVE_EVENTS + INACTIVE_EVENTS)))
    row = cur.fetchone()
    return bool(row and row["event_type"] in ACTIVE_EVENTS)


def run(apply: bool):
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur  = conn.cursor()

    cur.execute("""
        SELECT id, email, display_name, pro_until
          FROM users
         WHERE is_pro = TRUE
           AND pro_until IS NOT NULL
           AND pro_until <= NOW()
         ORDER BY pro_until
    """)
    lapsed = cur.fetchall()

    if not lapsed:
        print("No gifted Pro has lapsed. Nothing to do.")
        cur.close(); conn.close()
        return 0

    revoked, kept = [], []
    for u in lapsed:
        if _has_live_subscription(cur, u["id"]):
            kept.append(u)
            if apply:
                # Subscriber now — the gift no longer governs, so stop tracking it.
                cur.execute("UPDATE users SET pro_until = NULL WHERE id = %s", (u["id"],))
        else:
            revoked.append(u)
            if apply:
                cur.execute("""
                    UPDATE users SET is_pro = FALSE, pro_until = NULL WHERE id = %s
                """, (u["id"],))

    if apply:
        conn.commit()

    verb = "Revoked" if apply else "Would revoke"
    print(f"{verb} gifted Pro for {len(revoked)} user(s):")
    for u in revoked:
        print(f"  - {u['id']:>5}  {u['email']}  (lapsed {u['pro_until'].date()})")
    if kept:
        print(f"\nKept Pro for {len(kept)} user(s) who subscribed since being gifted:")
        for u in kept:
            print(f"  - {u['id']:>5}  {u['email']}")
    if not apply:
        print("\n(dry run — pass --apply to write)")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write the changes")
    a = ap.parse_args()
    sys.exit(run(a.apply))
