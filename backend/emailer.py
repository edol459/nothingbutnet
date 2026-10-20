"""
ydkball — shared product-email machinery
=============================================================
Imported by both send_product_email.py (CLI) and the /admin Email tab, so
there is exactly one send path and the protections cannot be bypassed by
choosing a different entry point:

  * nobody with email_opt_out is ever selected
  * email_sends (campaign, user_id) is claimed BEFORE the API call, so a run
    that dies partway resumes instead of re-mailing everyone it reached
  * every message carries a per-user unsubscribe link + List-Unsubscribe
  * POSTAL_ADDRESS is required; send_campaign refuses without it

Sends are paced. ydkball.net was registered in April 2026 with no sending
reputation, and a few hundred messages in one burst is what trips filters.
"""
import os, json, time, html as _html
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

RESEND_ENDPOINT = "https://api.resend.com/emails"
BATCH_SIZE  = 25
BATCH_PAUSE = 20.0
SEND_PAUSE  = 0.6

TEMPLATES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "emails")


def _env(k, d=""):
    """Env var, with surrounding quotes removed.

    Railway's raw variable editor stores KEY="value" with the quotes as part
    of the value, and a quoted POSTAL_ADDRESS renders literally in the footer
    of every email. Strip them here rather than relying on how it was pasted.
    """
    v = (os.getenv(k) or d).strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1].strip()
    return v


def unsub_url(user_id) -> str:
    """Must match _unsub_token() in server.py — same secret, same input string."""
    import hmac, hashlib
    secret = _env("SECRET_KEY").encode() or b"ydkball-dev"
    tok = hmac.new(secret, f"unsub:{user_id}".encode(), hashlib.sha256).hexdigest()[:32]
    return f"https://ydkball.net/unsubscribe?u={user_id}&t={tok}"


FOOTER_MARK = "You're getting this because"


def strip_footer(body: str) -> str:
    """Drop a footer the author pasted in with the copy.

    The template appends its own unsubscribe line and postal address. Copy
    taken from season_opener.txt carries one too, so without this the reader
    sees both — which is what the first live test produced.
    """
    for mark in (FOOTER_MARK, "Unsubscribe:"):
        i = (body or "").find(mark)
        if i != -1:
            body = body[:i]
    return (body or "").rstrip()


def body_to_html(body: str) -> str:
    """Composer text → email HTML.

    Everything is escaped first, then a small whitelist is restored, so a stray
    `<` is still a typo rather than markup that eats the rest of the message.
    What survives:

        <b>…</b>  <strong>  <i>  <em>        and  **bold**

    A block whose FIRST LINE is entirely bold renders as a heading with the
    rest beneath it, tighter. That is how feature lists get written in practice
    — a short label, then a sentence — and it saves hand-building each one.

    Unclosed bold is closed at end of line. One missing </b> would otherwise
    bold every remaining section.
    """
    import re

    P_BODY = ('<p style="margin:0 0 16px;font:400 15px/1.6 -apple-system,Segoe UI,'
              'sans-serif;color:#4a443c;">')
    P_HEAD = ('<p style="margin:0 0 3px;font:600 15px/1.45 -apple-system,Segoe UI,'
              'sans-serif;color:#1a1a1a;">')
    P_UNDER = ('<p style="margin:0 0 16px;font:400 14px/1.6 -apple-system,Segoe UI,'
               'sans-serif;color:#6b6358;">')

    def inline(s: str) -> str:
        # quote=False: this is a text node, not an attribute, so escaping
        # apostrophes to &#x27; only adds noise.
        s = _html.escape(s, quote=False)
        for tag in ("b", "strong", "i", "em"):
            s = s.replace(f"&lt;{tag}&gt;", f"<{tag}>").replace(f"&lt;/{tag}&gt;", f"</{tag}>")
        s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
        # Close what the author left open, per line.
        for tag in ("b", "strong", "i", "em"):
            if s.count(f"<{tag}>") > s.count(f"</{tag}>"):
                s += f"</{tag}>" * (s.count(f"<{tag}>") - s.count(f"</{tag}>"))
        return s.strip()

    def is_all_bold(s: str) -> bool:
        return bool(re.fullmatch(r"<(b|strong)>.*</(b|strong)>[:：]?", s.strip()))

    out = []
    for block in re.split(r"\n\s*\n", strip_footer(body).replace("\r\n", "\n")):
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        if not lines:
            continue
        head = inline(lines[0])
        if len(lines) > 1 and is_all_bold(head):
            out.append(P_HEAD + head + "</p>")
            out.append(P_UNDER + "<br>".join(inline(l) for l in lines[1:]) + "</p>")
        elif is_all_bold(head) and len(lines) == 1:
            out.append(P_HEAD + head + "</p>")
        else:
            out.append(P_BODY + "<br>".join(inline(l) for l in lines) + "</p>")
    return "\n".join(out)


def render(subject: str, body_html: str, user_id, postal: str,
           template: str = "shell"):
    with open(os.path.join(TEMPLATES, f"{template}.html")) as f:
        html = f.read()
    html = (html.replace("{{subject}}", _html.escape(subject))
                .replace("{{body_html}}", body_html)
                .replace("{{unsubscribe_url}}", unsub_url(user_id))
                .replace("{{postal_address}}", _html.escape(postal)))
    # Text part built from the same source, so the two cannot drift.
    import re
    text = re.sub(r"<[^>]+>", "", body_html)
    text = _html.unescape(text).strip()
    text += (f"\n\n\nYou're getting this because you have a ydkball account."
             f"\nUnsubscribe: {unsub_url(user_id)}\n{postal}")
    return html, text


def audience(cur, campaign: str, only_email: str = None, include_relay=True):
    """Everyone eligible for this campaign who has not already received it."""
    sql = """
        SELECT u.id, u.email, u.display_name,
               (u.email ILIKE '%%privaterelay.appleid.com') AS relay
          FROM users u
         WHERE u.email IS NOT NULL AND u.email <> ''
           AND u.email_opt_out = FALSE
           AND NOT EXISTS (SELECT 1 FROM email_sends e
                            WHERE e.campaign = %s AND e.user_id = u.id)
    """
    p = [campaign]
    if only_email:
        sql += " AND LOWER(u.email) = LOWER(%s)"; p.append(only_email)
    if not include_relay:
        sql += " AND u.email NOT ILIKE '%%privaterelay.appleid.com'"
    sql += " ORDER BY u.id"
    cur.execute(sql, p)
    return cur.fetchall()


def send_one(to_email, subject, html, text, user_id):
    key = _env("RESEND_API_KEY")
    if not key:
        return None, "RESEND_API_KEY not set"
    try:
        r = requests.post(
            RESEND_ENDPOINT,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            data=json.dumps({
                "from": _env("RESEND_FROM", "ydkball <hello@ydkball.net>"),
                "to": [to_email], "subject": subject, "html": html, "text": text,
                # From must stay on the Resend-verified domain or DKIM stops
                # aligning. REPLY_TO lets replies go somewhere else — useful
                # when no mailbox exists at that domain yet, because a reply to
                # an address nobody reads is worse than no reply address.
                **({"reply_to": [_env("REPLY_TO")]} if _env("REPLY_TO") else {}),
                "headers": {
                    "List-Unsubscribe": f"<{unsub_url(user_id)}>",
                    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
                },
            }), timeout=30)
    except Exception as e:
        return None, f"network: {e}"
    if r.status_code >= 300:
        return None, f"{r.status_code} {r.text[:160]}"
    return r.json().get("id"), None


def preflight(campaign: str, subject: str, body: str):
    """Reasons this campaign must not send. Empty list means go."""
    problems = []
    if not _env("RESEND_API_KEY"):
        problems.append("RESEND_API_KEY is not set on the server")
    if not _env("POSTAL_ADDRESS"):
        problems.append("POSTAL_ADDRESS is not set — CAN-SPAM requires a postal "
                        "address in commercial email")
    if not (campaign or "").strip():
        problems.append("campaign id is required (it is the never-send-twice key)")
    if not (subject or "").strip():
        problems.append("subject is required")
    if not (body or "").strip():
        problems.append("body is required")
    return problems


def send_campaign(campaign, subject, body, only_email=None, include_relay=True,
                  limit=None, on_progress=None, database_url=None):
    """Run a campaign to completion. Opens its OWN connection.

    Called from a background thread (a 346-person send takes ~7 minutes and
    gunicorn kills a request at 60s), and server.py's get_conn() is
    thread-local, so borrowing it across threads is not safe.
    """
    problems = preflight(campaign, subject, body)
    if problems:
        return {"ok": False, "problems": problems}

    postal = _env("POSTAL_ADDRESS")
    body_html = body_to_html(body)
    conn = psycopg2.connect(database_url or os.environ["DATABASE_URL"],
                            cursor_factory=RealDictCursor)
    cur = conn.cursor()
    people = audience(cur, campaign, only_email, include_relay)
    if limit:
        people = people[:limit]

    sent = failed = skipped = 0
    for i, p in enumerate(people, 1):
        try:
            cur.execute("""INSERT INTO email_sends (campaign, user_id, email, status)
                           VALUES (%s,%s,%s,'sending')""", (campaign, p["id"], p["email"]))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback(); skipped += 1; continue

        html, text = render(subject, body_html, p["id"], postal)
        pid, err = send_one(p["email"], subject, html, text, p["id"])
        if err:
            failed += 1
            cur.execute("UPDATE email_sends SET status=%s WHERE campaign=%s AND user_id=%s",
                        (f"failed:{err[:80]}", campaign, p["id"]))
        else:
            sent += 1
            cur.execute("""UPDATE email_sends SET status='sent', provider_id=%s
                            WHERE campaign=%s AND user_id=%s""", (pid, campaign, p["id"]))
        conn.commit()
        if on_progress:
            on_progress({"done": i, "total": len(people), "sent": sent, "failed": failed})

        time.sleep(SEND_PAUSE)
        if i % BATCH_SIZE == 0 and i < len(people):
            time.sleep(BATCH_PAUSE)

    cur.close(); conn.close()
    return {"ok": True, "sent": sent, "failed": failed, "skipped": skipped,
            "total": len(people)}
