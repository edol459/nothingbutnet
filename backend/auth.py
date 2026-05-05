"""
ydkball — Google OAuth + Sign in with Apple
==============================
Registers a Flask Blueprint at /auth/* that handles:
  GET  /auth/google/login     → redirect to Google consent screen
  GET  /auth/google/callback  → handle token exchange, create/find user
  POST /auth/apple            → verify Apple identity token, create/find user
  GET  /auth/me               → return current session user (or 401)
  POST /auth/logout           → clear session

Usage in server.py:
  from auth import auth_bp
  app.register_blueprint(auth_bp)
"""

import os
import secrets
import requests as _requests
from flask import Blueprint, redirect, url_for, session, jsonify, request
from authlib.integrations.flask_client import OAuth
from authlib.jose import JsonWebToken, JsonWebKey

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# Authlib OAuth registry — bound to the app in init_oauth()
oauth = OAuth()


def init_oauth(app):
    """Call this after creating your Flask app: init_oauth(app)"""
    oauth.init_app(app)
    oauth.register(
        name="google",
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={
            "scope": "openid email profile",
        },
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn():
    """Import lazily to avoid circular imports."""
    import psycopg2, psycopg2.extras
    return psycopg2.connect(os.getenv("DATABASE_URL"),
                            cursor_factory=psycopg2.extras.RealDictCursor)


def upsert_user(google_id: str, email: str, display_name: str, picture_url: str = "") -> dict:
    """Insert or update a user row, return the full user dict."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO users (google_id, email, display_name, avatar_url)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (google_id) DO UPDATE SET
            email      = EXCLUDED.email,
            updated_at = NOW(),
            avatar_url = CASE
                WHEN users.avatar_url = '' OR users.avatar_url IS NULL
                THEN EXCLUDED.avatar_url
                ELSE users.avatar_url
            END
        RETURNING id, google_id, email, display_name, avatar_url, favorite_team, created_at
    """, (google_id, email, display_name, picture_url))
    user = dict(cur.fetchone())
    conn.commit()
    cur.close(); conn.close()
    return user


def _get_user_from_mobile_token(token: str) -> dict | None:
    """Return a minimal user dict if the mobile token matches a user row."""
    if not token:
        return None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id, google_id, email, display_name FROM users WHERE mobile_token = %s",
            (token,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            return {
                "id":           row["id"],
                "google_id":    row["google_id"],
                "email":        row["email"],
                "display_name": row["display_name"],
                "created_at":   "",
            }
    except Exception:
        pass
    return None


def current_user() -> dict | None:
    """Return the user dict from the session or mobile Bearer token, or None."""
    user = session.get("user")
    if user:
        return user
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return _get_user_from_mobile_token(auth_header[7:].strip())
    return None


def login_required(f):
    """Decorator for routes that need a logged-in user."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            return jsonify({"error": "Authentication required"}), 401
        return f(*args, **kwargs)
    return decorated


# ── Apple Sign-In helpers ─────────────────────────────────────────────────────

_apple_jwks_cache: dict | None = None

def _get_apple_jwks() -> dict:
    global _apple_jwks_cache
    if _apple_jwks_cache is None:
        resp = _requests.get("https://appleid.apple.com/auth/keys", timeout=10)
        _apple_jwks_cache = resp.json()
    return _apple_jwks_cache


def verify_apple_token(id_token: str) -> dict:
    jwks = _get_apple_jwks()
    key_set = JsonWebKey.import_key_set(jwks)
    jwt = JsonWebToken(["RS256"])
    claims = jwt.decode(id_token, key_set)
    claims.validate()
    aud = claims.get("aud")
    if isinstance(aud, list):
        if "net.ydkball.ydkball" not in aud:
            raise ValueError(f"Invalid audience: {aud}")
    elif aud != "net.ydkball.ydkball":
        raise ValueError(f"Invalid audience: {aud}")
    return dict(claims)


def upsert_apple_user(apple_id: str, email: str, display_name: str) -> dict:
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS apple_id TEXT")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_apple_id ON users(apple_id) WHERE apple_id IS NOT NULL")
    # Try to find existing user by apple_id first
    cur.execute("SELECT id FROM users WHERE apple_id = %s", (apple_id,))
    existing = cur.fetchone()
    if existing:
        cur.execute("""
            UPDATE users SET updated_at = NOW()
            WHERE apple_id = %s
            RETURNING id, google_id, apple_id, email, display_name, avatar_url, favorite_team, created_at
        """, (apple_id,))
    else:
        # New Apple user — use a placeholder google_id so the NOT NULL constraint is met
        placeholder_google_id = f"apple_{apple_id}"
        safe_name = display_name or (email.split("@")[0] if email else "ydkball user")
        cur.execute("""
            INSERT INTO users (google_id, apple_id, email, display_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (google_id) DO UPDATE SET
                apple_id   = EXCLUDED.apple_id,
                updated_at = NOW()
            RETURNING id, google_id, apple_id, email, display_name, avatar_url, favorite_team, created_at
        """, (placeholder_google_id, apple_id, email, safe_name))
    user = dict(cur.fetchone())
    conn.commit()
    cur.close(); conn.close()
    return user


def _issue_mobile_token(user_id: int) -> str:
    mobile_token = secrets.token_urlsafe(32)
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS mobile_token TEXT")
    cur.execute("UPDATE users SET mobile_token = %s WHERE id = %s", (mobile_token, user_id))
    conn.commit()
    cur.close(); conn.close()
    return mobile_token


# ── Routes ────────────────────────────────────────────────────────────────────

@auth_bp.route("/google/login")
def google_login():
    """Redirect the user to Google's OAuth consent screen."""
    next_url = request.args.get("next", "/")
    session["oauth_next"] = next_url
    if request.args.get("mobile") == "1":
        session["oauth_mobile"] = True
    # Use OAUTH_REDIRECT_URI env var if set (recommended for production).
    # Falls back to building the URI from the request, preferring https in
    # production (FLASK_ENV=production) and http locally.
    redirect_uri = os.getenv("OAUTH_REDIRECT_URI") or url_for(
        "auth.google_callback", _external=True,
        _scheme="https" if os.getenv("FLASK_ENV") == "production" else "http",
    )
    return oauth.google.authorize_redirect(redirect_uri, prompt="select_account")


@auth_bp.route("/google/callback")
def google_callback():
    """Handle the OAuth callback from Google."""
    try:
        token    = oauth.google.authorize_access_token()
        userinfo = token.get("userinfo") or oauth.google.userinfo()
    except Exception as e:
        return jsonify({"error": f"OAuth failed: {e}"}), 400

    google_id    = userinfo.get("sub")
    email        = userinfo.get("email", "")
    display_name = userinfo.get("name", email.split("@")[0])
    picture_url  = userinfo.get("picture", "")
    user = upsert_user(google_id, email, display_name, picture_url)

    # Store only small fields in the session cookie — avatar_url can be a
    # base64 data URL (~200 KB) which silently overflows the 4 KB cookie limit.
    # /auth/me fetches avatar_url and favorite_team fresh from the DB instead.
    session["user"] = {
        "id":               user["id"],
        "google_id":        user["google_id"],
        "email":            user["email"],
        "display_name":     user["display_name"],
        "created_at":       str(user.get("created_at", "")),
    }
    session.permanent = True

    # Mobile app flow: issue a persistent token and redirect to custom scheme
    if session.pop("oauth_mobile", False):
        try:
            mobile_token = _issue_mobile_token(user["id"])
        except Exception:
            mobile_token = secrets.token_urlsafe(32)
        return redirect(f"ydkball://auth-complete?token={mobile_token}")

    next_url = session.pop("oauth_next", "/")
    return redirect(next_url)


@auth_bp.route("/apple", methods=["POST"])
def apple_signin():
    """Verify an Apple identity token (from the iOS native Sign in with Apple flow)
    and return a mobile_token the app can use as a Bearer token."""
    body = request.get_json() or {}
    id_token  = body.get("identity_token", "").strip()
    full_name = (body.get("full_name") or "").strip()

    if not id_token:
        return jsonify({"error": "identity_token required"}), 400

    # Invalidate JWKS cache on failure so we re-fetch fresh keys next time
    global _apple_jwks_cache
    try:
        claims = verify_apple_token(id_token)
    except Exception:
        _apple_jwks_cache = None
        try:
            claims = verify_apple_token(id_token)
        except Exception as e:
            return jsonify({"error": f"Invalid Apple token: {e}"}), 401

    apple_id = claims.get("sub")
    email    = claims.get("email", "")
    if not apple_id:
        return jsonify({"error": "Missing subject in token"}), 401

    try:
        user         = upsert_apple_user(apple_id, email, full_name)
        mobile_token = _issue_mobile_token(user["id"])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"token": mobile_token})


@auth_bp.route("/me")
def me():
    """Return the currently logged-in user, or 401."""
    user = current_user()
    if not user:
        return jsonify({"user": None}), 401

    # Fetch avatar_url and favorite_team fresh from DB — these can be large
    # (data URLs) or recently changed, so we don't rely on the session cookie.
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT avatar_url, favorite_team, night_mode FROM users WHERE id = %s",
            (user["id"],)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            user = dict(user)
            user["avatar_url"]    = row["avatar_url"] or ""
            user["favorite_team"] = row["favorite_team"] or ""
            user["night_mode"]    = bool(row["night_mode"])
    except Exception:
        pass  # fall back to session values if DB is unavailable

    return jsonify({"user": user})


@auth_bp.route("/dev-login")
def dev_login():
    """
    Local-only shortcut: sets the session without going through Google OAuth.
    Disabled in production — returns 403 if FLASK_ENV == 'production'.

    Usage:
      GET /auth/dev-login                  → signs in as the first user in the DB
      GET /auth/dev-login?email=you@x.com  → signs in as a specific user by email
      GET /auth/dev-login?next=/reviews    → redirects there after sign-in
    """
    if os.getenv("FLASK_ENV") == "production":
        return jsonify({"error": "Dev login is disabled in production"}), 403

    target_email = request.args.get("email", "").strip()
    next_url     = request.args.get("next", "/")

    try:
        conn = get_conn()
        cur  = conn.cursor()
        if target_email:
            cur.execute(
                "SELECT id, google_id, email, display_name FROM users WHERE email = %s LIMIT 1",
                (target_email,)
            )
        else:
            cur.execute(
                "SELECT id, google_id, email, display_name FROM users ORDER BY id LIMIT 1"
            )
        user = cur.fetchone()
        cur.close(); conn.close()
    except Exception as e:
        return jsonify({"error": f"DB error: {e}"}), 500

    if not user:
        return jsonify({"error": "No users in DB yet. Create one via Google OAuth first, then use dev-login."}), 404

    session["user"] = {
        "id":           user["id"],
        "google_id":    user["google_id"],
        "email":        user["email"],
        "display_name": user["display_name"],
        "created_at":   "",
    }
    session.permanent = True
    return redirect(next_url)


@auth_bp.route("/logout", methods=["POST"])
def logout():
    """Clear the session (fetch-based)."""
    session.clear()
    return jsonify({"ok": True})


@auth_bp.route("/logout")
def logout_get():
    """Clear the session and redirect — reliable on iOS PWA where fetch drops cookies."""
    next_url = request.args.get("next", "/")
    session.clear()
    return redirect(next_url)