"""
ydkball — Google OAuth
==============================
Registers a Flask Blueprint at /auth/* that handles:
  GET /auth/google/login     → redirect to Google consent screen
  GET /auth/google/callback  → handle token exchange, create/find user
  GET /auth/me               → return current session user (or 401)
  POST /auth/logout          → clear session

Usage in server.py:
  from auth import auth_bp
  app.register_blueprint(auth_bp)
"""

import os
from flask import Blueprint, redirect, url_for, session, jsonify, request
from authlib.integrations.flask_client import OAuth

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


def upsert_user(google_id: str, email: str, display_name: str) -> dict:
    """Insert or update a user row, return the full user dict."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO users (google_id, email, display_name, avatar_url)
        VALUES (%s, %s, %s, '')
        ON CONFLICT (google_id) DO UPDATE SET
            email      = EXCLUDED.email,
            updated_at = NOW()
        RETURNING id, google_id, email, display_name, avatar_url, favorite_team, created_at
    """, (google_id, email, display_name))
    user = dict(cur.fetchone())
    conn.commit()
    cur.close(); conn.close()
    return user


def current_user() -> dict | None:
    """Return the user dict from the session, or None if not logged in."""
    return session.get("user")


def login_required(f):
    """Decorator for routes that need a logged-in user."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            return jsonify({"error": "Authentication required"}), 401
        return f(*args, **kwargs)
    return decorated


# ── Routes ────────────────────────────────────────────────────────────────────

@auth_bp.route("/google/login")
def google_login():
    """Redirect the user to Google's OAuth consent screen."""
    # Store a 'next' URL so we can redirect back after login
    next_url = request.args.get("next", "/")
    session["oauth_next"] = next_url
    # Force http in development to prevent scheme mismatch with Google's
    # registered redirect URI. In production on Railway (https), remove this.
    redirect_uri = url_for("auth.google_callback", _external=True,
                           _scheme="http" if os.getenv("FLASK_ENV") != "production" else "https")
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
    user = upsert_user(google_id, email, display_name)

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

    next_url = session.pop("oauth_next", "/")
    return redirect(next_url)


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
            "SELECT avatar_url, favorite_team FROM users WHERE id = %s",
            (user["id"],)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            user = dict(user)
            user["avatar_url"]    = row["avatar_url"] or ""
            user["favorite_team"] = row["favorite_team"] or ""
    except Exception:
        pass  # fall back to session values if DB is unavailable

    return jsonify({"user": user})


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