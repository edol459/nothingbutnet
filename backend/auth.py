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


def upsert_user(google_id: str, email: str, display_name: str, avatar_url: str) -> dict:
    """Insert or update a user row, return the full user dict."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO users (google_id, email, display_name, avatar_url)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (google_id) DO UPDATE SET
            email        = EXCLUDED.email,
            display_name = EXCLUDED.display_name,
            avatar_url   = EXCLUDED.avatar_url,
            updated_at   = NOW()
        RETURNING id, google_id, email, display_name, avatar_url, created_at
    """, (google_id, email, display_name, avatar_url))
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
    return oauth.google.authorize_redirect(redirect_uri)


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
    avatar_url   = userinfo.get("picture", "")

    user = upsert_user(google_id, email, display_name, avatar_url)

    # Serialise dates for session storage
    user["created_at"] = str(user.get("created_at", ""))

    session["user"] = user
    session.permanent = True

    next_url = session.pop("oauth_next", "/")
    return redirect(next_url)


@auth_bp.route("/me")
def me():
    """Return the currently logged-in user, or 401."""
    user = current_user()
    if not user:
        return jsonify({"user": None}), 401
    return jsonify({"user": user})


@auth_bp.route("/logout", methods=["POST"])
def logout():
    """Clear the session."""
    session.clear()
    return jsonify({"ok": True})