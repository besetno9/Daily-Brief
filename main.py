import os
import time
import html
import json
import secrets
import threading
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuth

import psycopg
from psycopg.rows import dict_row

# ============================================================
# Env / Settings
# ============================================================

PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
SESSION_SECRET = os.environ.get("SESSION_SECRET", "")

# Postgres connection string:
#   postgres://user:pass@host:5432/dbname?sslmode=require
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

if not SESSION_SECRET:
    SESSION_SECRET = secrets.token_urlsafe(32)


def _is_https_base(url: str) -> bool:
    try:
        return urlparse(url).scheme == "https"
    except Exception:
        return False


# ============================================================
# App + Sessions
# ============================================================

app = FastAPI()

# NOTE:
# - In production (Cloud Run) you'll be https.
# - For local http dev, you might need to set https_only=False,
#   but we're keeping your current secure settings.
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="none",
    https_only=True,
)

# ============================================================
# OAuth (Google)
# ============================================================

oauth = OAuth()
if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    oauth.register(
        name="google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )

# ============================================================
# Content settings (simple feeds)
# ============================================================

FEEDS = {
    "World": [
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://www.reuters.com/world/rss",
        "https://apnews.com/hub/world-news?outputType=xml",
        "https://feeds.skynews.com/feeds/rss/world.xml",
        "https://www.ft.com/world?format=rss",
    ],
    "US / Policy": [
        "https://rss.nytimes.com/services/xml/rss/nyt/US.xml",
        "https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
        "https://www.politico.com/rss/politics08.xml",
        "https://www.axios.com/rss.xml",
        "https://www.reuters.com/politics/rss",
        "https://apnews.com/hub/politics?outputType=xml",
    ],
    "Markets": [
        "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://feeds.marketwatch.com/marketwatch/topstories/",
        "https://www.reuters.com/business/finance/rss",
        "https://www.ft.com/markets?format=rss",
    ],
}

NEWS_CACHE_TTL = 600
_news_cache = {}

DEFAULT_PREFS = {
    "show_world": True,
    "show_us": True,
    "show_markets": True,
}

# ============================================================
# DB Helpers (Postgres) — LAZY INIT (no startup crash)
# ============================================================

_db_init_lock = threading.Lock()
_db_initialized = False


def db():
    """
    Returns a Postgres connection with dict rows.
    """
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL env var (Postgres connection string).")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db_if_needed():
    """
    Creates tables if they don't exist.
    Runs ONLY when the app actually needs DB access.
    Never runs at startup (so a broken DB doesn't crash the server).
    """
    global _db_initialized
    if _db_initialized:
        return

    with _db_init_lock:
        if _db_initialized:
            return

        conn = db()
        cur = conn.cursor()

        # users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT,
                picture TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # sessions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
        """)

        # prefs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prefs (
                user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                prefs_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.commit()
        conn.close()
        _db_initialized = True


def _db_error_page(title: str, details: str):
    safe_details = html.escape(details)
    return HTMLResponse(
        f"""
        <html>
          <head><meta name="viewport" content="width=device-width, initial-scale=1" /></head>
          <body style="margin:0;padding:22px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;background:#05070c;color:#e5e7eb;">
            <div style="max-width:860px;margin:0 auto;">
              <div style="font-size:20px;font-weight:900;margin-bottom:10px;">{html.escape(title)}</div>
              <div style="opacity:0.85;margin-bottom:14px;">
                The app is running, but the database connection failed.
              </div>
              <pre style="white-space:pre-wrap;background:#0b1220;border:1px solid #1f2937;border-radius:14px;padding:14px;overflow:auto;">
{safe_details}
              </pre>
              <div style="opacity:0.75;margin-top:12px;font-size:13px;">
                Common fixes: DATABASE_URL correct, password correct, and include <b>sslmode=require</b>.
              </div>
            </div>
          </body>
        </html>
        """,
        status_code=500,
    )


def upsert_user(email: str, name: str | None, picture: str | None):
    init_db_if_needed()
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO users (email, name, picture, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(email) DO UPDATE SET
            name=EXCLUDED.name,
            picture=EXCLUDED.picture,
            updated_at=EXCLUDED.updated_at
        """,
        (email, name or "", picture or "", now, now)
    )
    conn.commit()

    cur.execute("SELECT id, email, name, picture FROM users WHERE email=%s", (email,))
    row = cur.fetchone()
    conn.close()
    return row if row else None


def create_session(user_id: int) -> str:
    init_db_if_needed()
    sid = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (id, user_id, created_at, last_seen_at) VALUES (%s,%s,%s,%s)",
        (sid, user_id, now, now)
    )
    conn.commit()
    conn.close()
    return sid


def get_session(sid: str):
    init_db_if_needed()
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id as session_id, s.user_id, s.created_at, s.last_seen_at,
               u.email, u.name, u.picture
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.id = %s
        """,
        (sid,)
    )
    row = cur.fetchone()
    conn.close()
    return row if row else None


def touch_session(sid: str):
    init_db_if_needed()
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE sessions SET last_seen_at=%s WHERE id=%s", (now, sid))
    conn.commit()
    conn.close()


def delete_session(sid: str):
    init_db_if_needed()
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE id=%s", (sid,))
    conn.commit()
    conn.close()


def get_prefs(user_id: int):
    init_db_if_needed()
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT prefs_json FROM prefs WHERE user_id=%s", (user_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return DEFAULT_PREFS.copy()

    try:
        saved = json.loads(row["prefs_json"])
        out = DEFAULT_PREFS.copy()
        out.update({k: bool(v) for k, v in saved.items()})
        return out
    except Exception:
        return DEFAULT_PREFS.copy()


def save_prefs(user_id: int, prefs: dict):
    init_db_if_needed()
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO prefs (user_id, prefs_json, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            prefs_json=EXCLUDED.prefs_json,
            updated_at=EXCLUDED.updated_at
        """,
        (user_id, json.dumps(prefs), now)
    )
    conn.commit()
    conn.close()

# ============================================================
# Auth Helpers
# ============================================================

def current_user(request: Request):
    sid = request.session.get("sid")
    if not sid:
        return None
    try:
        s = get_session(sid)
    except Exception:
        # DB down or auth broken; clear session so app doesn't loop-crash
        request.session.clear()
        return None

    if not s:
        request.session.clear()
        return None

    try:
        touch_session(sid)
    except Exception:
        # If touching fails, still allow user object to work
        pass

    return {
        "id": s["user_id"],
        "email": s["email"],
        "name": (s.get("name") or ""),
        "picture": (s.get("picture") or ""),
        "sid": s["session_id"],
    }


def require_login(request: Request):
    if not current_user(request):
        return RedirectResponse("/login", status_code=302)
    return None

# ============================================================
# RSS Helpers
# ============================================================

def _clean_title(title: str) -> str:
    if not title:
        return ""

    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def fetch_rss_cached(key: str, urls: list[str], max_items: int = 15):
    now = time.time()
    cached = _news_cache.get(key)
    if cached and (now - cached["ts"] < NEWS_CACHE_TTL):
        return cached["items"]

    seen = set()
    out = []

    for url in urls:
        try:
            feed = feedparser.parse(url)

            for e in (getattr(feed, "entries", []) or []):
                title = (getattr(e, "title", "") or "").strip()
                link = (getattr(e, "link", "") or "").strip()

                if not title:
                    continue

                cleaned = _clean_title(title)

                if cleaned in seen:
                    continue

                seen.add(cleaned)

                out.append({
                    "title": title,
                    "link": link
                })

                if len(out) >= max_items:
                    break

            if len(out) >= max_items:
                break

        except Exception:
            continue

    _news_cache[key] = {
        "ts": now,
        "items": out
    }

    return out


def build_brief_data(prefs: dict):
    date_str = datetime.now(timezone.utc).strftime("%b %d")
    sections = []

    if prefs.get("show_world", True):
        sections.append({
            "key": "world",
            "title": "🌍 World",
            "items": fetch_rss_cached("world", FEEDS["World"], max_items=15)
        })

    if prefs.get("show_us", True):
        sections.append({
            "key": "us",
            "title": "🏛️ US / Policy",
            "items": fetch_rss_cached("us", FEEDS["US / Policy"], max_items=15)
        })

    if prefs.get("show_markets", True):
        sections.append({
            "key": "markets",
            "title": "📈 Markets",
            "items": fetch_rss_cached("markets", FEEDS["Markets"], max_items=15)
        })

    return {"date": date_str, "sections": sections}

# ============================================================
# Routes
# ============================================================

@app.get("/health")
@app.head("/health")
def health():
    # Keep health simple so it never fails due to DB.
    return {"status": "ok"}


@app.get("/favicon.ico")
def favicon():
    return HTMLResponse("", status_code=204)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=302)

    return HTMLResponse("""
    <html>
      <head><meta name="viewport" content="width=device-width, initial-scale=1" /></head>
      <body style="margin:0;padding:22px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;background:#05070c;color:#e5e7eb;">
        <div style="max-width:720px;margin:0 auto;">
          <div style="font-size:22px;font-weight:900;margin-bottom:6px;">Daily Brief</div>
          <div style="opacity:0.8;margin-bottom:16px;">Sign in with Google to use your dashboard.</div>
          <a href="/login" style="display:inline-block;padding:12px 14px;border-radius:12px;background:#111827;border:1px solid #1f2937;color:#e5e7eb;text-decoration:none;">
            Sign in with Google
          </a>
        </div>
      </body>
    </html>
    """)


@app.get("/auth/google")
async def auth_google(request: Request):
    return await login(request)


@app.get("/login")
async def login(request: Request):
    missing = []
    if not PUBLIC_BASE_URL:
        missing.append("PUBLIC_BASE_URL")
    if not GOOGLE_CLIENT_ID:
        missing.append("GOOGLE_CLIENT_ID")
    if not GOOGLE_CLIENT_SECRET:
        missing.append("GOOGLE_CLIENT_SECRET")
    if not DATABASE_URL:
        missing.append("DATABASE_URL")

    if missing:
        return HTMLResponse(
            "<pre style='font-size:15px;white-space:pre-wrap;'>"
            "Missing env vars:\\n\\n"
            + "\\n".join([f"- {m}" for m in missing]) +
            "\\n\\nSet them in your host secrets, then redeploy."
            "</pre>",
            status_code=500
        )

    # Quick DB check (but DO NOT crash server if it fails)
    try:
        init_db_if_needed()
    except Exception as e:
        return _db_error_page("Database connection failed", str(e))

    redirect_uri = f"{PUBLIC_BASE_URL}/auth/google/callback"
    return await oauth.google.authorize_redirect(request, redirect_uri)


@app.get("/auth/google/callback")
async def auth_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
    except Exception as e:
        return HTMLResponse(f"Login failed during OAuth token step: {html.escape(str(e))}", status_code=400)

    userinfo = token.get("userinfo")
    if not userinfo:
        try:
            userinfo = await oauth.google.parse_id_token(request, token)
        except Exception:
            userinfo = None

    if not userinfo:
        return HTMLResponse("Login failed: no user info returned.", status_code=400)

    email = userinfo.get("email")
    name = userinfo.get("name")
    picture = userinfo.get("picture")

    if not email:
        return HTMLResponse("Login failed: Google did not return email.", status_code=400)

    try:
        user = upsert_user(email=email, name=name, picture=picture)
        if not user:
            return HTMLResponse("Login failed: could not save user.", status_code=500)

        sid = create_session(user_id=user["id"])
        request.session["sid"] = sid

        prefs = get_prefs(user["id"])
        save_prefs(user["id"], prefs)
    except Exception as e:
        return _db_error_page("Database error during login", str(e))

    return RedirectResponse("/dashboard", status_code=302)


@app.get("/logout")
def logout(request: Request):
    sid = request.session.get("sid")
    if sid:
        try:
            delete_session(sid)
        except Exception:
            pass
    request.session.clear()
    return RedirectResponse("/", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    go = require_login(request)
    if go:
        return go

    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    try:
        prefs = get_prefs(user["id"])
    except Exception as e:
        return _db_error_page("Database error loading dashboard", str(e))

    def ck(v): return "checked" if v else ""

    pic = (user.get("picture") or "").strip()
    pic_html = f"<img src='{html.escape(pic)}' style='width:44px;height:44px;border-radius:999px;object-fit:cover;'/>" if pic else ""

    return HTMLResponse(f"""
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Daily Brief Dashboard</title>
      </head>
      <body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;background:#05070c;color:#e5e7eb;">
        <div style="max-width:980px;margin:0 auto;padding:18px;">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px;">
            <div style="display:flex;align-items:center;gap:12px;">
              {pic_html}
              <div>
                <div style="font-size:18px;font-weight:900;">Daily Brief</div>
                <div style="opacity:0.7;font-size:13px;">Signed in as {html.escape(user.get("email",""))}</div>
              </div>
            </div>
            <div style="display:flex;gap:10px;">
              <a href="/logout" style="padding:10px 12px;border-radius:12px;background:#111827;border:1px solid #1f2937;color:#e5e7eb;text-decoration:none;">Logout</a>
            </div>
          </div>

          <div style="display:grid;grid-template-columns: 1fr;gap:12px;">
            <div style="padding:14px;border-radius:16px;background:#0b1220;border:1px solid #1f2937;">
              <div style="font-weight:900;margin-bottom:10px;">Sections</div>
              <form method="post" action="/prefs" style="display:grid;grid-template-columns: repeat(2, minmax(0,1fr));gap:8px 14px;">
                <label style="display:flex;gap:8px;align-items:center;">
                  <input type="checkbox" name="show_world" {ck(prefs.get("show_world", True))}/> World
                </label>
                <label style="display:flex;gap:8px;align-items:center;">
                  <input type="checkbox" name="show_us" {ck(prefs.get("show_us", True))}/> US / Policy
                </label>
                <label style="display:flex;gap:8px;align-items:center;">
                  <input type="checkbox" name="show_markets" {ck(prefs.get("show_markets", True))}/> Markets
                </label>

                <div style="grid-column:1 / -1;display:flex;gap:10px;align-items:center;margin-top:8px;">
                  <button type="submit" style="padding:10px 12px;border-radius:12px;background:#111827;border:1px solid #1f2937;color:#e5e7eb;cursor:pointer;">Save</button>
                  <button type="button" onclick="loadBrief()" style="padding:10px 12px;border-radius:12px;background:#111827;border:1px solid #1f2937;color:#e5e7eb;cursor:pointer;">Refresh</button>
                  <span id="status" style="opacity:0.7;font-size:13px;"></span>
                </div>
              </form>
            </div>

            <div style="padding:14px;border-radius:16px;background:#0b1220;border:1px solid #1f2937;">
              <div id="brief"></div>
            </div>
          </div>
        </div>

        <script>
          async function loadBrief() {{
            const status = document.getElementById("status");
            status.textContent = "Loading...";
            try {{
              const r = await fetch("/brief.data.json", {{cache: "no-store"}});
              const data = await r.json();

              let out = `<div style="font-size:18px;font-weight:900;margin-bottom:12px;">Daily Brief — ${'{'}data.date{'}'}</div>`;
              for (const sec of data.sections) {{
                out += `<div style="margin:14px 0 6px;font-weight:900;">${'{'}sec.title{'}'}</div>`;
                if (!sec.items || sec.items.length === 0) {{
                  out += `<div style="opacity:0.8;">• (no items)</div>`;
                  continue;
                }}
                out += `<ul style="margin:0;padding-left:18px;">`;
                for (const it of sec.items) {{
                  const t = it.title || "";
                  const link = it.link || "";
                  if (link) {{
                    out += `<li style="margin:6px 0;"><a href="${'{'}link{'}'}" target="_blank" style="color:#7dd3fc;text-decoration:none;">${'{'}t{'}'}</a></li>`;
                  }} else {{
                    out += `<li style="margin:6px 0;color:#e5e7eb;">${'{'}t{'}'}</li>`;
                  }}
                }}
                out += `</ul>`;
              }}

              document.getElementById("brief").innerHTML = out;
              status.textContent = "Updated";
            }} catch (e) {{
              status.textContent = "Error loading brief";
            }}
          }}

          loadBrief();
        </script>
      </body>
    </html>
    """)


@app.post("/prefs")
def prefs_save(
    request: Request,
    show_world: str | None = Form(None),
    show_us: str | None = Form(None),
    show_markets: str | None = Form(None),
):
    go = require_login(request)
    if go:
        return go

    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    prefs = {
        "show_world": bool(show_world),
        "show_us": bool(show_us),
        "show_markets": bool(show_markets),
    }

    try:
        save_prefs(user["id"], prefs)
    except Exception as e:
        return _db_error_page("Database error saving preferences", str(e))

    return RedirectResponse("/dashboard", status_code=302)


@app.get("/brief.data.json")
def brief_data_json(request: Request):
    go = require_login(request)
    if go:
        return JSONResponse({"error": "not_logged_in"}, status_code=401)

    user = current_user(request)
    if not user:
        return JSONResponse({"error": "not_logged_in"}, status_code=401)

    try:
        prefs = get_prefs(user["id"])
    except Exception as e:
        return JSONResponse({"error": "db_error", "details": str(e)}, status_code=500)

    return build_brief_data(prefs)
