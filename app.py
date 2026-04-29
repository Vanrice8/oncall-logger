import io
import os
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from urllib import error, parse, request

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="On-Call Logger",
    page_icon="📟",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = Path(os.environ.get("ONCALL_DB_PATH", BASE_DIR / "oncall.db"))

KATEGORIER = [
    "Incident", "Change", "Approval", "Alert",
    "Backjour", "Question", "Info", "Other",
]

JA_NEJ = ["—", "Yes", "No"]


# ── Settings & Auth ───────────────────────────────────────────────────────────

def get_secret(name: str) -> str | None:
    try:
        v = st.secrets.get(name)
    except Exception:
        return None
    return str(v) if v is not None else None


def get_setting(name: str, default: str | None = None) -> str | None:
    return get_secret(name) or os.environ.get(name, default)


def get_app_password() -> str:
    return get_setting("APP_PASSWORD", "DimmanComp8") or "DimmanComp8"


def using_supabase() -> bool:
    return bool(get_setting("SUPABASE_URL") and get_setting("SUPABASE_KEY"))


# ── Supabase ──────────────────────────────────────────────────────────────────

def supabase_request(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    body: dict | list | None = None,
    prefer: str | None = None,
) -> list | dict | None:
    base_url = get_setting("SUPABASE_URL")
    api_key = get_setting("SUPABASE_KEY")
    if not base_url or not api_key:
        raise RuntimeError("Supabase not configured.")
    query = f"?{parse.urlencode(params)}" if params else ""
    url = f"{base_url}/rest/v1/{path}{query}"
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    payload = None if body is None else json.dumps(body).encode()
    req = request.Request(url, data=payload, headers=headers, method=method)
    try:
        with request.urlopen(req) as r:
            raw = r.read()
            return json.loads(raw.decode()) if raw else None
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase {exc.code}: {detail}") from exc


def _fetch_all(table: str, order: str) -> list:
    rows, offset, page = [], 0, 500
    while True:
        batch = supabase_request(
            "GET", table,
            params={"order": order, "limit": str(page), "offset": str(offset)},
        ) or []
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


@st.cache_data(ttl=60)
def _sb_calls() -> list:
    return _fetch_all("oncall_calls", "datum.desc,tid.desc")


@st.cache_data(ttl=60)
def _sb_larm() -> list:
    return _fetch_all("oncall_larm", "datum.desc,tid.desc")


@st.cache_data(ttl=60)
def _sb_members() -> list:
    return supabase_request(
        "GET", "members",
        params={"is_archived": "eq.false", "order": "name.asc"},
    ) or []


def invalidate_cache() -> None:
    _sb_calls.clear()
    _sb_larm.clear()
    _sb_members.clear()


# ── SQLite fallback ───────────────────────────────────────────────────────────

def sqlite_conn() -> sqlite3.Connection:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_sqlite() -> None:
    conn = sqlite_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS oncall_calls (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            chef                TEXT NOT NULL,
            kategori            TEXT,
            datum               TEXT NOT NULL,
            tid                 TEXT,
            tid_lost            TEXT,
            tidsatgang_minutes  INTEGER DEFAULT 0,
            arende              TEXT,
            beskrivning         TEXT,
            kommentar           TEXT,
            relevant            TEXT,
            forbattring         TEXT,
            kontaktat_mod       TEXT,
            vecka               INTEGER,
            manad               TEXT,
            ar                  INTEGER,
            kvartal             INTEGER,
            created_at          TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS oncall_larm (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            im                          TEXT NOT NULL,
            datum                       TEXT NOT NULL,
            tid                         TEXT,
            larm_incidentnummer         TEXT,
            larm_dynatrace_nummer       TEXT,
            beskrivning                 TEXT,
            kommentar                   TEXT,
            atgard_utford               TEXT,
            aterhamtning_forbattring    TEXT,
            vecka                       INTEGER,
            manad                       TEXT,
            ar                          INTEGER,
            kvartal                     INTEGER,
            larminstruktioner_tillagt   TEXT,
            uppfoljning                 TEXT,
            created_at                  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS members (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT UNIQUE NOT NULL,
            nickname    TEXT,
            is_archived INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS entries (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id   INTEGER NOT NULL,
            date        TEXT NOT NULL,
            minutes     INTEGER NOT NULL,
            comment     TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


# ── Data operations ───────────────────────────────────────────────────────────

def load_members() -> list[dict]:
    if using_supabase():
        return _sb_members()
    conn = sqlite_conn()
    return [dict(r) for r in conn.execute(
        "SELECT * FROM members WHERE is_archived=0 ORDER BY name"
    ).fetchall()]


def add_member(name: str) -> None:
    if using_supabase():
        supabase_request("POST", "members", body={"name": name, "is_archived": False}, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        conn.execute("INSERT INTO members (name) VALUES (?)", (name,))
        conn.commit()
    invalidate_cache()


def archive_member(member_id: int) -> None:
    if using_supabase():
        supabase_request("PATCH", f"members?id=eq.{member_id}", body={"is_archived": True}, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        conn.execute("UPDATE members SET is_archived=1 WHERE id=?", (member_id,))
        conn.commit()
    invalidate_cache()


def restore_member(member_id: int) -> None:
    if using_supabase():
        supabase_request("PATCH", f"members?id=eq.{member_id}", body={"is_archived": False}, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        conn.execute("UPDATE members SET is_archived=0 WHERE id=?", (member_id,))
        conn.commit()
    invalidate_cache()


def load_all_members() -> list[dict]:
    if using_supabase():
        return supabase_request("GET", "members", params={"order": "name.asc"}) or []
    conn = sqlite_conn()
    return [dict(r) for r in conn.execute("SELECT * FROM members ORDER BY name").fetchall()]


def load_calls() -> list[dict]:
    if using_supabase():
        return _sb_calls()
    conn = sqlite_conn()
    return [dict(r) for r in conn.execute(
        "SELECT * FROM oncall_calls ORDER BY datum DESC, tid DESC"
    ).fetchall()]


def add_call(row: dict) -> None:
    if using_supabase():
        supabase_request("POST", "oncall_calls", body=row, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        cols = list(row.keys())
        conn.execute(
            f"INSERT INTO oncall_calls ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            [row[c] for c in cols],
        )
        conn.commit()
    invalidate_cache()


def update_call(call_id: int, row: dict) -> None:
    if using_supabase():
        supabase_request("PATCH", "oncall_calls", params={"id": f"eq.{call_id}"}, body=row, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        sets = ", ".join(f"{k}=?" for k in row)
        conn.execute(f"UPDATE oncall_calls SET {sets} WHERE id=?", [*row.values(), call_id])
        conn.commit()
    invalidate_cache()


def delete_call(call_id: int) -> None:
    if using_supabase():
        supabase_request("DELETE", "oncall_calls", params={"id": f"eq.{call_id}"})
    else:
        conn = sqlite_conn()
        conn.execute("DELETE FROM oncall_calls WHERE id=?", (call_id,))
        conn.commit()
    invalidate_cache()


def load_larm() -> list[dict]:
    if using_supabase():
        return _sb_larm()
    conn = sqlite_conn()
    return [dict(r) for r in conn.execute(
        "SELECT * FROM oncall_larm ORDER BY datum DESC, tid DESC"
    ).fetchall()]


def add_larm(row: dict) -> None:
    if using_supabase():
        supabase_request("POST", "oncall_larm", body=row, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        cols = list(row.keys())
        conn.execute(
            f"INSERT INTO oncall_larm ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            [row[c] for c in cols],
        )
        conn.commit()
    invalidate_cache()


def update_larm(larm_id: int, row: dict) -> None:
    if using_supabase():
        supabase_request("PATCH", "oncall_larm", params={"id": f"eq.{larm_id}"}, body=row, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        sets = ", ".join(f"{k}=?" for k in row)
        conn.execute(f"UPDATE oncall_larm SET {sets} WHERE id=?", [*row.values(), larm_id])
        conn.commit()
    invalidate_cache()


def delete_larm(larm_id: int) -> None:
    if using_supabase():
        supabase_request("DELETE", "oncall_larm", params={"id": f"eq.{larm_id}"})
    else:
        conn = sqlite_conn()
        conn.execute("DELETE FROM oncall_larm WHERE id=?", (larm_id,))
        conn.commit()
    invalidate_cache()


def transfer_week_to_comp(member_id: int, week: int, year: int, total_minutes: int) -> None:
    thursday = date.fromisocalendar(year, week, 4)
    thu_start = thursday - timedelta(weeks=1)
    def fmt(d): return f"{d.day}/{d.month}/{str(d.year)[2:]}"
    comment = f"Intjänat under beredskap {fmt(thu_start)}–{fmt(thursday)}"
    row = {
        "member_id": member_id,
        "date": thursday.isoformat(),
        "minutes": total_minutes,
        "comment": comment,
    }
    if using_supabase():
        supabase_request("POST", "entries", body=row, prefer="return=minimal")
    else:
        conn = sqlite_conn()
        conn.execute(
            "INSERT INTO entries (member_id, date, minutes, comment) VALUES (?,?,?,?)",
            (member_id, row["date"], total_minutes, comment),
        )
        conn.commit()


# ── Time utilities ────────────────────────────────────────────────────────────

def parse_time_hhmm(text: str) -> str | None:
    if not text:
        return None
    text = text.strip()
    parts = text.split(":")
    if len(parts) >= 2:
        try:
            h, m = int(parts[0]), int(parts[1])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f"{h:02d}:{m:02d}"
        except ValueError:
            pass
    return None


def calc_minutes(start: str, end: str) -> int:
    try:
        sh, sm = map(int, start.split(":"))
        eh, em = map(int, end.split(":"))
        diff = (eh * 60 + em) - (sh * 60 + sm)
        return diff if diff >= 0 else diff + 24 * 60
    except Exception:
        return 0


def mins_to_hhmm(minutes: int | None) -> str:
    if not minutes:
        return "0:00"
    h, m = divmod(int(minutes), 60)
    return f"{h}:{m:02d}"


def date_meta(d: date) -> dict:
    return {
        "vecka": d.isocalendar()[1],
        "manad": d.strftime("%B"),
        "ar": d.year,
        "kvartal": (d.month - 1) // 3 + 1,
    }


def format_date(iso: str) -> str:
    try:
        d = date.fromisoformat(str(iso)[:10])
        return f"{d.day}/{d.month}/{d.year}"
    except Exception:
        return str(iso)


# ── Excel export ─────────────────────────────────────────────────────────────

def build_beredskap_excel(calls: list[dict]) -> bytes:
    rows = []
    # Sort by chef, ar, vecka, datum, tid for cumulative calc
    sorted_calls = sorted(
        calls,
        key=lambda r: (r.get("chef") or "", r.get("ar") or 0, r.get("vecka") or 0, r.get("datum") or "", r.get("tid") or ""),
    )
    cumulative: dict[tuple, int] = {}
    for r in sorted_calls:
        key = (r.get("chef"), r.get("ar"), r.get("vecka"))
        mins = r.get("tidsatgang_minutes") or 0
        cumulative[key] = cumulative.get(key, 0) + mins
        cum_mins = cumulative[key]
        cum_h, cum_m = divmod(cum_mins, 60)

        rows.append({
            "Chef":             r.get("chef"),
            "Kategori":         r.get("kategori"),
            "Datum":            r.get("datum"),
            "Tid":              r.get("tid"),
            "Tid löst":         r.get("tid_lost"),
            "Tidberäkning":     mins_to_hhmm(mins),
            "Tidsåtgång":       mins_to_hhmm(mins),
            "Sammanlagd tid":   f"{cum_h}:{cum_m:02d}",
            "Ärende":           r.get("arende"),
            "Beskrivning":      r.get("beskrivning"),
            "Kommentar":        r.get("kommentar"),
            "Relevant":         r.get("relevant"),
            "Förbättring":      r.get("forbattring"),
            "Kontaktat MOD":    r.get("kontaktat_mod"),
            "Vecka":            r.get("vecka"),
            "Månad":            r.get("manad"),
            "År":               r.get("ar"),
            "Kvartal":          r.get("kvartal"),
        })

    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Underlag", index=False)
    return buf.getvalue()


def build_larm_excel(larm_list: list[dict]) -> bytes:
    rows = []
    for r in larm_list:
        rows.append({
            "IM":                           r.get("im"),
            "Datum":                        r.get("datum"),
            "Tid":                          r.get("tid"),
            "Larm incidentnummer":          r.get("larm_incidentnummer"),
            "Larm Dynatrace nummer":        r.get("larm_dynatrace_nummer"),
            "Beskrivning":                  r.get("beskrivning"),
            "Kommentar":                    r.get("kommentar"),
            "Åtgärd utförd":               r.get("atgard_utford"),
            "Återhämtning/Förbättring":    r.get("aterhamtning_forbattring"),
            "Vecka":                        r.get("vecka"),
            "Månad":                        r.get("manad"),
            "År":                           r.get("ar"),
            "Kvartal":                      r.get("kvartal"),
            "Larminstruktioner tillagt":    r.get("larminstruktioner_tillagt"),
            "Uppföljning":                  r.get("uppfoljning"),
        })

    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Larm", index=False)
    return buf.getvalue()


# ── CSS / Theme ───────────────────────────────────────────────────────────────

def inject_css() -> None:
    theme_vars = """
        --kt-bg: #152235;
        --kt-surface: #1b2840;
        --kt-surface-soft: #21304b;
        --kt-border: #334867;
        --kt-text: #edf4ff;
        --kt-muted: #9fb0c9;
        --kt-primary: #6ea8ff;
        --kt-primary-dark: #4b8cff;
        --kt-green: #49d7a2;
        --kt-green-bg: rgba(73, 215, 162, 0.16);
        --kt-red: #ff8a8a;
        --kt-red-bg: rgba(255, 138, 138, 0.16);
        --kt-amber: #f59e0b;
        --kt-shadow: 0 8px 24px rgba(0,0,0,0.22);
        --kt-shadow-lg: 0 24px 64px rgba(0,0,0,0.4);
    """
    st.markdown(f"""
        <style>
        :root {{ {theme_vars} }}

        .stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {{
            background: var(--kt-bg);
            color: var(--kt-text);
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #10213d 0%, #1a3d6d 100%);
            border-right: none;
        }}
        [data-testid="stSidebar"] * {{ color: #f8fafc; }}
        [data-testid="stSidebar"] > div:first-child {{ padding-top: 1rem !important; }}
        [data-testid="stSidebar"] .stButton button {{
            background: var(--kt-primary) !important;
            border: none !important;
            color: #fff !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
            transition: all 0.15s ease !important;
        }}
        [data-testid="stSidebar"] .stButton button:hover {{
            background: var(--kt-primary-dark) !important;
            color: #fff !important;
        }}
        [data-testid="stSidebar"] .stLinkButton a {{
            background: var(--kt-primary) !important;
            border: none !important;
            color: #fff !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
            text-align: center !important;
            transition: all 0.15s ease !important;
        }}
        [data-testid="stSidebar"] .stLinkButton a:hover {{
            background: var(--kt-primary-dark) !important;
            color: #fff !important;
        }}
        .block-container {{
            max-width: 1200px;
            padding-top: 1.5rem;
            padding-bottom: 3rem;
        }}
        h1, h2, h3 {{ color: var(--kt-text); letter-spacing: -0.02em; }}
        .kt-hero {{
            background: linear-gradient(135deg, #0f2748 0%, #1d4f91 56%, #2d6ecf 100%);
            border-radius: 18px;
            padding: 1.4rem 1.6rem;
            box-shadow: var(--kt-shadow-lg);
            color: white;
            margin-bottom: 1.1rem;
        }}
        .kt-hero h1 {{ color: white; font-size: 2.2rem; margin: 0; }}
        .kt-hero p {{ margin: 0.35rem 0 0; color: rgba(255,255,255,0.82); font-size: 0.96rem; }}
        .kt-card {{
            background: var(--kt-surface);
            border: 1px solid var(--kt-border);
            border-radius: 16px;
            padding: 1.25rem 1.35rem;
            box-shadow: var(--kt-shadow);
            margin-bottom: 1rem;
        }}
        .kt-card-label {{
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--kt-muted);
            margin-bottom: 0.95rem;
        }}
        .kt-metric {{
            background: var(--kt-surface);
            border: 1px solid var(--kt-border);
            border-radius: 16px;
            padding: 1rem 1.1rem;
            box-shadow: var(--kt-shadow);
        }}
        .kt-metric-label {{
            color: var(--kt-muted);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            margin-bottom: 0.45rem;
        }}
        .kt-metric-value {{
            color: var(--kt-text);
            font-weight: 800;
            font-size: 2rem;
            letter-spacing: -0.03em;
        }}
        .kt-metric-sub {{
            color: var(--kt-muted);
            font-size: 0.82rem;
            font-weight: 500;
            margin-top: 0.25rem;
        }}
        .kt-badge {{
            display: inline-block;
            padding: 0.2rem 0.55rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.04em;
        }}
        .kt-badge.ja  {{ background: var(--kt-green-bg); color: var(--kt-green); }}
        .kt-badge.nej {{ background: var(--kt-red-bg);   color: var(--kt-red);   }}
        .kt-login-shell {{
            min-height: 70vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .kt-login-card {{
            background: var(--kt-surface);
            border-radius: 20px;
            box-shadow: var(--kt-shadow-lg);
            padding: 2.5rem 2.25rem;
            width: 100%;
            max-width: 580px;
            text-align: center;
        }}
        .kt-login-logo  {{ font-size: 2.5rem; margin-bottom: 0.5rem; }}
        .kt-login-title {{ font-size: 1.9rem; font-weight: 800; color: var(--kt-text); margin-bottom: 0.4rem; }}
        .kt-login-sub   {{ color: var(--kt-muted); margin-bottom: 0; font-size: 0.95rem; }}
        .kt-login-tagline {{
            color: var(--kt-muted);
            font-size: 0.88rem;
            margin-top: 1.2rem;
            border-top: 1px solid var(--kt-border);
            padding-top: 1.1rem;
        }}
        [data-testid="stSegmentedControl"] {{ margin: 0.2rem 0 1rem; }}
        [data-testid="stSegmentedControl"] [role="radiogroup"] {{
            gap: 0.6rem;
            background: transparent;
            flex-wrap: wrap;
        }}
        [data-testid="stSegmentedControl"] label {{
            border-radius: 10px;
            border: 1px solid var(--kt-border);
            background: var(--kt-surface);
            color: var(--kt-muted);
            font-weight: 700;
            padding: 0.5rem 0.9rem;
            min-width: 110px;
            justify-content: center;
        }}
        [data-testid="stSegmentedControl"] label[data-selected="true"] {{
            color: var(--kt-primary);
            border-color: var(--kt-primary);
            box-shadow: inset 0 -2px 0 var(--kt-primary);
        }}
        .stButton button, .stDownloadButton button, .stFormSubmitButton button {{
            border-radius: 10px;
            border: 1px solid var(--kt-border);
            font-weight: 700;
        }}
        .stButton button {{ background: var(--kt-surface); color: var(--kt-text); }}
        .stButton button:hover {{ border-color: var(--kt-primary); color: var(--kt-primary); }}
        .stFormSubmitButton button {{
            background: var(--kt-primary);
            color: white;
            border-color: var(--kt-primary);
        }}
        .stFormSubmitButton button:hover {{
            background: var(--kt-primary-dark);
            border-color: var(--kt-primary-dark);
            color: white;
        }}
        .stTextInput input, .stDateInput input,
        .stSelectbox [data-baseweb="select"], .stTextArea textarea {{
            border-radius: 10px;
        }}
        .stDataFrame {{ border-radius: 12px; overflow: hidden; }}
        * {{ scrollbar-width: thick; scrollbar-color: #4a5568 #1e2433; }}
        ::-webkit-scrollbar {{ width: 14px; height: 14px; }}
        ::-webkit-scrollbar-track {{ background: #1e2433; border-radius: 6px; }}
        ::-webkit-scrollbar-thumb {{ background: #4a5568; border-radius: 6px; border: 2px solid #1e2433; }}
        ::-webkit-scrollbar-thumb:hover {{ background: #6b7280; }}
        </style>
        <script>
        (function() {{
          const SCROLL_CSS = `
            ::-webkit-scrollbar {{ width: 14px !important; height: 14px !important; }}
            ::-webkit-scrollbar-track {{ background: #1e2433; border-radius: 6px; }}
            ::-webkit-scrollbar-thumb {{ background: #4a5568 !important; border-radius: 6px; border: 2px solid #1e2433; min-height: 40px; min-width: 40px; }}
            ::-webkit-scrollbar-thumb:hover {{ background: #6b7280 !important; }}
            * {{ scrollbar-width: thick; scrollbar-color: #4a5568 #1e2433; }}
          `;
          function injectIntoFrame(frame) {{
            try {{
              const doc = frame.contentDocument;
              if (!doc || doc.querySelector('#kt-scroll-style')) return;
              const s = doc.createElement('style');
              s.id = 'kt-scroll-style';
              s.textContent = SCROLL_CSS;
              (doc.head || doc.documentElement).appendChild(s);
            }} catch(e) {{}}
          }}
          function injectAll() {{
            document.querySelectorAll('iframe').forEach(injectIntoFrame);
          }}
          injectAll();
          new MutationObserver(injectAll).observe(document.body, {{ childList: true, subtree: true }});
        }})();
        </script>
        <style>
        .kt-transfer-card {{
            background: linear-gradient(135deg, #0f2748 0%, #1d3f6f 100%);
            border: 1px solid var(--kt-primary);
            border-radius: 16px;
            padding: 1.25rem 1.35rem;
            margin-bottom: 1rem;
        }}
        .kt-transfer-card .kt-card-label {{ color: #93c5fd; }}
        @media (max-width: 760px) {{
            .block-container {{ padding-top: 1rem; }}
            .kt-hero h1 {{ font-size: 1.8rem; }}
        }}
        </style>
        <script>
        (function() {{
          const fix = el => {{
            if (el.dataset.kt_pw_fixed) return;
            el.dataset.kt_pw_fixed = '1';
            el.setAttribute('autocomplete', 'one-time-code');
            el.setAttribute('data-lpignore', 'true');
            el.setAttribute('readonly', 'readonly');
            const unlock = () => el.removeAttribute('readonly');
            el.addEventListener('focus', unlock, {{ once: true }});
            el.addEventListener('mousedown', unlock, {{ once: true }});
          }};
          const apply = () => document.querySelectorAll('input[type="password"]').forEach(fix);
          apply();
          new MutationObserver(apply).observe(document.body, {{ childList: true, subtree: true }});
        }})();
        </script>
    """, unsafe_allow_html=True)


# ── Login ─────────────────────────────────────────────────────────────────────

def render_login() -> None:
    inject_css()
    st.markdown("""
        <style>
        .block-container { display: flex; justify-content: center; }
        section[data-testid="stMain"] .block-container { max-width: 480px !important; padding-top: 8vh; }
        </style>
        <div style="text-align:center;margin-bottom:1.5rem;">
          <div class="kt-login-title">On-Call Logger</div>
          <div class="kt-login-sub">On-call log &amp; alert follow-up</div>
        </div>
    """, unsafe_allow_html=True)
    with st.form("login_form"):
        password = st.text_input("Password", type="password", placeholder="········")
        submitted = st.form_submit_button("Log in", use_container_width=True)
    if submitted:
        if password == get_app_password():
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password.")


# ── Metric card helper ────────────────────────────────────────────────────────

def metric(label: str, value: str, sub: str = "") -> None:
    st.markdown(f"""
        <div class="kt-metric">
          <div class="kt-metric-label">{label}</div>
          <div class="kt-metric-value">{value}</div>
          <div class="kt-metric-sub">{sub if sub else "&nbsp;"}</div>
        </div>
    """, unsafe_allow_html=True)


# ── Beredskap tab ─────────────────────────────────────────────────────────────

def render_beredskap_tab() -> None:
    members = load_members()
    member_names = [m["name"] for m in members]
    calls = load_calls()

    # ── Sidebar controls ──────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filter")
        filter_person = st.selectbox("Person", ["All"] + member_names, key="b_person")
        current_week = date.today().isocalendar()[1]
        current_year = date.today().year
        filter_year = st.selectbox(
            "Year",
            list(range(current_year, 2021, -1)),
            key="b_year",
        )
        MONTHS = ["All", "January", "February", "March", "April", "May", "June",
                  "July", "August", "September", "October", "November", "December"]
        current_month_name = date.today().strftime("%B")
        filter_month = st.selectbox(
            "Month",
            MONTHS,
            index=MONTHS.index(current_month_name),
            key="b_month",
        )

        st.divider()
        st.markdown("### Transfer to Comp")
        transfer_person = st.selectbox("Person", member_names, key="t_person") if member_names else None

        def week_label(w, y):
            try:
                thu_start = date.fromisocalendar(y, w, 4)
                thu_end   = thu_start + timedelta(weeks=1)
                return f"w.{w} — {thu_start.day}/{thu_start.month} → {thu_end.day}/{thu_end.month}/{thu_end.year}"
            except Exception:
                return f"w.{w} {y}"

        transfer_year = st.number_input("Year", min_value=2022, max_value=current_year + 1, value=current_year, key="t_year")
        transfer_week = st.number_input("Week", min_value=1, max_value=53, value=current_week, key="t_week")
        st.caption(week_label(transfer_week, transfer_year))

        DEDUCTION_MINS = 180

        if transfer_person:
            matching = [
                c for c in calls
                if c.get("chef") == transfer_person
                and c.get("ar") == transfer_year
                and c.get("vecka") == transfer_week
            ]
            total_mins = sum(c.get("tidsatgang_minutes") or 0 for c in matching)
            net_mins   = max(0, total_mins - DEDUCTION_MINS)

            st.markdown(f"""
                <div style="background:var(--kt-surface-soft);border:1px solid var(--kt-border);border-radius:12px;padding:0.85rem 1rem;font-size:0.9rem;line-height:2;">
                  <div style="display:flex;justify-content:space-between;"><span>Calls ({len(matching)})</span><span><b>{mins_to_hhmm(total_mins)}</b></span></div>
                  <div style="display:flex;justify-content:space-between;color:var(--kt-red)"><span>Deduction (3h)</span><span><b>−3:00</b></span></div>
                  <hr style="border-color:var(--kt-border);margin:0.4rem 0;">
                  <div style="display:flex;justify-content:space-between;color:var(--kt-green);font-size:1rem;"><span><b>Comp to save</b></span><span><b>{mins_to_hhmm(net_mins)}</b></span></div>
                </div>
            """, unsafe_allow_html=True)

            member_id = next((m["id"] for m in members if m["name"] == transfer_person), None)
            if st.button(
                f"Flytta {mins_to_hhmm(net_mins)} → Komp",
                type="primary",
                use_container_width=True,
                disabled=(net_mins == 0 or member_id is None),
            ):
                try:
                    transfer_week_to_comp(member_id, transfer_week, transfer_year, net_mins)
                    st.success(f"Added {mins_to_hhmm(net_mins)} for {transfer_person} in Comp!")
                except Exception as exc:
                    st.error(str(exc))

        st.divider()
        st.markdown("### Download")
        dl_person = st.selectbox("Person (export)", ["All"] + member_names, key="dl_b_person")
        dl_calls = calls if dl_person == "All" else [c for c in calls if c.get("chef") == dl_person]
        if st.download_button(
            "Download On-Call Log (.xlsx)",
            data=build_beredskap_excel(dl_calls),
            file_name=f"Uppföljning beredskap{' - ' + dl_person if dl_person != 'All' else ''}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        ):
            pass

    # ── Filter calls ──────────────────────────────────────────────────────────
    filtered = calls
    if filter_person != "All":
        filtered = [c for c in filtered if c.get("chef") == filter_person]
    if filter_year:
        filtered = [c for c in filtered if c.get("ar") == filter_year]
    if filter_month != "All":
        month_num = MONTHS.index(filter_month)
        filtered = [c for c in filtered if c.get("datum") and int(str(c.get("datum"))[5:7]) == month_num]

    # ── Metrics ───────────────────────────────────────────────────────────────
    total_mins = sum(c.get("tidsatgang_minutes") or 0 for c in filtered)
    c1, c2, c3 = st.columns(3)
    with c1:
        metric("Calls", str(len(filtered)))
    with c2:
        metric("Total time", mins_to_hhmm(total_mins))
    with c3:
        metric("All-time total", str(len(calls)), f"{mins_to_hhmm(sum(c.get('tidsatgang_minutes') or 0 for c in calls))} total")

    # ── Add call form ─────────────────────────────────────────────────────────
    with st.expander("+ Log new call", expanded=False):
        st.markdown('<div class="kt-card">', unsafe_allow_html=True)
        with st.form("add_call_form", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            with fc1:
                default_chef_idx = member_names.index(filter_person) if filter_person != "All" and filter_person in member_names else 0
                chef = st.selectbox("Chef / IM", member_names or ["—"], index=default_chef_idx, key="f_chef")
                kategori = st.selectbox("Category", KATEGORIER, key="f_kat")
                datum_val = st.date_input("Date", value=date.today(), key="f_datum")
            with fc2:
                tid_start = st.text_input("Time (start) HH:MM", placeholder="17:30", key="f_tid")
                tid_lost = st.text_input("Time resolved HH:MM", placeholder="18:00", key="f_lost")
                arende = st.text_input("Ticket / case", placeholder="LFINC000123", key="f_arende")

            beskrivning = st.text_area("Description", key="f_besk", height=100)
            kommentar = st.text_area("Comment", key="f_kom", height=100)
            forbattring = st.text_area("Improvement", key="f_forb", height=100)
            kontaktat_mod = st.selectbox("Contacted MOD", JA_NEJ, key="f_mod")

            submitted = st.form_submit_button("Save call", use_container_width=True)

        if submitted:
            t_start = parse_time_hhmm(tid_start)
            t_lost = parse_time_hhmm(tid_lost)
            mins = max(30, calc_minutes(t_start, t_lost) if t_start and t_lost else 30)

            row = {
                "chef": chef,
                "kategori": kategori,
                "datum": datum_val.isoformat(),
                "tid": t_start,
                "tid_lost": t_lost,
                "tidsatgang_minutes": mins,
                "arende": arende or None,
                "beskrivning": beskrivning or None,
                "kommentar": kommentar or None,
                "relevant": None,
                "forbattring": forbattring or None,
                "kontaktat_mod": None if kontaktat_mod == "—" else kontaktat_mod,
                **date_meta(datum_val),
            }
            try:
                add_call(row)
                st.success("Call saved!")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Call log table ────────────────────────────────────────────────────────
    st.markdown('<div class="kt-card">', unsafe_allow_html=True)
    month_label = filter_month if filter_month != "All" else "all months"
    person_label = filter_person if filter_person != "All" else "all"
    st.markdown(f'<div class="kt-card-label">Call log — {person_label}, {month_label} {filter_year}</div>', unsafe_allow_html=True)

    if not filtered:
        st.info("No calls for the selected filter.")
    else:
        df = pd.DataFrame(filtered)
        display_cols = {
            "datum": "Date",
            "chef": "Chef",
            "kategori": "Category",
            "tid": "Time",
            "tid_lost": "Time resolved",
            "tidsatgang_minutes": "Duration",
            "arende": "Ticket",
            "beskrivning": "Description",
            "kommentar": "Comment",
            "relevant": "Relevant",
            "kontaktat_mod": "Contacted MOD",
            "forbattring": "Improvement",
            "vecka": "Week",
        }
        show_cols = [c for c in display_cols if c in df.columns]
        df_show = df[show_cols].copy()
        if "datum" in df_show.columns:
            df_show["datum"] = df_show["datum"].apply(lambda x: format_date(str(x)) if pd.notna(x) else "")
        if "tidsatgang_minutes" in df_show.columns:
            df_show["tidsatgang_minutes"] = df_show["tidsatgang_minutes"].apply(
                lambda x: mins_to_hhmm(int(x)) if pd.notna(x) and x else "0:00"
            )
        df_show.rename(columns=display_cols, inplace=True)
        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            height=min(700, 38 + len(df_show) * 35),
            column_config={
                "Description": st.column_config.TextColumn(width="large"),
                "Comment":     st.column_config.TextColumn(width="large"),
                "Improvement": st.column_config.TextColumn(width="medium"),
                "Date":        st.column_config.TextColumn(width="small"),
                "Duration":    st.column_config.TextColumn(width="small"),
                "Category":    st.column_config.TextColumn(width="small"),
                "Time":        st.column_config.TextColumn(width="small"),
                "Time resolved": st.column_config.TextColumn(width="small"),
            },
        )

        # Detail view
        with st.expander("View details"):
            if filtered:
                id_opts_d = {f"#{r['id']} — {format_date(str(r.get('datum','')))} {r.get('chef','')} {(r.get('beskrivning') or '')[:50]}": r["id"] for r in filtered if r.get("id") is not None}
                chosen_d = st.selectbox("Select row", list(id_opts_d.keys()), key="detail_call_sel")
                rec_d = next((r for r in filtered if r.get("id") == id_opts_d[chosen_d]), {})
                if rec_d:
                    d1, d2 = st.columns(2)
                    with d1:
                        st.markdown(f"**Date:** {format_date(str(rec_d.get('datum','')))}")
                        st.markdown(f"**Chef:** {rec_d.get('chef','—')}")
                        st.markdown(f"**Category:** {rec_d.get('kategori','—')}")
                        st.markdown(f"**Time:** {rec_d.get('tid','—')} → {rec_d.get('tid_lost','—')}")
                        st.markdown(f"**Duration:** {mins_to_hhmm(rec_d.get('tidsatgang_minutes') or 0)}")
                        st.markdown(f"**Ticket:** {rec_d.get('arende','—')}")
                        st.markdown(f"**Contacted MOD:** {rec_d.get('kontaktat_mod','—')}")
                    with d2:
                        st.markdown("**Description:**")
                        st.info(rec_d.get("beskrivning") or "—")
                        st.markdown("**Comment:**")
                        st.info(rec_d.get("kommentar") or "—")
                        st.markdown("**Improvement:**")
                        st.info(rec_d.get("forbattring") or "—")

        # Edit row
        with st.expander("Edit row"):
            if filtered:
                id_opts = {f"#{r['id']} — {format_date(str(r.get('datum','')))} {r.get('chef','')} {(r.get('beskrivning') or '')[:40]}": r["id"] for r in filtered if r.get("id") is not None}
                chosen_label = st.selectbox("Select row to edit", list(id_opts.keys()), key="edit_call_sel")
                chosen_id = id_opts[chosen_label]
                rec = next((r for r in filtered if r.get("id") == chosen_id), {})

                with st.form("edit_call_form", clear_on_submit=False):
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        e_chef = st.selectbox("Chef / IM", member_names or ["—"], index=(member_names.index(rec.get("chef")) if rec.get("chef") in member_names else 0), key="e_chef")
                        e_kat  = st.selectbox("Category", KATEGORIER, index=(KATEGORIER.index(rec.get("kategori")) if rec.get("kategori") in KATEGORIER else 0), key="e_kat")
                        try:
                            e_datum = st.date_input("Date", value=date.fromisoformat(str(rec.get("datum", date.today()))[:10]), key="e_datum")
                        except Exception:
                            e_datum = st.date_input("Date", value=date.today(), key="e_datum")
                    with ec2:
                        e_tid   = st.text_input("Time (start) HH:MM", value=rec.get("tid") or "", key="e_tid")
                        e_lost  = st.text_input("Time resolved HH:MM", value=rec.get("tid_lost") or "", key="e_lost")
                        e_arende = st.text_input("Ticket / case", value=rec.get("arende") or "", key="e_arende")

                    e_besk = st.text_area("Description", value=rec.get("beskrivning") or "", key="e_besk", height=80)
                    e_kom  = st.text_area("Comment",     value=rec.get("kommentar") or "",  key="e_kom",  height=100)
                    e_forb = st.text_area("Improvement", value=rec.get("forbattring") or "", key="e_forb", height=100)
                    mod_idx = JA_NEJ.index(rec.get("kontaktat_mod")) if rec.get("kontaktat_mod") in JA_NEJ else 0
                    e_mod = st.selectbox("Contacted MOD", JA_NEJ, index=mod_idx, key="e_mod")

                    save = st.form_submit_button("Save changes", use_container_width=True)

                if save:
                    t_start = parse_time_hhmm(e_tid)
                    t_lost  = parse_time_hhmm(e_lost)
                    mins = max(30, calc_minutes(t_start, t_lost) if t_start and t_lost else (rec.get("tidsatgang_minutes") or 30))
                    update_call(chosen_id, {
                        "chef": e_chef, "kategori": e_kat,
                        "datum": e_datum.isoformat(),
                        "tid": t_start, "tid_lost": t_lost,
                        "tidsatgang_minutes": mins,
                        "arende": e_arende or None,
                        "beskrivning": e_besk or None,
                        "kommentar": e_kom or None,
                        "relevant": None,
                        "forbattring": e_forb or None,
                        "kontaktat_mod": None if e_mod == "—" else e_mod,
                        **date_meta(e_datum),
                    })
                    st.success("Row updated!")
                    st.rerun()

        # Delete row
        with st.expander("Delete row"):
            if filtered:
                id_opts = {f"#{r['id']} — {format_date(str(r.get('datum','')))} {r.get('chef','')} {(r.get('beskrivning') or '')[:40]}": r["id"] for r in filtered if r.get("id") is not None}
                chosen_label = st.selectbox("Select row to delete", list(id_opts.keys()), key="del_call")
                if st.button("Delete", key="del_call_btn"):
                    delete_call(id_opts[chosen_label])
                    st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)


def transfer_week_year_match(year):
    return year


# ── Larm tab ──────────────────────────────────────────────────────────────────

def render_larm_tab() -> None:
    members = load_members()
    member_names = [m["name"] for m in members]
    larm_list = load_larm()

    with st.sidebar:
        st.markdown("### Filter")
        filter_person = st.selectbox("Person", ["All"] + member_names, key="l_person")
        current_year = date.today().year
        filter_year = st.selectbox("Year", list(range(current_year, 2021, -1)), key="l_year")
        filter_atgard = st.selectbox("Action taken", ["All", "Yes", "No"], key="l_atgard")
        filter_uppfoljning = st.checkbox("Show only with follow-up", key="l_uppf")

        st.divider()
        st.markdown("### Download")
        dl_larm_person = st.selectbox("Person (export)", ["All"] + member_names, key="dl_l_person")
        dl_larm = larm_list if dl_larm_person == "All" else [l for l in larm_list if l.get("im") == dl_larm_person]
        if st.download_button(
            "Download Alert Log (.xlsx)",
            data=build_larm_excel(dl_larm),
            file_name=f"Uppföljning larm{' - ' + dl_larm_person if dl_larm_person != 'All' else ''}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        ):
            pass

    # Filter
    filtered = larm_list
    if filter_person != "All":
        filtered = [l for l in filtered if l.get("im") == filter_person]
    if filter_year:
        filtered = [l for l in filtered if l.get("ar") == filter_year]
    if filter_atgard != "All":
        filtered = [l for l in filtered if l.get("atgard_utford") == filter_atgard]
    if filter_uppfoljning:
        filtered = [l for l in filtered if l.get("uppfoljning")]

    # Metrics
    c1, c2, c3 = st.columns(3)
    with c1:
        metric("Alerts", str(len(filtered)))
    with c2:
        ja_count = sum(1 for l in filtered if l.get("atgard_utford") == "Yes")
        metric("Action taken", str(ja_count), f"of {len(filtered)}")
    with c3:
        metric("All-time total", str(len(larm_list)))

    # Add larm form
    with st.expander("+ Log new alert", expanded=False):
        st.markdown('<div class="kt-card">', unsafe_allow_html=True)
        with st.form("add_larm_form", clear_on_submit=True):
            lc1, lc2 = st.columns(2)
            with lc1:
                im = st.selectbox("IM", member_names or ["—"], key="l_im")
                datum_val = st.date_input("Date", value=date.today(), key="l_datum")
                tid_val = st.text_input("Time HH:MM", placeholder="03:28", key="l_tid")
            with lc2:
                inc_nr = st.text_input("Alert incident number", placeholder="LFINC000123", key="l_inc")
                dt_nr = st.text_input("Dynatrace number", placeholder="P-26021990", key="l_dt")

            beskrivning = st.text_area("Description", key="l_besk", height=80)
            kommentar = st.text_area("Comment", key="l_kom", height=80)

            ll1, ll2, ll3 = st.columns(3)
            with ll1:
                atgard = st.selectbox("Action taken", ["Yes", "No"], key="l_atg")
            with ll2:
                aterh = st.selectbox("Recovery/Improvement", ["Yes", "No"], key="l_ater")
            with ll3:
                larm_inst = st.selectbox("Alert instructions added", ["Yes", "No"], key="l_inst")

            uppfoljning = st.text_area("Follow-up", key="l_uppfoljning", height=68)
            submitted = st.form_submit_button("Save alert", use_container_width=True)

        if submitted:
            t = parse_time_hhmm(tid_val)
            row = {
                "im": im,
                "datum": datum_val.isoformat(),
                "tid": t,
                "larm_incidentnummer": inc_nr or None,
                "larm_dynatrace_nummer": dt_nr or None,
                "beskrivning": beskrivning or None,
                "kommentar": kommentar or None,
                "atgard_utford": atgard,
                "aterhamtning_forbattring": aterh,
                "larminstruktioner_tillagt": larm_inst,
                "uppfoljning": uppfoljning or None,
                **date_meta(datum_val),
            }
            try:
                add_larm(row)
                st.success("Alert saved!")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
        st.markdown('</div>', unsafe_allow_html=True)

    # Larm table
    st.markdown('<div class="kt-card">', unsafe_allow_html=True)
    person_label = filter_person if filter_person != "All" else "all"
    st.markdown(f'<div class="kt-card-label">Alert log — {person_label}, {filter_year}</div>', unsafe_allow_html=True)

    if not filtered:
        st.info("No alerts for the selected filter.")
    else:
        df = pd.DataFrame(filtered)
        display_cols = {
            "datum": "Date",
            "im": "IM",
            "tid": "Time",
            "larm_incidentnummer": "Incident nr",
            "larm_dynatrace_nummer": "Dynatrace nr",
            "beskrivning": "Description",
            "kommentar": "Comment",
            "atgard_utford": "Action taken",
            "aterhamtning_forbattring": "Recovery",
            "larminstruktioner_tillagt": "Alert instr.",
            "uppfoljning": "Follow-up",
            "vecka": "Week",
        }
        show_cols = [c for c in display_cols if c in df.columns]
        df_show = df[show_cols].copy()
        if "datum" in df_show.columns:
            df_show["datum"] = df_show["datum"].apply(lambda x: format_date(str(x)) if pd.notna(x) else "")
        df_show.rename(columns=display_cols, inplace=True)
        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            height=min(700, 38 + len(df_show) * 35),
            column_config={
                "Description": st.column_config.TextColumn(width="large"),
                "Comment":     st.column_config.TextColumn(width="large"),
                "Follow-up":   st.column_config.TextColumn(width="large"),
                "Date":        st.column_config.TextColumn(width="small"),
                "Time":        st.column_config.TextColumn(width="small"),
                "Action taken":  st.column_config.TextColumn(width="small"),
                "Recovery":      st.column_config.TextColumn(width="small"),
                "Alert instr.":  st.column_config.TextColumn(width="small"),
            },
        )

        # Detail view larm
        with st.expander("View details"):
            if filtered:
                id_opts_d = {f"#{r['id']} — {format_date(str(r.get('datum','')))} {r.get('im','')} {(r.get('beskrivning') or '')[:50]}": r["id"] for r in filtered if r.get("id") is not None}
                chosen_d = st.selectbox("Select row", list(id_opts_d.keys()), key="detail_larm_sel")
                rec_d = next((r for r in filtered if r.get("id") == id_opts_d[chosen_d]), {})
                if rec_d:
                    d1, d2 = st.columns(2)
                    with d1:
                        st.markdown(f"**Date:** {format_date(str(rec_d.get('datum','')))}")
                        st.markdown(f"**IM:** {rec_d.get('im','—')}")
                        st.markdown(f"**Time:** {rec_d.get('tid','—')}")
                        st.markdown(f"**Incident nr:** {rec_d.get('larm_incidentnummer','—')}")
                        st.markdown(f"**Dynatrace nr:** {rec_d.get('larm_dynatrace_nummer','—')}")
                        st.markdown(f"**Action taken:** {rec_d.get('atgard_utford','—')}")
                        st.markdown(f"**Recovery:** {rec_d.get('aterhamtning_forbattring','—')}")
                        st.markdown(f"**Alert instr. added:** {rec_d.get('larminstruktioner_tillagt','—')}")
                    with d2:
                        st.markdown("**Description:**")
                        st.info(rec_d.get("beskrivning") or "—")
                        st.markdown("**Comment:**")
                        st.info(rec_d.get("kommentar") or "—")
                        st.markdown("**Follow-up:**")
                        st.info(rec_d.get("uppfoljning") or "—")

        # Edit larm row
        with st.expander("Edit row"):
            if filtered:
                id_opts = {
                    f"#{r['id']} — {format_date(str(r.get('datum','')))} {r.get('im','')} {(r.get('beskrivning') or '')[:40]}": r["id"]
                    for r in filtered if r.get("id") is not None
                }
                chosen_label = st.selectbox("Select row to edit", list(id_opts.keys()), key="edit_larm_sel")
                chosen_id = id_opts[chosen_label]
                rec = next((r for r in filtered if r.get("id") == chosen_id), {})

                with st.form("edit_larm_form", clear_on_submit=False):
                    el1, el2 = st.columns(2)
                    with el1:
                        e_im = st.selectbox("IM", member_names or ["—"], index=(member_names.index(rec.get("im")) if rec.get("im") in member_names else 0), key="el_im")
                        try:
                            e_datum = st.date_input("Date", value=date.fromisoformat(str(rec.get("datum", date.today()))[:10]), key="el_datum")
                        except Exception:
                            e_datum = st.date_input("Date", value=date.today(), key="el_datum")
                        e_tid = st.text_input("Time HH:MM", value=rec.get("tid") or "", key="el_tid")
                    with el2:
                        e_inc = st.text_input("Alert incident number", value=rec.get("larm_incidentnummer") or "", key="el_inc")
                        e_dt  = st.text_input("Dynatrace number",      value=rec.get("larm_dynatrace_nummer") or "", key="el_dt")

                    e_besk = st.text_area("Description", value=rec.get("beskrivning") or "", key="el_besk", height=80)
                    e_kom  = st.text_area("Comment",     value=rec.get("kommentar") or "",   key="el_kom",  height=68)

                    ll1, ll2, ll3 = st.columns(3)
                    yes_no = ["Yes", "No"]
                    with ll1:
                        e_atg  = st.selectbox("Action taken", yes_no, index=(0 if rec.get("atgard_utford") == "Yes" else 1), key="el_atg")
                    with ll2:
                        e_ater = st.selectbox("Recovery/Improvement", yes_no, index=(0 if rec.get("aterhamtning_forbattring") == "Yes" else 1), key="el_ater")
                    with ll3:
                        e_inst = st.selectbox("Alert instructions added", yes_no, index=(0 if rec.get("larminstruktioner_tillagt") == "Yes" else 1), key="el_inst")

                    e_uppf = st.text_area("Follow-up", value=rec.get("uppfoljning") or "", key="el_uppf", height=68)
                    save = st.form_submit_button("Save changes", use_container_width=True)

                if save:
                    update_larm(chosen_id, {
                        "im": e_im,
                        "datum": e_datum.isoformat(),
                        "tid": parse_time_hhmm(e_tid),
                        "larm_incidentnummer": e_inc or None,
                        "larm_dynatrace_nummer": e_dt or None,
                        "beskrivning": e_besk or None,
                        "kommentar": e_kom or None,
                        "atgard_utford": e_atg,
                        "aterhamtning_forbattring": e_ater,
                        "larminstruktioner_tillagt": e_inst,
                        "uppfoljning": e_uppf or None,
                        **date_meta(e_datum),
                    })
                    st.success("Row updated!")
                    st.rerun()

        with st.expander("Delete row"):
            if filtered:
                id_opts = {
                    f"#{r['id']} — {format_date(str(r.get('datum','')))} {r.get('im','')} {(r.get('beskrivning') or '')[:40]}": r["id"]
                    for r in filtered if r.get("id") is not None
                }
                chosen_label = st.selectbox("Select row to delete", list(id_opts.keys()), key="del_larm")
                if st.button("Delete", key="del_larm_btn"):
                    delete_larm(id_opts[chosen_label])
                    st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)


# ── Members tab ───────────────────────────────────────────────────────────────

def render_members_tab() -> None:
    all_members = load_all_members()
    active = [m for m in all_members if not m.get("is_archived")]
    archived = [m for m in all_members if m.get("is_archived")]
    active_names = [m["name"] for m in active]

    with st.container():
        st.markdown('<div class="kt-card-label">Add member</div>', unsafe_allow_html=True)
        c1, c2 = st.columns([4, 1])
        with c1:
            new_name = st.text_input("Name", placeholder="Full name", key="new_member_name", label_visibility="collapsed")
        with c2:
            if st.button("Add", use_container_width=True, key="add_member_btn"):
                if not new_name.strip():
                    st.warning("Enter a name.")
                elif new_name.strip() in active_names:
                    st.warning("Already exists.")
                else:
                    add_member(new_name.strip())
                    st.success(f"{new_name.strip()} added!")
                    st.rerun()

    st.markdown('<div class="kt-card">', unsafe_allow_html=True)
    st.markdown('<div class="kt-card-label">Active members</div>', unsafe_allow_html=True)
    if not active:
        st.info("No active members.")
    else:
        for m in active:
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"**{m['name']}**")
            with col2:
                if st.button("Archive", key=f"arch_{m['id']}"):
                    archive_member(m["id"])
                    st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    if archived:
        st.markdown('<div class="kt-card">', unsafe_allow_html=True)
        st.markdown('<div class="kt-card-label">Archived members</div>', unsafe_allow_html=True)
        for m in archived:
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"~~{m['name']}~~")
            with col2:
                if st.button("Restore", key=f"rest_{m['id']}"):
                    restore_member(m["id"])
                    st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not using_supabase():
        init_sqlite()

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        render_login()
        return

    inject_css()

    st.markdown("""
        <div class="kt-hero">
          <h1>On-Call Logger</h1>
          <p>On-call log &amp; alert follow-up — Incident Managers</p>
        </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("## On-Call Logger")
        if st.button("Log out", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()
        comp_url = get_setting("COMP_URL", "https://4ig74m8abezhu4ighxtxwe.streamlit.app/")
        st.link_button("Open Comp Portal", comp_url, use_container_width=True)
        st.divider()

    tab = st.segmented_control(
        "Navigation",
        ["📞 Beredskap", "🚨 Larm", "👥 Members"],
        default="📞 Beredskap",
        key="main_tab",
    )

    if tab == "📞 Beredskap":
        render_beredskap_tab()
    elif tab == "🚨 Larm":
        render_larm_tab()
    else:
        render_members_tab()


main()
