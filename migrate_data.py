"""
Import historical data from the two Excel files into Supabase (or SQLite).

Usage:
    python migrate_data.py

Set SUPABASE_URL and SUPABASE_KEY env vars (or in .env) before running.
Reads:
  - Uppföljning beredskap (1).xlsx  →  oncall_calls
  - Uppföljning larm (1).xlsx       →  oncall_larm
"""

import json
import os
import sqlite3
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path
from urllib import error, parse, request as urllib_request

import pandas as pd

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

BEREDSKAP_FILE = Path(r"C:\Users\dima_\Downloads\Uppföljning beredskap (1).xlsx")
LARM_FILE      = Path(r"C:\Users\dima_\Downloads\Uppföljning larm (1).xlsx")
SQLITE_DB      = Path(__file__).parent / "oncall.db"

BATCH_SIZE = 200


# ── Supabase ──────────────────────────────────────────────────────────────────

def sb_request(method, path, *, params=None, body=None, prefer=None):
    query = f"?{parse.urlencode(params)}" if params else ""
    url = f"{SUPABASE_URL}/rest/v1/{path}{query}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    payload = None if body is None else json.dumps(body).encode()
    req = urllib_request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib_request.urlopen(req) as r:
            raw = r.read()
            return json.loads(raw.decode()) if raw else None
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase {exc.code}: {detail}") from exc


def sb_insert_batch(table, rows):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sb_request("POST", table, body=batch, prefer="return=minimal")
        print(f"  → Inserted rows {i+1}–{min(i+BATCH_SIZE, len(rows))}")


# ── SQLite ────────────────────────────────────────────────────────────────────

def sqlite_insert_batch(table, rows):
    conn = sqlite3.connect(SQLITE_DB, check_same_thread=False)
    if not rows:
        return
    cols = list(rows[0].keys())
    conn.executemany(
        f"INSERT OR IGNORE INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
        [[r.get(c) for c in cols] for r in rows],
    )
    conn.commit()
    conn.close()


# ── Time helpers ──────────────────────────────────────────────────────────────

def td_to_minutes(val) -> int:
    """Convert pandas Timedelta / datetime / string to integer minutes."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0
    try:
        td = pd.to_timedelta(val)
        return max(0, int(td.total_seconds() / 60))
    except Exception:
        pass
    return 0


def val_to_hhmm(val) -> str | None:
    """Convert Excel time value to 'HH:MM' string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        td = pd.to_timedelta(val)
        total_sec = int(td.total_seconds())
        h = (total_sec // 3600) % 24
        m = (total_sec % 3600) // 60
        return f"{h:02d}:{m:02d}"
    except Exception:
        pass
    if hasattr(val, "hour"):
        return f"{val.hour:02d}:{val.minute:02d}"
    s = str(val).strip()
    if ":" in s:
        parts = s.split(":")
        try:
            return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
        except Exception:
            pass
    return None


def safe_date(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        d = pd.to_datetime(val)
        return d.date().isoformat()
    except Exception:
        return None


def safe_int(val) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except Exception:
        return None


def safe_str(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def week_of(d: date) -> int:
    return d.isocalendar()[1]


# ── Beredskap migration ───────────────────────────────────────────────────────

def migrate_beredskap():
    print(f"\nReading {BEREDSKAP_FILE.name}…")
    df = pd.read_excel(BEREDSKAP_FILE, sheet_name="Underlag")
    print(f"  {len(df)} rows found.")

    rows = []
    skipped = 0

    for _, r in df.iterrows():
        datum_str = safe_date(r.get("Datum"))
        if not datum_str:
            skipped += 1
            continue

        try:
            d = date.fromisoformat(datum_str)
        except Exception:
            skipped += 1
            continue

        chef = safe_str(r.get("Chef"))
        if not chef:
            skipped += 1
            continue

        tid_str  = val_to_hhmm(r.get("Tid"))
        lost_str = val_to_hhmm(r.get("Tid löst"))
        mins     = td_to_minutes(r.get("Tidsåtgång"))

        # Fallback: calculate from start/end if tidsåtgång missing
        if mins == 0 and tid_str and lost_str:
            sh, sm = map(int, tid_str.split(":"))
            eh, em = map(int, lost_str.split(":"))
            diff = (eh * 60 + em) - (sh * 60 + sm)
            mins = diff if diff >= 0 else diff + 24 * 60

        kat   = safe_str(r.get("Kategori"))
        arende = safe_str(r.get("Ärende"))
        besk  = safe_str(r.get("Beskrivning"))
        kom   = safe_str(r.get("Kommentar"))
        rel   = safe_str(r.get("Relevant"))
        forb  = safe_str(r.get("Förbättring"))
        mod   = safe_str(r.get("Kontaktat MOD"))

        rows.append({
            "chef":               chef,
            "kategori":           kat,
            "datum":              datum_str,
            "tid":                tid_str,
            "tid_lost":           lost_str,
            "tidsatgang_minutes": mins,
            "arende":             arende,
            "beskrivning":        besk,
            "kommentar":          kom,
            "relevant":           rel,
            "forbattring":        forb,
            "kontaktat_mod":      mod,
            "vecka":              week_of(d),
            "manad":              d.strftime("%B"),
            "ar":                 d.year,
            "kvartal":            quarter(d.month),
        })

    print(f"  Prepared {len(rows)} rows ({skipped} skipped — no date or chef).")

    if not rows:
        print("  Nothing to insert.")
        return

    if USE_SUPABASE:
        print("  Inserting into Supabase oncall_calls…")
        sb_insert_batch("oncall_calls", rows)
    else:
        print("  Inserting into SQLite oncall_calls…")
        sqlite_insert_batch("oncall_calls", rows)

    print(f"  Done. {len(rows)} beredskap rows imported.")


# ── Larm migration ────────────────────────────────────────────────────────────

def migrate_larm():
    print(f"\nReading {LARM_FILE.name}…")
    df = pd.read_excel(LARM_FILE, sheet_name="Larm")
    print(f"  {len(df)} rows found.")

    rows = []
    skipped = 0

    for _, r in df.iterrows():
        datum_str = safe_date(r.get("Datum"))
        if not datum_str:
            skipped += 1
            continue

        try:
            d = date.fromisoformat(datum_str)
        except Exception:
            skipped += 1
            continue

        im = safe_str(r.get("IM"))
        if not im:
            skipped += 1
            continue

        # Skip the example/template row
        if safe_str(r.get("Larm incidentnummer")) == "EXEMPEL":
            skipped += 1
            continue

        tid_str = val_to_hhmm(r.get("Tid"))

        rows.append({
            "im":                       im,
            "datum":                    datum_str,
            "tid":                      tid_str,
            "larm_incidentnummer":      safe_str(r.get("Larm incidentnummer")),
            "larm_dynatrace_nummer":    safe_str(r.get("Larm Dynatrace nummer")),
            "beskrivning":              safe_str(r.get("Beskrivning")),
            "kommentar":                safe_str(r.get("Kommentar")),
            "atgard_utford":            safe_str(r.get("Åtgärd utförd")),
            "aterhamtning_forbattring": safe_str(r.get("Återhämtning/Förbättring")),
            "vecka":                    safe_int(r.get("Vecka")),
            "manad":                    safe_str(r.get("Månad")),
            "ar":                       safe_int(r.get("År")),
            "kvartal":                  safe_int(r.get("Kvartal")),
            "larminstruktioner_tillagt":safe_str(r.get("Larminstruktioner tillagt")),
            "uppfoljning":              safe_str(r.get("Uppföljning ")),
        })

    print(f"  Prepared {len(rows)} rows ({skipped} skipped).")

    if not rows:
        print("  Nothing to insert.")
        return

    if USE_SUPABASE:
        print("  Inserting into Supabase oncall_larm…")
        sb_insert_batch("oncall_larm", rows)
    else:
        print("  Inserting into SQLite oncall_larm…")
        sqlite_insert_batch("oncall_larm", rows)

    print(f"  Done. {len(rows)} larm rows imported.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if USE_SUPABASE:
        print(f"Using Supabase: {SUPABASE_URL[:40]}…")
    else:
        print(f"Supabase not configured — falling back to SQLite at {SQLITE_DB}")
        print("(Set SUPABASE_URL and SUPABASE_KEY to target Supabase instead.)")

    try:
        migrate_beredskap()
        migrate_larm()
        print("\nAll done!")
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
