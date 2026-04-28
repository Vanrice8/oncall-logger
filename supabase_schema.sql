-- Run this in your Supabase SQL editor to create the on-call portal tables.
-- Uses the same Supabase project as the Comp portal.

CREATE TABLE IF NOT EXISTS oncall_calls (
    id          BIGSERIAL PRIMARY KEY,
    chef        TEXT NOT NULL,
    kategori    TEXT,
    datum       DATE NOT NULL,
    tid         TEXT,                   -- start time "HH:MM"
    tid_lost    TEXT,                   -- end time "HH:MM"
    tidsatgang_minutes INTEGER DEFAULT 0,
    arende      TEXT,
    beskrivning TEXT,
    kommentar   TEXT,
    relevant    TEXT,                   -- "Ja" / "Nej" / null
    forbattring TEXT,
    kontaktat_mod TEXT,                 -- "Ja" / "Nej" / null
    vecka       INTEGER,
    manad       TEXT,
    ar          INTEGER,
    kvartal     INTEGER,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oncall_calls_datum ON oncall_calls (datum DESC);
CREATE INDEX IF NOT EXISTS idx_oncall_calls_chef  ON oncall_calls (chef);
CREATE INDEX IF NOT EXISTS idx_oncall_calls_vecka ON oncall_calls (ar, vecka);

CREATE TABLE IF NOT EXISTS oncall_larm (
    id                          BIGSERIAL PRIMARY KEY,
    im                          TEXT NOT NULL,
    datum                       DATE NOT NULL,
    tid                         TEXT,
    larm_incidentnummer         TEXT,
    larm_dynatrace_nummer       TEXT,
    beskrivning                 TEXT,
    kommentar                   TEXT,
    atgard_utford               TEXT,   -- "Ja" / "Nej"
    aterhamtning_forbattring    TEXT,   -- "Ja" / "Nej"
    vecka                       INTEGER,
    manad                       TEXT,
    ar                          INTEGER,
    kvartal                     INTEGER,
    larminstruktioner_tillagt   TEXT,   -- "Ja" / "Nej"
    uppfoljning                 TEXT,
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oncall_larm_datum ON oncall_larm (datum DESC);
CREATE INDEX IF NOT EXISTS idx_oncall_larm_im    ON oncall_larm (im);
