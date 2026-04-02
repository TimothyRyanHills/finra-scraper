"""SQLite database layer for checkpoint/resume and data storage."""

import json
import os
import sqlite3
from datetime import datetime
from typing import Optional

from config import DB_PATH, DATA_DIR
from models import FirmDetail, FirmListing

_SCHEMA = """
CREATE TABLE IF NOT EXISTS firm_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    mailing_address TEXT,
    source_page TEXT NOT NULL,
    scraped_at TEXT NOT NULL,
    UNIQUE(name, address)
);

CREATE TABLE IF NOT EXISTS firm_details (
    crd_number INTEGER PRIMARY KEY,
    sec_number TEXT,
    name TEXT NOT NULL,
    other_names TEXT,
    finra_approved_registration_count INTEGER,
    number_of_branches INTEGER,
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT,
    latitude REAL,
    longitude REAL,
    registration_status TEXT,
    registration_begin_date TEXT,
    firm_size TEXT,
    disclosures_count INTEGER,
    branch_locations_count INTEGER,
    phone TEXT,
    firm_type TEXT,
    formed_state TEXT,
    formed_date TEXT,
    search_response_raw TEXT,
    detail_response_raw TEXT,
    matched_from_name TEXT,
    match_confidence REAL,
    phase INTEGER NOT NULL DEFAULT 2,
    scraped_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    url TEXT NOT NULL,
    status_code INTEGER,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_firm_listings_source ON firm_listings(source_page);
CREATE INDEX IF NOT EXISTS idx_firm_details_phase ON firm_details(phase);
CREATE INDEX IF NOT EXISTS idx_firm_details_name ON firm_details(matched_from_name);
"""

# Migration: add columns if they don't exist (for existing DBs)
_MIGRATIONS = [
    "ALTER TABLE firm_details ADD COLUMN phone TEXT",
    "ALTER TABLE firm_details ADD COLUMN firm_type TEXT",
    "ALTER TABLE firm_details ADD COLUMN formed_state TEXT",
    "ALTER TABLE firm_details ADD COLUMN formed_date TEXT",
]


def get_or_create_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure tables exist."""
    path = path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    # Run migrations (ignore if columns already exist)
    for migration in _MIGRATIONS:
        try:
            conn.execute(migration)
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
    return conn


def upsert_listing(conn: sqlite3.Connection, listing: FirmListing) -> None:
    """Insert or update a firm listing."""
    conn.execute(
        """INSERT INTO firm_listings (name, address, mailing_address, source_page, scraped_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(name, address) DO UPDATE SET
               mailing_address = excluded.mailing_address,
               source_page = excluded.source_page,
               scraped_at = excluded.scraped_at""",
        (
            listing.name,
            listing.address,
            listing.mailing_address,
            listing.source_page,
            listing.scraped_at.isoformat(),
        ),
    )


def upsert_detail(conn: sqlite3.Connection, detail: FirmDetail, phase: int) -> None:
    """Insert or update a firm detail record."""
    conn.execute(
        """INSERT INTO firm_details (
               crd_number, sec_number, name, other_names,
               finra_approved_registration_count, number_of_branches,
               street, city, state, zip_code, country,
               latitude, longitude,
               registration_status, registration_begin_date,
               firm_size, disclosures_count, branch_locations_count,
               phone, firm_type, formed_state, formed_date,
               search_response_raw, detail_response_raw,
               matched_from_name, match_confidence,
               phase, scraped_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(crd_number) DO UPDATE SET
               sec_number = COALESCE(excluded.sec_number, sec_number),
               name = excluded.name,
               other_names = COALESCE(excluded.other_names, other_names),
               finra_approved_registration_count = COALESCE(excluded.finra_approved_registration_count, finra_approved_registration_count),
               number_of_branches = COALESCE(excluded.number_of_branches, number_of_branches),
               street = COALESCE(excluded.street, street),
               city = COALESCE(excluded.city, city),
               state = COALESCE(excluded.state, state),
               zip_code = COALESCE(excluded.zip_code, zip_code),
               country = COALESCE(excluded.country, country),
               latitude = COALESCE(excluded.latitude, latitude),
               longitude = COALESCE(excluded.longitude, longitude),
               registration_status = COALESCE(excluded.registration_status, registration_status),
               registration_begin_date = COALESCE(excluded.registration_begin_date, registration_begin_date),
               firm_size = COALESCE(excluded.firm_size, firm_size),
               disclosures_count = COALESCE(excluded.disclosures_count, disclosures_count),
               branch_locations_count = COALESCE(excluded.branch_locations_count, branch_locations_count),
               phone = COALESCE(excluded.phone, phone),
               firm_type = COALESCE(excluded.firm_type, firm_type),
               formed_state = COALESCE(excluded.formed_state, formed_state),
               formed_date = COALESCE(excluded.formed_date, formed_date),
               search_response_raw = COALESCE(excluded.search_response_raw, search_response_raw),
               detail_response_raw = COALESCE(excluded.detail_response_raw, detail_response_raw),
               matched_from_name = COALESCE(excluded.matched_from_name, matched_from_name),
               match_confidence = COALESCE(excluded.match_confidence, match_confidence),
               phase = MAX(excluded.phase, phase),
               scraped_at = excluded.scraped_at""",
        (
            detail.crd_number,
            detail.sec_number,
            detail.name,
            detail.other_names,
            detail.finra_approved_registration_count,
            detail.number_of_branches,
            detail.street,
            detail.city,
            detail.state,
            detail.zip_code,
            detail.country,
            detail.latitude,
            detail.longitude,
            detail.registration_status,
            detail.registration_begin_date,
            detail.firm_size,
            detail.disclosures_count,
            detail.branch_locations_count,
            detail.phone,
            detail.firm_type,
            detail.formed_state,
            detail.formed_date,
            detail.search_response_raw,
            detail.detail_response_raw,
            detail.matched_from_name,
            detail.match_confidence,
            phase,
            detail.scraped_at.isoformat(),
        ),
    )


def get_completed_pages(conn: sqlite3.Connection) -> set[str]:
    """Return set of source_page values already scraped in Phase 1."""
    rows = conn.execute("SELECT DISTINCT source_page FROM firm_listings").fetchall()
    return {row["source_page"] for row in rows}


def get_all_listing_names(conn: sqlite3.Connection) -> list[str]:
    """Return all firm names from Phase 1 listings."""
    rows = conn.execute("SELECT DISTINCT name FROM firm_listings ORDER BY name").fetchall()
    return [row["name"] for row in rows]


def get_unmatched_listings(conn: sqlite3.Connection) -> list[str]:
    """Return firm names from Phase 1 that have no CRD match yet."""
    rows = conn.execute(
        """SELECT DISTINCT fl.name
           FROM firm_listings fl
           LEFT JOIN firm_details fd ON fd.matched_from_name = fl.name
           WHERE fd.crd_number IS NULL
           ORDER BY fl.name"""
    ).fetchall()
    return [row["name"] for row in rows]


def get_crd_numbers_needing_detail(conn: sqlite3.Connection) -> list[int]:
    """Return CRD numbers at phase=2 (need Phase 3 enrichment)."""
    rows = conn.execute(
        "SELECT crd_number FROM firm_details WHERE phase = 2 ORDER BY crd_number"
    ).fetchall()
    return [row["crd_number"] for row in rows]


def get_all_details(conn: sqlite3.Connection, phase: Optional[int] = None) -> list[sqlite3.Row]:
    """Return all firm detail rows, optionally filtered by phase."""
    if phase is not None:
        return conn.execute(
            "SELECT * FROM firm_details WHERE phase >= ? ORDER BY name", (phase,)
        ).fetchall()
    return conn.execute("SELECT * FROM firm_details ORDER BY name").fetchall()


def log_request(
    conn: sqlite3.Connection,
    url: str,
    status_code: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    """Log an HTTP request for auditing."""
    conn.execute(
        "INSERT INTO scrape_log (timestamp, url, status_code, error) VALUES (?, ?, ?, ?)",
        (datetime.utcnow().isoformat(), url, status_code, error),
    )


def get_stats(conn: sqlite3.Connection) -> dict:
    """Return summary statistics."""
    listings = conn.execute("SELECT COUNT(*) as c FROM firm_listings").fetchone()["c"]
    details_p2 = conn.execute("SELECT COUNT(*) as c FROM firm_details WHERE phase >= 2").fetchone()["c"]
    details_p3 = conn.execute("SELECT COUNT(*) as c FROM firm_details WHERE phase >= 3").fetchone()["c"]
    unmatched = len(get_unmatched_listings(conn))
    return {
        "firm_listings": listings,
        "crd_matched": details_p2,
        "detail_enriched": details_p3,
        "unmatched": unmatched,
    }
