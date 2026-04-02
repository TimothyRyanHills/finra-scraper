"""Phase 1: Collect firm names and details from BrokerCheck API.

Strategy: enumerate all ACTIVE firms via 2-letter prefix queries to stay
under the API's 9000-result pagination cap. Deduplicates by CRD number.
The 'filter' param on FINRA's API is broken as of 2026-04, so we filter
client-side for firm_scope == "ACTIVE".
"""

import json
import logging
import string
import time
from datetime import datetime

import db
from config import DEFAULT_DELAY
from models import FirmListing, FirmDetail
from scraper.brokercheck_client import BrokerCheckClient

logger = logging.getLogger(__name__)

# Pagination cap enforced by BrokerCheck API
API_MAX_OFFSET = 9000
PAGE_SIZE = 100

# Characters to use for prefix generation
CHARS = list(string.ascii_lowercase) + [str(d) for d in range(10)]


def run_api_enumeration(conn, delay: float = 0.5) -> int:
    """Enumerate all active firms via BrokerCheck search API.

    Uses 2-character prefix queries (aa, ab, ..., zz, 0-9 combos) to
    paginate through all firms. Deduplicates by CRD and filters for
    active firms client-side.

    Returns total number of unique active firms found.
    """
    client = BrokerCheckClient(delay=delay)
    seen_crds = _get_existing_crds(conn)
    initial_count = len(seen_crds)
    logger.info("Starting enumeration. %d firms already in DB.", initial_count)

    # Generate all 2-character prefixes: a-z + 0-9 × a-z + 0-9 + space
    prefixes = []
    for first in CHARS:
        for second in CHARS + [' ']:
            prefixes.append(first + second)

    total_prefixes = len(prefixes)
    skipped = 0
    logger.info("Will query %d 2-char prefixes", total_prefixes)

    for i, prefix in enumerate(prefixes):
        total, new = _enumerate_prefix(client, conn, prefix, seen_crds)
        if new > 0:
            logger.info(
                "[%d/%d] Prefix '%s': %d total, %d new active (cumulative: %d)",
                i + 1, total_prefixes, prefix, total, new, len(seen_crds),
            )
        else:
            skipped += 1

        # Progress report every 100 prefixes
        if (i + 1) % 100 == 0:
            logger.info(
                "Progress: %d/%d prefixes done, %d active firms found so far",
                i + 1, total_prefixes, len(seen_crds),
            )

    client.close()
    new_total = len(seen_crds) - initial_count
    logger.info(
        "Enumeration complete. %d new firms (%d total). %d prefixes returned no new results.",
        new_total, len(seen_crds), skipped,
    )
    return len(seen_crds)


def _get_existing_crds(conn) -> set[int]:
    """Get CRD numbers already in the database."""
    rows = conn.execute("SELECT crd_number FROM firm_details").fetchall()
    return {row["crd_number"] for row in rows}


def _enumerate_prefix(client, conn, prefix: str, seen_crds: set[int]) -> tuple[int, int]:
    """Paginate through all results for a given query prefix.

    Returns (total_results, new_firms_added).
    """
    start = 0
    total = None
    new_count = 0
    consecutive_empty_pages = 0

    while True:
        try:
            data = client.search_firm_paginated(prefix, start=start, count=PAGE_SIZE)
        except Exception as e:
            logger.error("API error for prefix '%s' at start=%d: %s", prefix, start, e)
            break

        hits_data = data.get("hits")
        if not hits_data or not hits_data.get("hits"):
            break

        if total is None:
            total = hits_data.get("total", 0)
            # Skip prefixes with 0 results
            if total == 0:
                break

        hits = hits_data["hits"]
        page_new = 0
        for hit in hits:
            crd, added = _save_hit(conn, hit, seen_crds)
            if added:
                page_new += 1
                new_count += 1

        conn.commit()

        # Early termination: if 3 consecutive pages had no new active firms,
        # likely all remaining results for this prefix are inactive/dupes
        if page_new == 0:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= 3:
                break
        else:
            consecutive_empty_pages = 0

        # Check if we've gotten all results or hit the API cap
        if start + PAGE_SIZE >= min(total or 0, API_MAX_OFFSET):
            break

        start += PAGE_SIZE

    return total or 0, new_count


def _save_hit(conn, hit: dict, seen_crds: set[int]) -> tuple[int | None, bool]:
    """Parse and save a single search hit. Returns (crd_number, was_new)."""
    source = hit.get("_source", hit)

    # Extract CRD
    crd_raw = source.get("firm_source_id") or source.get("bc_source_id") or source.get("sourceId")
    if crd_raw is None:
        return None, False

    crd = int(crd_raw)
    if crd in seen_crds:
        return crd, False

    # Filter: only keep active firms
    scope = (source.get("firm_scope") or "").upper()
    if scope != "ACTIVE":
        return crd, False

    seen_crds.add(crd)

    name = (
        source.get("firm_name")
        or source.get("bc_source_name")
        or source.get("name")
        or ""
    ).strip()
    if not name:
        return crd, False

    # Parse address from firm_address_details (JSON string)
    street, city, state, zip_code, country = "", "", "", "", ""
    addr_json = source.get("firm_address_details")
    if addr_json:
        try:
            addr_data = json.loads(addr_json) if isinstance(addr_json, str) else addr_json
            office = addr_data.get("officeAddress", {})
            street = office.get("street1", "")
            city = office.get("city", "")
            state = office.get("state", "")
            zip_code = office.get("postalCode", "")
            country = office.get("country", "")
        except (json.JSONDecodeError, AttributeError):
            pass

    address_parts = [p for p in [street, city, state, zip_code] if p]
    address = ", ".join(address_parts)

    # Save listing
    listing = FirmListing(
        name=name,
        address=address,
        source_page="api_enumeration",
        scraped_at=datetime.utcnow(),
    )
    db.upsert_listing(conn, listing)

    # Parse other names
    other_names_list = source.get("firm_other_names", source.get("otherNames", []))
    other_names = "|".join(other_names_list) if isinstance(other_names_list, list) else other_names_list

    # Save detail (Phase 2 data — we already have CRD from search)
    detail = FirmDetail(
        crd_number=crd,
        sec_number=source.get("firm_bd_sec_number") or source.get("bc_sec_number"),
        name=name,
        other_names=other_names,
        finra_approved_registration_count=source.get("firm_approved_finra_registration_count"),
        number_of_branches=source.get("firm_branches_count"),
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
        country=country,
        search_response_raw=json.dumps(source),
        matched_from_name=name,
        match_confidence=1.0,
        scraped_at=datetime.utcnow(),
    )
    db.upsert_detail(conn, detail, phase=2)
    return crd, True


# Legacy fallback strategies kept for compatibility

def run_playwright_scrape(conn, letter=None):
    raise NotImplementedError("Use --try-api-enumeration instead")

def run_requests_scrape(conn, letter=None):
    raise NotImplementedError("Use --try-api-enumeration instead")
