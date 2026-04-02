"""Phase 1: Collect firm names and addresses from FINRA directory pages.

Supports two strategies:
  1. API enumeration via BrokerCheck (preferred -- no browser needed)
  2. Playwright-based scraping of the alphabetical HTML pages (fallback)
"""

import json
import logging
import re
import time
from datetime import datetime

from bs4 import BeautifulSoup

import db
from config import (
    FINRA_BASE_URL,
    FINRA_LETTER_PAGES,
    PLAYWRIGHT_DELAY,
    BROWSER_HEADERS,
)
from models import FirmListing, FirmDetail
from scraper.brokercheck_client import BrokerCheckClient

logger = logging.getLogger(__name__)

# ---------- Strategy 1: BrokerCheck API enumeration ----------


def run_api_enumeration(conn, delay: float = 0.5) -> int:
    """Enumerate all firms directly from the BrokerCheck search API.

    This bypasses the FINRA HTML pages entirely. Each search hit gives us
    both the firm listing data AND the CRD number, so we populate both
    firm_listings and firm_details in one pass.

    Returns the total number of firms found.
    """
    client = BrokerCheckClient(delay=delay)
    total_found = 0
    start = 0
    page_size = 100

    logger.info("Starting BrokerCheck API enumeration (page_size=%d)", page_size)

    while True:
        try:
            data = client.search_firm_all(start=start, count=page_size)
        except Exception as e:
            logger.error("API enumeration failed at start=%d: %s", start, e)
            db.log_request(conn, f"search/firm?start={start}", error=str(e))
            conn.commit()
            break

        hits = _extract_hits(data)
        total = _extract_total(data)

        if not hits:
            logger.info("No more hits at start=%d. Done.", start)
            break

        for hit in hits:
            _save_hit(conn, hit)
            total_found += 1

        conn.commit()
        logger.info(
            "Enumerated %d / %d firms (start=%d)",
            total_found,
            total or "?",
            start,
        )

        if total and start + page_size >= total:
            break

        start += page_size

    client.close()
    logger.info("API enumeration complete. Total firms: %d", total_found)
    return total_found


def _extract_hits(data: dict) -> list[dict]:
    """Pull the list of firm hit dicts from the API response.

    The BrokerCheck response structure can vary; try common shapes.
    """
    # Shape 1: { "hits": { "hits": [ ... ] } }
    if "hits" in data and isinstance(data["hits"], dict):
        return data["hits"].get("hits", [])
    # Shape 2: { "results": [ ... ] }
    if "results" in data:
        return data["results"]
    # Shape 3: top-level list
    if isinstance(data, list):
        return data
    return []


def _extract_total(data: dict):
    """Pull the total count from the API response."""
    if "hits" in data and isinstance(data["hits"], dict):
        total = data["hits"].get("total")
        if isinstance(total, dict):
            return total.get("value")
        return total
    return data.get("total")


def _save_hit(conn, hit: dict) -> None:
    """Persist a single search hit as both a listing and a detail record."""
    source = hit.get("_source", hit)

    name = source.get("bc_source_name") or source.get("name") or source.get("firm_name", "")
    if not name:
        return

    # Build address from nested fields
    main_office = {}
    branch_locations = source.get("branchLocations", source.get("branch_locations", []))
    if branch_locations and isinstance(branch_locations, list):
        main_office = branch_locations[0] if branch_locations else {}

    address_parts = []
    addr = main_office.get("address", {}) if isinstance(main_office, dict) else {}
    for key in ("street", "city", "state", "zip"):
        val = addr.get(key)
        if val:
            address_parts.append(str(val))
    address = ", ".join(address_parts) if address_parts else ""

    # Save as listing
    listing = FirmListing(
        name=name.strip(),
        address=address,
        source_page="api_enumeration",
        scraped_at=datetime.utcnow(),
    )
    db.upsert_listing(conn, listing)

    # Save as detail (Phase 2 already done -- we have the CRD)
    crd = source.get("bc_source_id") or source.get("sourceId") or source.get("source_id")
    if crd is not None:
        detail = FirmDetail(
            crd_number=int(crd),
            sec_number=source.get("bc_sec_number") or source.get("secNumber"),
            name=name.strip(),
            other_names="|".join(source.get("otherNames", [])) if isinstance(source.get("otherNames"), list) else source.get("otherNames"),
            finra_approved_registration_count=source.get("finraApprovedRegistrationCount"),
            number_of_branches=source.get("numberOfBranches") or source.get("number_of_branches"),
            street=addr.get("street"),
            city=addr.get("city"),
            state=addr.get("state"),
            zip_code=addr.get("zip"),
            country=addr.get("country"),
            latitude=(main_office.get("coordinates", {}) or {}).get("latitude"),
            longitude=(main_office.get("coordinates", {}) or {}).get("longitude"),
            search_response_raw=json.dumps(source),
            matched_from_name=name.strip(),
            match_confidence=1.0,
            scraped_at=datetime.utcnow(),
        )
        db.upsert_detail(conn, detail, phase=2)


# ---------- Strategy 2: Playwright scraping ----------


def run_playwright_scrape(conn, letter: str | None = None) -> int:
    """Scrape firm listings from FINRA alphabetical pages using Playwright.

    Args:
        conn: SQLite connection.
        letter: If given, scrape only this letter page (e.g. 'a'). Otherwise all.

    Returns the total number of firms scraped.
    """
    from playwright.sync_api import sync_playwright

    completed = db.get_completed_pages(conn)
    pages_to_scrape = FINRA_LETTER_PAGES
    if letter:
        slug = f"firms-we-regulate-{letter.lower()}" if letter != "#" else "firms-we-regulate-no"
        pages_to_scrape = [slug]

    total = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=BROWSER_HEADERS["User-Agent"],
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = context.new_page()

        for slug in pages_to_scrape:
            if slug in completed:
                logger.info("Skipping already-scraped page: %s", slug)
                continue

            url = f"{FINRA_BASE_URL}/{slug}"
            logger.info("Scraping %s", url)

            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                time.sleep(2)  # let dynamic content settle
                html = page.content()
                firms = _parse_firm_page(html, slug)
                for firm in firms:
                    db.upsert_listing(conn, firm)
                    total += 1
                conn.commit()
                db.log_request(conn, url, status_code=200)
                conn.commit()
                logger.info("Parsed %d firms from %s", len(firms), slug)
            except Exception as e:
                logger.error("Failed to scrape %s: %s", url, e)
                db.log_request(conn, url, error=str(e))
                conn.commit()

            time.sleep(PLAYWRIGHT_DELAY)

        browser.close()

    logger.info("Playwright scrape complete. Total firms: %d", total)
    return total


def _parse_firm_page(html: str, source_page: str) -> list[FirmListing]:
    """Parse a FINRA alphabetical firm page into FirmListing objects."""
    soup = BeautifulSoup(html, "html.parser")
    firms = []

    # The firm entries are in the main content area, separated by middle-dot (·)
    # Look for the main content div
    content = soup.find("div", class_="field--name-body") or soup.find("article") or soup.find("main")
    if not content:
        logger.warning("Could not find content div on %s", source_page)
        return firms

    text = content.get_text(separator="\n")

    # Split by middle dot separator
    entries = re.split(r"\s*·\s*", text)

    for entry in entries:
        entry = entry.strip()
        if not entry or len(entry) < 5:
            continue

        # Try to separate firm name from address
        # Pattern: FIRM NAME IN CAPS followed by address
        # The name is typically the first line or all-caps portion
        lines = [l.strip() for l in entry.split("\n") if l.strip()]
        if not lines:
            continue

        name = lines[0].strip()
        address_lines = lines[1:] if len(lines) > 1 else []

        # Check for mailing address
        mailing_address = None
        primary_parts = []
        for line in address_lines:
            if line.lower().startswith("mailing address"):
                # Everything after "Mailing Address:" is the mailing address
                mailing_address = line.split(":", 1)[-1].strip() if ":" in line else ""
            else:
                primary_parts.append(line)

        address = ", ".join(primary_parts)

        if name:
            firms.append(
                FirmListing(
                    name=name,
                    address=address,
                    mailing_address=mailing_address,
                    source_page=source_page,
                    scraped_at=datetime.utcnow(),
                )
            )

    return firms


# ---------- Strategy 3: Requests with browser headers (simple fallback) ----------


def run_requests_scrape(conn, letter: str | None = None) -> int:
    """Scrape using plain requests with full browser headers. Last resort."""
    import requests as req

    completed = db.get_completed_pages(conn)
    pages_to_scrape = FINRA_LETTER_PAGES
    if letter:
        slug = f"firms-we-regulate-{letter.lower()}" if letter != "#" else "firms-we-regulate-no"
        pages_to_scrape = [slug]

    total = 0
    session = req.Session()
    session.headers.update(BROWSER_HEADERS)

    for slug in pages_to_scrape:
        if slug in completed:
            continue

        url = f"{FINRA_BASE_URL}/{slug}"
        logger.info("Fetching %s via requests", url)

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            firms = _parse_firm_page(resp.text, slug)
            for firm in firms:
                db.upsert_listing(conn, firm)
                total += 1
            conn.commit()
            db.log_request(conn, url, status_code=resp.status_code)
            conn.commit()
            logger.info("Parsed %d firms from %s", len(firms), slug)
        except Exception as e:
            logger.error("Requests fallback failed for %s: %s", url, e)
            db.log_request(conn, url, error=str(e))
            conn.commit()

        time.sleep(PLAYWRIGHT_DELAY)

    session.close()
    return total
