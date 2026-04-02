"""Phase 2: Look up CRD numbers for each firm via BrokerCheck search API."""

import json
import logging
from datetime import datetime

from rapidfuzz import fuzz
from tqdm import tqdm

import db
from config import MATCH_THRESHOLD
from models import FirmDetail
from scraper.brokercheck_client import BrokerCheckClient

logger = logging.getLogger(__name__)


def run(conn, delay: float = 0.5) -> dict:
    """For each unmatched firm listing, search BrokerCheck and record the CRD.

    Returns a summary dict with counts of matched, unmatched, and errors.
    """
    unmatched = db.get_unmatched_listings(conn)
    if not unmatched:
        logger.info("No unmatched firms. Phase 2 already complete.")
        return {"matched": 0, "unmatched": 0, "errors": 0}

    logger.info("Phase 2: looking up CRD numbers for %d firms", len(unmatched))

    client = BrokerCheckClient(delay=delay)
    matched = 0
    no_match = 0
    errors = 0
    batch_count = 0

    for name in tqdm(unmatched, desc="Phase 2 - CRD Lookup"):
        try:
            data = client.search_firm(name, count=5)
            hits = _extract_hits(data)

            best = _find_best_match(name, hits)
            if best:
                detail = _hit_to_detail(best, name)
                db.upsert_detail(conn, detail, phase=2)
                matched += 1
                logger.debug("Matched '%s' -> CRD %d (%.2f)", name, detail.crd_number, detail.match_confidence or 0)
            else:
                no_match += 1
                logger.warning("No match for '%s'", name)
                db.log_request(conn, f"search/firm?query={name}", error="no_match")

        except Exception as e:
            errors += 1
            logger.error("Error looking up '%s': %s", name, e)
            db.log_request(conn, f"search/firm?query={name}", error=str(e))

        batch_count += 1
        if batch_count % 10 == 0:
            conn.commit()

    conn.commit()
    client.close()

    summary = {"matched": matched, "unmatched": no_match, "errors": errors}
    logger.info("Phase 2 complete: %s", summary)
    return summary


def _extract_hits(data: dict) -> list[dict]:
    """Pull hit list from the search response."""
    if "hits" in data and isinstance(data["hits"], dict):
        return data["hits"].get("hits", [])
    if "results" in data:
        return data["results"]
    if isinstance(data, list):
        return data
    return []


def _find_best_match(query_name: str, hits: list[dict]) -> dict | None:
    """Find the best matching hit for a given firm name.

    Returns the best hit dict, or None if no match exceeds the threshold.
    """
    if not hits:
        return None

    query_norm = _normalize(query_name)
    best_hit = None
    best_score = 0.0

    for hit in hits:
        source = hit.get("_source", hit)
        hit_name = source.get("bc_source_name") or source.get("name") or ""
        hit_norm = _normalize(hit_name)

        # Exact match
        if hit_norm == query_norm:
            return hit

        # Fuzzy match
        score = fuzz.ratio(query_norm, hit_norm) / 100.0
        if score > best_score:
            best_score = score
            best_hit = hit

    if best_score >= MATCH_THRESHOLD:
        return best_hit

    return None


def _normalize(name: str) -> str:
    """Normalize a firm name for comparison."""
    name = name.upper().strip()
    # Remove common suffixes that vary
    for suffix in (" LLC", " INC", " INC.", " CORP", " CORP.", " LP", " L.P.", " CO.", " LTD", " LTD."):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    # Collapse whitespace
    name = " ".join(name.split())
    return name


def _hit_to_detail(hit: dict, matched_name: str) -> FirmDetail:
    """Convert a search hit dict into a FirmDetail."""
    source = hit.get("_source", hit)

    name = source.get("bc_source_name") or source.get("name") or ""
    crd = int(source.get("bc_source_id") or source.get("sourceId") or source.get("source_id", 0))

    # Address from first branch
    branches = source.get("branchLocations", source.get("branch_locations", []))
    main_office = branches[0] if branches and isinstance(branches, list) else {}
    addr = main_office.get("address", {}) if isinstance(main_office, dict) else {}
    coords = (main_office.get("coordinates", {}) or {}) if isinstance(main_office, dict) else {}

    # Match confidence
    query_norm = _normalize(matched_name)
    hit_norm = _normalize(name)
    confidence = 1.0 if query_norm == hit_norm else fuzz.ratio(query_norm, hit_norm) / 100.0

    return FirmDetail(
        crd_number=crd,
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
        latitude=coords.get("latitude"),
        longitude=coords.get("longitude"),
        search_response_raw=json.dumps(source),
        matched_from_name=matched_name,
        match_confidence=confidence,
        scraped_at=datetime.utcnow(),
    )
