"""Phase 3: Fetch comprehensive detail for each firm by CRD number."""

import json
import logging
from datetime import datetime

from tqdm import tqdm

import db
from models import FirmDetail
from scraper.brokercheck_client import BrokerCheckClient

logger = logging.getLogger(__name__)


def run(conn, delay: float = 0.5) -> dict:
    """Enrich all phase-2 firms with full detail from the BrokerCheck detail API.

    Returns a summary dict.
    """
    crd_numbers = db.get_crd_numbers_needing_detail(conn)
    if not crd_numbers:
        logger.info("No firms need detail enrichment. Phase 3 already complete.")
        return {"enriched": 0, "errors": 0}

    logger.info("Phase 3: enriching %d firms with full detail", len(crd_numbers))

    client = BrokerCheckClient(delay=delay)
    enriched = 0
    errors = 0
    batch_count = 0

    for crd in tqdm(crd_numbers, desc="Phase 3 - Detail Enrichment"):
        try:
            data = client.get_firm_detail(crd)
            _update_detail(conn, crd, data)
            enriched += 1
            logger.debug("Enriched CRD %d", crd)
        except Exception as e:
            errors += 1
            logger.error("Error enriching CRD %d: %s", crd, e)
            db.log_request(conn, f"firm/{crd}", error=str(e))

        batch_count += 1
        if batch_count % 10 == 0:
            conn.commit()

    conn.commit()
    client.close()

    summary = {"enriched": enriched, "errors": errors}
    logger.info("Phase 3 complete: %s", summary)
    return summary


def _update_detail(conn, crd_number: int, data: dict) -> None:
    """Extract fields from the detail API response and update the DB record."""
    # The detail response structure varies; try to extract from common shapes
    firm = data
    if "firmSummary" in data:
        firm = data["firmSummary"]
    elif "hits" in data:
        hits = data["hits"]
        if isinstance(hits, dict) and "hits" in hits:
            hit_list = hits["hits"]
            if hit_list:
                firm = hit_list[0].get("_source", hit_list[0])

    # Extract registration info
    reg_status = firm.get("registrationStatus") or firm.get("registration_status")
    reg_begin = firm.get("registrationBeginDate") or firm.get("registration_begin_date")
    firm_size = firm.get("firmSize") or firm.get("firm_size")

    # Disclosures
    disclosures = firm.get("disclosures", [])
    disclosures_count = len(disclosures) if isinstance(disclosures, list) else firm.get("disclosureCount", 0)
    if firm.get("hasDisclosures") is False:
        disclosures_count = 0

    # Branch offices
    branches = firm.get("branchLocations", firm.get("branch_locations", []))
    branch_count = len(branches) if isinstance(branches, list) else firm.get("numberOfBranches")

    # Address (may have better data than search)
    main_addr = {}
    if branches and isinstance(branches, list):
        main_office = branches[0] if branches else {}
        main_addr = main_office.get("address", {}) if isinstance(main_office, dict) else {}

    name = firm.get("bc_source_name") or firm.get("name") or firm.get("firmName", "")

    detail = FirmDetail(
        crd_number=crd_number,
        sec_number=firm.get("bc_sec_number") or firm.get("secNumber"),
        name=name.strip() if name else "",
        registration_status=reg_status,
        registration_begin_date=reg_begin,
        firm_size=firm_size,
        disclosures_count=disclosures_count,
        branch_locations_count=branch_count,
        street=main_addr.get("street"),
        city=main_addr.get("city"),
        state=main_addr.get("state"),
        zip_code=main_addr.get("zip"),
        country=main_addr.get("country"),
        detail_response_raw=json.dumps(data),
        scraped_at=datetime.utcnow(),
    )
    db.upsert_detail(conn, detail, phase=3)
