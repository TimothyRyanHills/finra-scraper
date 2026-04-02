"""Phase 3: Fetch comprehensive detail for each firm by CRD number.

The detail endpoint is /search/firm/{crd} which returns a nested JSON
structure with content as a JSON string containing basicInformation,
firmAddressDetails, registrations, disclosures, etc.
"""

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
            db.log_request(conn, f"search/firm/{crd}", error=str(e))

        batch_count += 1
        if batch_count % 10 == 0:
            conn.commit()

    conn.commit()
    client.close()

    summary = {"enriched": enriched, "errors": errors}
    logger.info("Phase 3 complete: %s", summary)
    return summary


def _update_detail(conn, crd_number: int, data: dict) -> None:
    """Extract fields from the detail API response and update the DB record.

    The detail response structure:
    {
      "hits": {
        "total": 1,
        "hits": [{
          "_source": {
            "content": "{JSON string with basicInformation, firmAddressDetails, ...}"
          }
        }]
      }
    }
    """
    # Navigate to the content
    content = None
    try:
        hits = data.get("hits", {})
        hit_list = hits.get("hits", [])
        if hit_list:
            source = hit_list[0].get("_source", {})
            content_str = source.get("content", "")
            if content_str:
                content = json.loads(content_str) if isinstance(content_str, str) else content_str
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        logger.warning("Could not parse detail response for CRD %d: %s", crd_number, e)
        return

    if not content:
        logger.warning("Empty content for CRD %d", crd_number)
        return

    basic = content.get("basicInformation", {})
    addr_details = content.get("firmAddressDetails", {})
    regs = content.get("registrations", {})
    disclosures = content.get("disclosures", [])

    # Address
    office = addr_details.get("officeAddress", {})
    street = office.get("street1", "")
    city = office.get("city", "")
    state = office.get("state", "")
    zip_code = office.get("postalCode", "")
    country = office.get("country", "")

    # Registration info
    reg_status = basic.get("firmStatus")
    reg_begin = basic.get("finraLastApprovalDate")
    firm_size = basic.get("firmSize")

    # Disclosures count
    disclosures_count = 0
    if isinstance(disclosures, list):
        for d in disclosures:
            disclosures_count += d.get("disclosureCount", 0) if isinstance(d, dict) else 0

    # Branch count
    branch_count = regs.get("approvedSRORegistrationCount")

    name = basic.get("firmName") or basic.get("iaFirmName") or ""

    # Other names
    other_names_list = basic.get("otherNames", [])
    other_names = "|".join(other_names_list) if isinstance(other_names_list, list) else other_names_list

    # Additional fields
    phone = addr_details.get("businessPhoneNumber")
    firm_type = basic.get("firmType")
    formed_state = basic.get("formedState")
    formed_date = basic.get("formedDate")

    detail = FirmDetail(
        crd_number=crd_number,
        sec_number=basic.get("bdSECNumber"),
        name=name.strip() if name else "",
        other_names=other_names,
        registration_status=reg_status,
        registration_begin_date=reg_begin,
        firm_size=firm_size,
        disclosures_count=disclosures_count,
        branch_locations_count=branch_count,
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
        country=country,
        phone=phone,
        firm_type=firm_type,
        formed_state=formed_state,
        formed_date=formed_date,
        detail_response_raw=json.dumps(content),
        scraped_at=datetime.utcnow(),
    )
    db.upsert_detail(conn, detail, phase=3)
