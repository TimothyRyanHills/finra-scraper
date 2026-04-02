"""Data models for the FINRA scraper."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class FirmListing:
    """Phase 1 output: scraped from FINRA alphabetical pages."""

    name: str
    address: str
    mailing_address: Optional[str] = None
    source_page: str = ""
    scraped_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FirmDetail:
    """Phase 2+3 output: enriched data from BrokerCheck."""

    # Identifiers
    crd_number: int
    sec_number: Optional[str] = None
    name: str = ""
    other_names: Optional[str] = None

    # Registration
    finra_approved_registration_count: Optional[int] = None
    number_of_branches: Optional[int] = None

    # Primary address
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Detail fields (Phase 3)
    registration_status: Optional[str] = None
    registration_begin_date: Optional[str] = None
    firm_size: Optional[str] = None
    disclosures_count: Optional[int] = None
    branch_locations_count: Optional[int] = None

    # Raw JSON for future re-parsing
    search_response_raw: str = ""
    detail_response_raw: Optional[str] = None

    # Matching metadata
    matched_from_name: Optional[str] = None
    match_confidence: Optional[float] = None
    scraped_at: datetime = field(default_factory=datetime.utcnow)
