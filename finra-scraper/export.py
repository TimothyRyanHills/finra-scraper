"""Export scraped data from SQLite to CSV and JSON."""

import json
import logging
import os

import pandas as pd

import db
from config import OUTPUT_DIR

logger = logging.getLogger(__name__)

# Columns for CSV export (excludes raw JSON blobs)
CSV_COLUMNS = [
    "crd_number",
    "sec_number",
    "name",
    "other_names",
    "registration_status",
    "registration_begin_date",
    "firm_size",
    "firm_type",
    "formed_state",
    "formed_date",
    "phone",
    "finra_approved_registration_count",
    "number_of_branches",
    "branch_locations_count",
    "disclosures_count",
    "street",
    "city",
    "state",
    "zip_code",
    "country",
    "latitude",
    "longitude",
    "matched_from_name",
    "match_confidence",
    "phase",
    "scraped_at",
]


def export_all(conn) -> dict[str, str]:
    """Export firm_details to CSV and JSON. Returns dict of output file paths."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    rows = db.get_all_details(conn)
    if not rows:
        logger.warning("No data to export.")
        return {}

    # Convert to list of dicts
    records = [dict(row) for row in rows]

    # CSV (flat, no raw JSON)
    csv_path = os.path.join(OUTPUT_DIR, "firms.csv")
    df = pd.DataFrame(records)
    csv_cols = [c for c in CSV_COLUMNS if c in df.columns]
    df[csv_cols].to_csv(csv_path, index=False, encoding="utf-8")
    logger.info("Exported %d firms to %s", len(records), csv_path)

    # JSON (full data including raw blobs)
    json_path = os.path.join(OUTPUT_DIR, "firms.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str, ensure_ascii=False)
    logger.info("Exported %d firms to %s", len(records), json_path)

    # JSON Lines
    jsonl_path = os.path.join(OUTPUT_DIR, "firms.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    logger.info("Exported %d firms to %s", len(records), jsonl_path)

    return {"csv": csv_path, "json": json_path, "jsonl": jsonl_path}
