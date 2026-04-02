#!/usr/bin/env python3
"""FINRA Broker-Dealer Scraper - CLI entry point.

Usage:
    python main.py                          # run all phases sequentially
    python main.py --phase 1                # scrape firm list only
    python main.py --phase 2                # CRD lookup only
    python main.py --phase 3                # detail enrichment only
    python main.py --export                 # export to CSV/JSON
    python main.py --try-api-enumeration    # enumerate firms via BrokerCheck API (skip HTML scraping)
    python main.py --delay 1.0              # custom rate limit (seconds between requests)
    python main.py --letter a               # Phase 1 only: scrape a specific letter page
    python main.py --stats                  # show current database stats
"""

import argparse
import logging
import sys
import os

# Ensure the project root is on sys.path so imports work when run from any directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db
import export
from config import DEFAULT_DELAY
from scraper import phase1_firm_list, phase2_crd_lookup, phase3_firm_details


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape FINRA broker-dealer firm data."
    )
    parser.add_argument(
        "--phase",
        type=int,
        choices=[1, 2, 3],
        help="Run only a specific phase (1=firm list, 2=CRD lookup, 3=detail).",
    )
    parser.add_argument(
        "--try-api-enumeration",
        action="store_true",
        help="Phase 1 alternative: enumerate firms via BrokerCheck API instead of HTML scraping.",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        dest="do_export",
        help="Export data to CSV/JSON.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Seconds between API requests (default: {DEFAULT_DELAY}).",
    )
    parser.add_argument(
        "--letter",
        type=str,
        help="Phase 1 only: scrape a specific letter page (e.g., 'a', 'z', '#').",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show current database statistics and exit.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger("main")

    conn = db.get_or_create_db()

    # Stats only
    if args.stats:
        stats = db.get_stats(conn)
        print("\n=== FINRA Scraper Database Stats ===")
        print(f"  Firm listings (Phase 1): {stats['firm_listings']}")
        print(f"  CRD matched (Phase 2):   {stats['crd_matched']}")
        print(f"  Detail enriched (Phase 3): {stats['detail_enriched']}")
        print(f"  Unmatched firms:          {stats['unmatched']}")
        print()
        conn.close()
        return

    # Export only
    if args.do_export:
        paths = export.export_all(conn)
        if paths:
            print("\nExported files:")
            for fmt, path in paths.items():
                print(f"  {fmt}: {path}")
        conn.close()
        return

    # Determine which phases to run
    phases = [args.phase] if args.phase else [1, 2, 3]

    # Phase 1
    if 1 in phases:
        print("\n=== Phase 1: Collecting Firm List ===")
        if args.try_api_enumeration:
            logger.info("Using BrokerCheck API enumeration strategy")
            count = phase1_firm_list.run_api_enumeration(conn, delay=args.delay)
        else:
            logger.info("Using Playwright scraping strategy")
            try:
                count = phase1_firm_list.run_playwright_scrape(conn, letter=args.letter)
            except Exception as e:
                logger.warning("Playwright failed (%s), trying requests fallback", e)
                count = phase1_firm_list.run_requests_scrape(conn, letter=args.letter)
        print(f"  Firms collected: {count}")

    # Phase 2
    if 2 in phases:
        print("\n=== Phase 2: CRD Number Lookup ===")
        summary = phase2_crd_lookup.run(conn, delay=args.delay)
        print(f"  Matched: {summary['matched']}")
        print(f"  Unmatched: {summary['unmatched']}")
        print(f"  Errors: {summary['errors']}")

    # Phase 3
    if 3 in phases:
        print("\n=== Phase 3: Detail Enrichment ===")
        summary = phase3_firm_details.run(conn, delay=args.delay)
        print(f"  Enriched: {summary['enriched']}")
        print(f"  Errors: {summary['errors']}")

    # Auto-export after full run
    if not args.phase:
        print("\n=== Exporting Data ===")
        paths = export.export_all(conn)
        if paths:
            for fmt, path in paths.items():
                print(f"  {fmt}: {path}")

    # Final stats
    stats = db.get_stats(conn)
    print("\n=== Final Stats ===")
    print(f"  Firm listings: {stats['firm_listings']}")
    print(f"  CRD matched:   {stats['crd_matched']}")
    print(f"  Detail enriched: {stats['detail_enriched']}")
    print(f"  Unmatched:      {stats['unmatched']}")
    print()

    conn.close()


if __name__ == "__main__":
    main()
