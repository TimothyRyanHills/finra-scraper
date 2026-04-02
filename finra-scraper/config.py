"""Configuration constants for the FINRA scraper."""

import os

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "finra.db")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

# Rate limiting
DEFAULT_DELAY = 0.5          # seconds between BrokerCheck API requests
PLAYWRIGHT_DELAY = 3.0       # seconds between page loads
MAX_RETRIES = 3

# Matching
MATCH_THRESHOLD = 0.85       # fuzzy match ratio for firm names

# BrokerCheck API
BROKERCHECK_BASE_URL = "https://api.brokercheck.finra.org"
BROKERCHECK_SEARCH_FIRM = f"{BROKERCHECK_BASE_URL}/search/firm"
# Detail endpoint: /search/firm/{crd} (NOT /firm/{crd} which returns 403)
BROKERCHECK_FIRM_DETAIL = f"{BROKERCHECK_BASE_URL}/search/firm"

# FINRA directory pages
FINRA_BASE_URL = "https://www.finra.org/about/firms-we-regulate"
FINRA_LETTER_PAGES = [f"firms-we-regulate-{chr(c)}" for c in range(ord("a"), ord("z") + 1)]
FINRA_LETTER_PAGES.insert(0, "firms-we-regulate-no")  # numbers page first

# Browser headers (fallback for requests-based scraping)
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.finra.org/",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Connection": "keep-alive",
}

# BrokerCheck API headers
API_HEADERS = {
    "User-Agent": BROWSER_HEADERS["User-Agent"],
    "Accept": "application/json",
    "Referer": "https://brokercheck.finra.org/",
    "Origin": "https://brokercheck.finra.org",
}
