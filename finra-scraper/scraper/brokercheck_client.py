"""HTTP client for the undocumented BrokerCheck API with throttling and retries."""

import time
import logging
from typing import Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import (
    BROKERCHECK_SEARCH_FIRM,
    BROKERCHECK_FIRM_DETAIL,
    API_HEADERS,
    DEFAULT_DELAY,
    MAX_RETRIES,
)

logger = logging.getLogger(__name__)


class BrokerCheckError(Exception):
    """Raised when the BrokerCheck API returns an unexpected response."""


class BrokerCheckClient:
    """Thin HTTP client wrapping the BrokerCheck search and detail endpoints."""

    def __init__(self, delay: float = DEFAULT_DELAY):
        self.session = requests.Session()
        self.session.headers.update(API_HEADERS)
        self.delay = delay
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.monotonic()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=2, max=16),
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout, BrokerCheckError)),
    )
    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        self._throttle()
        resp = self.session.get(url, params=params, timeout=30)

        if resp.status_code == 429:
            logger.warning("Rate limited (429). Backing off.")
            self.delay = min(self.delay * 2, 10.0)
            raise BrokerCheckError("Rate limited")

        if resp.status_code >= 500:
            logger.warning("Server error %d from %s", resp.status_code, url)
            raise BrokerCheckError(f"Server error {resp.status_code}")

        resp.raise_for_status()
        return resp

    def search_firm(
        self, query: str, start: int = 0, count: int = 10
    ) -> dict:
        """Search for firms by name. Returns the raw JSON response."""
        params = {
            "query": query,
            "filter": "action=active,type=firm",
            "hl": "true",
            "nrows": count,
            "start": start,
            "r": 25,
            "sort": "score+desc",
            "wt": "json",
        }
        resp = self._get(BROKERCHECK_SEARCH_FIRM, params=params)
        return resp.json()

    def search_firm_all(self, start: int = 0, count: int = 100) -> dict:
        """Enumerate all firms via empty-query pagination."""
        params = {
            "query": "",
            "filter": "action=active,type=firm",
            "hl": "true",
            "nrows": count,
            "start": start,
            "r": 25,
            "sort": "bc_source_name+asc",
            "wt": "json",
        }
        resp = self._get(BROKERCHECK_SEARCH_FIRM, params=params)
        return resp.json()

    def get_firm_detail(self, crd_number: int) -> dict:
        """Fetch comprehensive detail for a firm by CRD number."""
        url = f"{BROKERCHECK_FIRM_DETAIL}/{crd_number}"
        resp = self._get(url)
        return resp.json()

    def close(self) -> None:
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
