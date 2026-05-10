"""
WhaleClient: HTTP scraping client for whale-alert.io.

Extracts top-whale cryptocurrency movement data using
BeautifulSoup and models it into a typed dataclass.
"""

import os
import requests

from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class WhaleRecord:
    """Represents a single row from the whale-alert.io top-whales table."""

    datetime_utc: datetime
    crypto: str
    known: str
    unknown: str

    def to_dict(self) -> dict:
        return {
            "datetime_utc": self.datetime_utc.isoformat(),
            "crypto": self.crypto,
            "known": self.known,
            "unknown": self.unknown,
        }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class WhaleClient:
    """
    HTTP client responsible for extracting whale data from whale-alert.io.

    Args:
        url: Target URL to scrape. Falls back to the WHALE_URL env var.
        timeout: Request timeout in seconds. Defaults to 30.
    """

    DEFAULT_URL: str = "https://whale-alert.io/whales.html"

    def __init__(
        self,
        url: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        self.url: str = url or os.getenv("WHALE_URL", self.DEFAULT_URL)
        self.timeout: int = timeout
        self._session: requests.Session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; WhaleAlertBot/1.0)"
                )
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self) -> List[WhaleRecord]:
        """
        Fetches the page and parses the whale-alert table.

        Returns:
            List of WhaleRecord dataclass instances.

        Raises:
            requests.RequestException: On network or HTTP error.
            ValueError: If the expected table structure is not found.
        """
        logger.info("Starting extraction from %s", self.url)

        try:
            response = self._fetch()
            records = self._parse(response.content)
            logger.info(
                "Extraction successful — %d records retrieved", len(records)
            )
            return records

        except requests.RequestException as exc:
            logger.error("Network error during extraction: %s", exc)
            raise
        except ValueError as exc:
            logger.error("Parsing error: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch(self) -> requests.Response:
        """Performs the HTTP GET and validates the response."""
        logger.debug("GET %s (timeout=%ds)", self.url, self.timeout)
        response = self._session.get(self.url, timeout=self.timeout)
        response.raise_for_status()
        response.encoding = "utf-8"
        logger.debug("Response received — status %s", response.status_code)
        return response

    def _parse(self, content: bytes) -> List[WhaleRecord]:
        """Parses raw HTML content into a list of WhaleRecord objects."""
        soup = BeautifulSoup(content, "html.parser")
        rows = soup.select("table.table tbody tr")

        if not rows:
            raise ValueError(
                "No table rows found. The page structure may have changed."
            )

        logger.debug("Found %d table rows to parse", len(rows))

        extracted_at: datetime = datetime.now(timezone.utc)
        records: List[WhaleRecord] = []

        for row in rows:
            try:
                header_cell = row.find("th", {"scope": "row"})
                img = header_cell.find("img") if header_cell else None
                crypto: str = (
                    img["alt"].strip()
                    if img
                    else (header_cell.get_text(strip=True) if header_cell else "")
                )

                tds = row.find_all("td")
                known: str = tds[0].get_text(strip=True) if len(tds) > 0 else ""
                unknown: str = tds[1].get_text(strip=True) if len(tds) > 1 else ""

                records.append(
                    WhaleRecord(
                        datetime_utc=extracted_at,
                        crypto=crypto,
                        known=known,
                        unknown=unknown,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping malformed row: %s", exc)

        return records

    def close(self) -> None:
        """Closes the underlying HTTP session."""
        self._session.close()
        logger.debug("HTTP session closed")

    # Context manager support
    def __enter__(self) -> "WhaleClient":
        return self

    def __exit__(self, *_) -> None:
        self.close()
