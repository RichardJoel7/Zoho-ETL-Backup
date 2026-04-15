"""
api_client.py — Retry-hardened HTTP session used by ALL extractors.

Every API call in this pipeline goes through this client. Features:
  - Exponential backoff with jitter (tenacity)
  - Automatic 429 / 5xx retry with Retry-After header respect
  - Per-run API call counter (thread-safe)
  - Structured error logging
"""

import logging
import threading
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
    before_sleep_log,
    RetryError,
)
from urllib3.util.retry import Retry

from config.settings import (
    RETRY_MAX_ATTEMPTS,
    RETRY_WAIT_MIN_SECONDS,
    RETRY_WAIT_MAX_SECONDS,
    RETRY_MULTIPLIER,
)

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when the API returns HTTP 429 — triggers tenacity retry."""
    pass


class TransientError(Exception):
    """Raised for 5xx responses — triggers tenacity retry."""
    pass


class APIClient:
    """
    Thread-safe, retry-hardened requests wrapper.
    Subclass this in each extractor and set self.base_url + self.headers.
    """

    def __init__(self):
        self._call_count: int = 0
        self._lock = threading.Lock()
        self.session = self._build_session()

    # ------------------------------------------------------------------
    # Session setup — connection-level retries (network drops, DNS)
    # ------------------------------------------------------------------
    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        urllib3_retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=urllib3_retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    # ------------------------------------------------------------------
    # Core request with tenacity retry
    # ------------------------------------------------------------------
    @retry(
        retry=retry_if_exception_type((RateLimitError, TransientError)),
        stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
        wait=wait_exponential(
            multiplier=RETRY_MULTIPLIER,
            min=RETRY_WAIT_MIN_SECONDS,
            max=RETRY_WAIT_MAX_SECONDS,
        ) + wait_random(0, 2),  # jitter prevents thundering herd
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        json: Optional[Dict] = None,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        with self._lock:
            self._call_count += 1

        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=timeout,
            )
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise TransientError(str(e)) from e
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timed out: {url}")
            raise TransientError("Timeout") from e

        # Handle rate limiting — respect Retry-After if provided
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            logger.warning(f"Rate limited. Waiting {retry_after}s before retry.")
            time.sleep(retry_after)
            raise RateLimitError(f"HTTP 429 from {url}")

        # Transient server errors
        if response.status_code in (500, 502, 503, 504):
            logger.warning(f"Transient server error {response.status_code} from {url}")
            raise TransientError(f"HTTP {response.status_code}")

        # Client errors that should NOT be retried
        if response.status_code == 204:
            return {}  # No content — not an error

        if not response.ok:
            logger.error(
                f"Non-retryable HTTP {response.status_code} from {url}: {response.text[:400]}"
            )
            response.raise_for_status()

        return response.json()

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------
    def get(self, url: str, **kwargs) -> Dict[str, Any]:
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> Dict[str, Any]:
        return self._request("POST", url, **kwargs)

    @property
    def api_call_count(self) -> int:
        return self._call_count
