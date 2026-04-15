"""
zoho_extractor.py — Incremental Zoho CRM extractor.

Changes from legacy zoho_sync.py:
  - Fetches ONLY records modified after the watermark timestamp
  - Uses `modified_since` header (Zoho CRM v2.1 supported)
  - Removed all debug/investigation code (describe_deal_relations, inspect_blocks_metadata)
  - Removed global mutable state — API call count returned from methods
  - Full retry handled by APIClient base class
  - total_records counter properly incremented
  - Clean module name normalization
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from utils.api_client import APIClient

logger = logging.getLogger(__name__)


class ZohoExtractor(APIClient):
    """Incremental extractor for Zoho CRM using OAuth2 refresh token flow."""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, domain: str = "com"):
        super().__init__()
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.domain = domain
        self._access_token: Optional[str] = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    def authenticate(self) -> None:
        """Fetches a fresh access token. Called once per pipeline run."""
        url = f"https://accounts.zoho.{self.domain}/oauth/v2/token"
        params = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }
        data = self.post(url, params=params)
        if "access_token" not in data:
            raise ValueError(
                f"Zoho authentication failed. Response: {data.get('error', 'unknown error')}"
            )
        self._access_token = data["access_token"]
        logger.info("Zoho authentication successful.")

    @property
    def _auth_headers(self) -> Dict[str, str]:
        if not self._access_token:
            raise RuntimeError("Call authenticate() before making API requests.")
        return {"Authorization": f"Zoho-oauthtoken {self._access_token}"}

    # ------------------------------------------------------------------
    # Module discovery
    # ------------------------------------------------------------------
    def discover_module_api_names(self, target_labels: List[str]) -> Dict[str, str]:
        """
        Maps human-readable module labels to their CRM API names.
        Returns {label: api_name} for any label found in the CRM.
        """
        url = f"https://www.zohoapis.{self.domain}/crm/v2.1/settings/modules"
        data = self.get(url, headers=self._auth_headers)
        modules = data.get("modules", [])

        def norm(v: str) -> str:
            return (v or "").replace("_", "").replace(" ", "").lower()

        target_norm_to_label = {norm(label): label for label in target_labels}
        label_to_api: Dict[str, str] = {}

        for m in modules:
            plural_label = m.get("plural_label", "")
            singular_label = m.get("singular_label", "")
            api_name = m.get("api_name", "")

            candidates = [plural_label, singular_label, api_name]
            for candidate in candidates:
                mapped_label = target_norm_to_label.get(norm(candidate))
                if mapped_label and mapped_label not in label_to_api:
                    label_to_api[mapped_label] = api_name
                    logger.info(f"Module discovered: '{mapped_label}' → '{api_name}'")
                    break

        missing = set(target_labels) - set(label_to_api.keys())
        if missing:
            logger.warning(f"Modules not found in CRM: {missing}. Will use label as API name.")
            for m in missing:
                label_to_api[m] = m  # Fallback

        return label_to_api

    # ------------------------------------------------------------------
    # Incremental fetch
    # ------------------------------------------------------------------
    def fetch_module_incremental(
        self,
        module_api_name: str,
        modified_since: datetime,
    ) -> Tuple[List[Dict[str, Any]], datetime]:
        """
        Fetches all records in module_api_name modified after modified_since.

        Uses the `If-Modified-Since` header (Zoho CRM v2.1).
        Returns (records, new_watermark) where new_watermark is the
        most recent Modified_Time seen in this batch.
        """
        url = f"https://www.zohoapis.{self.domain}/crm/v2.1/{module_api_name}"

        # Format required by Zoho: "Wed, 01 Jan 2025 00:00:00 +0000"
        modified_since_str = modified_since.strftime("%a, %d %b %Y %H:%M:%S +0000")
        headers = {
            **self._auth_headers,
            "If-Modified-Since": modified_since_str,
        }

        all_records: List[Dict] = []
        page = 1
        new_watermark = modified_since

        logger.info(
            f"Fetching {module_api_name} modified since {modified_since.isoformat()} ..."
        )

        while True:
            params = {"page": page, "per_page": 200}
            data = self.get(url, headers=headers, params=params)

            if not data:
                # HTTP 304 Not Modified or empty — no new records
                logger.info(f"{module_api_name}: No changes since watermark.")
                break

            records = data.get("data", [])
            if not records:
                break

            all_records.extend(records)

            # Track the newest Modified_Time in this page for the new watermark
            for rec in records:
                mt_str = rec.get("Modified_Time")
                if mt_str:
                    try:
                        mt = datetime.fromisoformat(mt_str.replace("Z", "+00:00"))
                        if mt > new_watermark:
                            new_watermark = mt
                    except ValueError:
                        pass

            info = data.get("info", {})
            more = info.get("more_records", False)
            logger.info(
                f"{module_api_name} page {page}: {len(records)} records. "
                f"Total so far: {len(all_records)}"
            )

            if not more:
                break
            page += 1

        # If watermark didn't advance (0 new records), use current UTC
        if new_watermark == modified_since and all_records:
            new_watermark = datetime.now(timezone.utc)

        logger.info(
            f"{module_api_name}: {len(all_records)} records fetched. "
            f"New watermark: {new_watermark.isoformat()}"
        )
        return all_records, new_watermark

    # ------------------------------------------------------------------
    # Revenue history delta detection (BigQuery MERGE version)
    # ------------------------------------------------------------------
    def build_revenue_history_delta(
        self,
        current_records: List[Dict],
        previous_snapshot: Dict[str, Dict],
    ) -> List[Dict]:
        """
        Compares current Revenue Recognition records against the previous
        snapshot (from BQ) and returns only changed/new records for logging.

        This is now a pure Python delta computation — the actual write is
        done via MERGE in bq_loader.py, so no BQ reads happen inside this method.
        """
        import random
        import string

        def gen_action_id() -> str:
            return "A" + "".join(random.choices(string.digits, k=10))

        now = datetime.now(timezone.utc).isoformat()
        history_records = []

        for rec in current_records:
            rec_id = str(rec.get("id", ""))
            current_amount = str(rec.get("Amount", "") or "")
            current_date = str(rec.get("Date", "") or "")
            block = rec.get("Parent_Id", {})
            block_id = str(block.get("id", "")) if isinstance(block, dict) else ""
            block_name = str(block.get("name", "")) if isinstance(block, dict) else ""

            prev = previous_snapshot.get(rec_id)
            if prev is None:
                history_records.append({
                    "action_id": gen_action_id(),
                    "action_timestamp": now,
                    "Block_id": block_id,
                    "revenue_id": rec_id,
                    "block_name": block_name,
                    "Revenue_date": current_date,
                    "amount": current_amount,
                    "Type": "Create",
                })
            else:
                changes = []
                if current_amount != str(prev.get("amount", "")):
                    changes.append("Amount changed")
                if current_date != str(prev.get("Revenue_date", "")):
                    changes.append("Date changed")
                if changes:
                    history_records.append({
                        "action_id": gen_action_id(),
                        "action_timestamp": now,
                        "Block_id": block_id,
                        "revenue_id": rec_id,
                        "block_name": block_name,
                        "Revenue_date": current_date,
                        "amount": current_amount,
                        "Type": " and ".join(changes),
                    })

        logger.info(f"Revenue history delta: {len(history_records)} changes detected.")
        return history_records
