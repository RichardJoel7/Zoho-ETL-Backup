"""
hubspot_extractor.py — Incremental HubSpot CRM extractor.

Changes from legacy hubspot_sync.py:
  - All fetches use lastmodifieddate filter → only delta records
  - Pipelines, Owners, Line Items still fetched in full (they're small + no incremental API)
  - No global mutable API_CALLS — count tracked inside APIClient base class
  - Batch fetch for companies replaced by association-based incremental fetch
  - Line item failures no longer silently return empty — they raise with context
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from utils.api_client import APIClient

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"


class HubSpotExtractor(APIClient):
    """Incremental extractor for HubSpot CRM using a private app access token."""

    def __init__(self, access_token: str):
        super().__init__()
        if not access_token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN is required.")
        self.access_token = access_token

    @property
    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Property discovery (unchanged — dynamic fetch is correct pattern)
    # ------------------------------------------------------------------
    def get_all_properties(self, object_type: str) -> List[str]:
        """Fetches all non-hidden property names for an object type."""
        url = f"{HUBSPOT_BASE}/crm/v3/properties/{object_type}"
        data = self.get(url, headers=self._auth_headers)

        properties = [
            p["name"]
            for p in data.get("results", [])
            if not p.get("hidden", False)
        ]
        logger.info(f"Discovered {len(properties)} properties for {object_type}.")

        # Confirm key custom fields exist
        if object_type == "deals" and "bidding" not in properties:
            logger.warning("Custom field 'bidding' (In Bidding) not found in deals properties.")
        return properties

    # ------------------------------------------------------------------
    # Incremental object fetch (deals, companies)
    # ------------------------------------------------------------------
    def fetch_objects_incremental(
        self,
        object_type: str,
        properties: List[str],
        modified_since: datetime,
        associations: Optional[List[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], datetime]:
        """
        Fetches records modified after modified_since using the
        HubSpot Search API (POST /crm/v3/objects/{type}/search).

        The Search API supports filterGroups with lastmodifieddate — this is
        the correct way to do incremental HubSpot syncs.

        Returns (records, new_watermark).
        """
        url = f"{HUBSPOT_BASE}/crm/v3/objects/{object_type}/search"

        # Format: "2024-01-01T00:00:00.000Z"
        modified_since_ms = int(modified_since.timestamp() * 1000)

        all_records: List[Dict] = []
        after: Optional[str] = None
        new_watermark = modified_since

        logger.info(
            f"Fetching {object_type} modified since {modified_since.isoformat()} ..."
        )

        while True:
            payload: Dict[str, Any] = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "lastmodifieddate",
                                "operator": "GTE",
                                "value": str(modified_since_ms),
                            }
                        ]
                    }
                ],
                "properties": properties,
                "limit": 100,
                "sorts": [{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}],
            }
            if after:
                payload["after"] = after

            data = self.post(url, headers=self._auth_headers, json=payload)
            results = data.get("results", [])

            for item in results:
                flat = item.get("properties", {}).copy()
                flat["id"] = item.get("id")
                flat["createdAt"] = item.get("createdAt")
                flat["updatedAt"] = item.get("updatedAt")
                flat["archived"] = item.get("archived", False)

                # Extract company associations for deals
                if object_type == "deals" and associations and "companies" in associations:
                    company_assocs = (
                        item.get("associations", {})
                        .get("companies", {})
                        .get("results", [])
                    )
                    ids = sorted({a["id"] for a in company_assocs if a.get("id")})
                    flat["associated_company_ids"] = ",".join(ids) if ids else None

                # Track newest lastmodifieddate for watermark
                lmd = flat.get("lastmodifieddate") or flat.get("hs_lastmodifieddate")
                if lmd:
                    try:
                        ts = datetime.fromisoformat(lmd.replace("Z", "+00:00"))
                        if ts > new_watermark:
                            new_watermark = ts
                    except ValueError:
                        pass

                all_records.append(flat)

            logger.info(
                f"{object_type}: page fetched {len(results)} records. Total: {len(all_records)}"
            )

            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after:
                break

        if not all_records:
            logger.info(f"{object_type}: No changes since watermark.")

        return all_records, new_watermark

    # ------------------------------------------------------------------
    # Full fetches for small/metadata objects
    # ------------------------------------------------------------------
    def fetch_pipelines(self) -> List[Dict]:
        """Full fetch — pipeline metadata is small and has no incremental API."""
        url = f"{HUBSPOT_BASE}/crm/v3/pipelines/deals"
        data = self.get(url, headers=self._auth_headers)
        pipeline_data = []
        for pipe in data.get("results", []):
            for stage in pipe.get("stages", []):
                pipeline_data.append({
                    "pipeline_id": pipe.get("id"),
                    "pipeline_label": pipe.get("label"),
                    "stage_id": stage.get("id"),
                    "stage_label": stage.get("label"),
                    "display_order": stage.get("displayOrder"),
                })
        logger.info(f"Fetched {len(pipeline_data)} pipeline stages.")
        return pipeline_data

    def fetch_owners(self) -> List[Dict]:
        """Full fetch of active + archived owners."""
        url = f"{HUBSPOT_BASE}/crm/v3/owners"
        all_owners: List[Dict] = []

        for is_archived in [False, True]:
            after = None
            while True:
                params: Dict[str, Any] = {
                    "limit": 100,
                    "archived": str(is_archived).lower(),
                }
                if after:
                    params["after"] = after

                data = self.get(url, headers=self._auth_headers, params=params)
                for owner in data.get("results", []):
                    record = owner.copy()
                    fn = owner.get("firstName", "")
                    ln = owner.get("lastName", "")
                    record["fullName"] = f"{fn} {ln}".strip() if fn or ln else None
                    record["archived"] = is_archived
                    all_owners.append(record)

                after = data.get("paging", {}).get("next", {}).get("after")
                if not after:
                    break

        logger.info(f"Fetched {len(all_owners)} owners (active + archived).")
        return all_owners

    def fetch_line_items_incremental(
        self, modified_since: datetime
    ) -> Tuple[List[Dict], datetime]:
        """
        Incremental line items fetch using Search API.
        Returns (records, new_watermark).
        """
        url = f"{HUBSPOT_BASE}/crm/v3/objects/line_items/search"
        modified_since_ms = int(modified_since.timestamp() * 1000)

        properties = [
            "name", "price", "quantity", "amount", "hs_product_id",
            "description", "recurringbillingfrequency",
            "hs_recurring_billing_start_date", "lastmodifieddate",
        ]

        all_lines: List[Dict] = []
        after = None
        new_watermark = modified_since

        while True:
            payload: Dict[str, Any] = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "lastmodifieddate",
                                "operator": "GTE",
                                "value": str(modified_since_ms),
                            }
                        ]
                    }
                ],
                "properties": properties,
                "associations": ["deals"],
                "limit": 100,
            }
            if after:
                payload["after"] = after

            data = self.post(url, headers=self._auth_headers, json=payload)
            results = data.get("results", [])

            for item in results:
                flat = item.get("properties", {}).copy()
                flat["id"] = item.get("id")
                deal_assocs = (
                    item.get("associations", {})
                    .get("deals", {})
                    .get("results", [])
                )
                flat["associated_deal_id"] = deal_assocs[0].get("id") if deal_assocs else None

                lmd = flat.get("lastmodifieddate")
                if lmd:
                    try:
                        ts = datetime.fromisoformat(lmd.replace("Z", "+00:00"))
                        if ts > new_watermark:
                            new_watermark = ts
                    except ValueError:
                        pass

                all_lines.append(flat)

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        logger.info(f"Line items: {len(all_lines)} records fetched incrementally.")
        return all_lines, new_watermark
