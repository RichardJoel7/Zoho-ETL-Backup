"""
hubspot_sync.py — Incremental HubSpot → BigQuery sync.

Orchestration flow:
  1. Pipelines + Owners → full fetch (small metadata, no incremental API)
  2. Deals            → incremental (Search API, lastmodifieddate filter)
  3. Companies        → incremental (Search API, lastmodifieddate filter)
                        ALSO fetches any company IDs from new deals that
                        weren't touched recently (association catch-up)
  4. Line Items       → incremental (Search API)

Each object:
  - Reads watermark → fetches delta → stages → MERGES → updates watermark

Writes to: hubspot_incremental dataset (NEVER touches hubspot_raw_data)
"""

import logging
import sys
from datetime import datetime
from typing import Set

from config.settings import (
    HUBSPOT_ACCESS_TOKEN, GCP_CREDENTIALS_JSON,
    BQ_HUBSPOT_DATASET, validate_config,
    IST,
)
from extractors.hubspot_extractor import HubSpotExtractor
from utils.bq_loader import (
    get_bq_client, ensure_dataset_exists,
    load_to_staging, merge_staging_to_target,
    validate_row_count, log_execution,
)
from utils.watermark import ensure_watermark_table, get_watermark, set_watermark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("hubspot_sync")


def run_hubspot_sync() -> None:
    start_time = datetime.now(IST)
    total_records = 0
    client = None

    try:
        validate_config()
        client = get_bq_client(GCP_CREDENTIALS_JSON)

        ensure_dataset_exists(client, BQ_HUBSPOT_DATASET)
        ensure_watermark_table(client)

        extractor = HubSpotExtractor(access_token=HUBSPOT_ACCESS_TOKEN)

        # ----------------------------------------------------------------
        # 1. Pipelines — full fetch (no incremental API, small table)
        # ----------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Syncing: Pipelines (full refresh)")
        pipelines = extractor.fetch_pipelines()
        if pipelines:
            load_to_staging(client, BQ_HUBSPOT_DATASET, pipelines, "_staging_hubspot_Pipelines")
            merge_staging_to_target(
                client, BQ_HUBSPOT_DATASET,
                "_staging_hubspot_Pipelines", "hubspot_Pipelines",
                id_column="stage_id",
            )
            total_records += len(pipelines)

        # ----------------------------------------------------------------
        # 2. Owners — full fetch (small, no incremental API)
        # ----------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Syncing: Owners (full refresh)")
        owners = extractor.fetch_owners()
        if owners:
            load_to_staging(client, BQ_HUBSPOT_DATASET, owners, "_staging_hubspot_Owners")
            merge_staging_to_target(
                client, BQ_HUBSPOT_DATASET,
                "_staging_hubspot_Owners", "hubspot_Owners",
                id_column="id",
            )
            total_records += len(owners)

        # ----------------------------------------------------------------
        # 3. Deals — incremental
        # ----------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Syncing: Deals (incremental)")
        deal_watermark = get_watermark(client, "hubspot", "deals")
        deal_props = extractor.get_all_properties("deals")

        deals, new_deal_wm = extractor.fetch_objects_incremental(
            object_type="deals",
            properties=deal_props,
            modified_since=deal_watermark,
            associations=["companies"],
        )

        if deals:
            load_to_staging(client, BQ_HUBSPOT_DATASET, deals, "_staging_hubspot_Deals")
            deal_count = merge_staging_to_target(
                client, BQ_HUBSPOT_DATASET,
                "_staging_hubspot_Deals", "hubspot_Deals",
                id_column="id",
            )
            validate_row_count(client, BQ_HUBSPOT_DATASET, "hubspot_Deals", expected_min=1)
            total_records += len(deals)
            logger.info(f"Deals: {len(deals)} delta rows. Table total: {deal_count}")
        else:
            logger.info("Deals: No changes since watermark.")

        set_watermark(client, "hubspot", "deals", new_deal_wm)

        # ----------------------------------------------------------------
        # 4. Companies — incremental + association catch-up
        # ----------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Syncing: Companies (incremental + association catch-up)")
        company_watermark = get_watermark(client, "hubspot", "companies")
        company_props = extractor.get_all_properties("companies")

        companies, new_company_wm = extractor.fetch_objects_incremental(
            object_type="companies",
            properties=company_props,
            modified_since=company_watermark,
        )

        # Association catch-up: fetch any company IDs from new deals
        # that may not have been recently modified themselves
        if deals:
            company_ids_from_deals = _extract_company_ids_from_deals(deals)
            if company_ids_from_deals:
                logger.info(
                    f"Association catch-up: fetching {len(company_ids_from_deals)} "
                    f"companies referenced in new deals..."
                )
                catchup_companies = _fetch_companies_by_id(
                    extractor, company_props, company_ids_from_deals
                )
                # Deduplicate against already-fetched companies
                existing_ids = {str(c.get("id")) for c in companies}
                new_catchup = [c for c in catchup_companies if str(c.get("id")) not in existing_ids]
                companies.extend(new_catchup)
                logger.info(f"Association catch-up added {len(new_catchup)} additional companies.")

        if companies:
            load_to_staging(client, BQ_HUBSPOT_DATASET, companies, "_staging_hubspot_Companies")
            company_count = merge_staging_to_target(
                client, BQ_HUBSPOT_DATASET,
                "_staging_hubspot_Companies", "hubspot_Companies",
                id_column="id",
            )
            validate_row_count(client, BQ_HUBSPOT_DATASET, "hubspot_Companies", expected_min=1)
            total_records += len(companies)
            logger.info(f"Companies: {len(companies)} delta rows. Table total: {company_count}")
        else:
            logger.info("Companies: No changes since watermark.")

        set_watermark(client, "hubspot", "companies", new_company_wm)

        # ----------------------------------------------------------------
        # 5. Line Items — incremental
        # ----------------------------------------------------------------
        logger.info("=" * 60)
        logger.info("Syncing: Line Items (incremental)")
        li_watermark = get_watermark(client, "hubspot", "line_items")

        line_items, new_li_wm = extractor.fetch_line_items_incremental(
            modified_since=li_watermark
        )

        if line_items:
            load_to_staging(client, BQ_HUBSPOT_DATASET, line_items, "_staging_hubspot_Line_Items")
            li_count = merge_staging_to_target(
                client, BQ_HUBSPOT_DATASET,
                "_staging_hubspot_Line_Items", "hubspot_Line_Items",
                id_column="id",
            )
            total_records += len(line_items)
            logger.info(f"Line Items: {len(line_items)} delta rows. Table total: {li_count}")
        else:
            logger.info("Line Items: No changes since watermark.")

        set_watermark(client, "hubspot", "line_items", new_li_wm)

        # ----------------------------------------------------------------
        # Done
        # ----------------------------------------------------------------
        log_execution(
            client, "hubspot", start_time, "SUCCESS", total_records,
            api_calls=extractor.api_call_count,
        )
        logger.info("=" * 60)
        logger.info(f"HubSpot incremental sync complete. Total records: {total_records}")

    except Exception as e:
        logger.error(f"HubSpot sync FAILED: {e}", exc_info=True)
        if client:
            try:
                log_execution(
                    client, "hubspot", start_time, "FAILED", total_records,
                    error_message=str(e),
                )
            except Exception:
                pass
        sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _extract_company_ids_from_deals(deals: list) -> Set[str]:
    ids: Set[str] = set()
    for d in deals:
        assoc = d.get("associated_company_ids") or ""
        for cid in assoc.split(","):
            cid = cid.strip()
            if cid:
                ids.add(cid)
    return ids


def _fetch_companies_by_id(
    extractor: HubSpotExtractor,
    properties: list,
    ids: Set[str],
) -> list:
    """Uses the HubSpot Batch Read API to fetch specific companies by ID."""
    from utils.api_client import APIClient

    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/read"
    all_records = []
    unique_ids = list(ids)
    chunk_size = 100

    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i + chunk_size]
        payload = {
            "properties": properties,
            "inputs": [{"id": cid} for cid in chunk],
        }
        try:
            data = extractor.post(url, headers=extractor._auth_headers, json=payload)
            for item in data.get("results", []):
                flat = item.get("properties", {}).copy()
                flat["id"] = item.get("id")
                flat["createdAt"] = item.get("createdAt")
                flat["updatedAt"] = item.get("updatedAt")
                all_records.append(flat)
        except Exception as e:
            logger.warning(f"Batch company fetch error (chunk {i}): {e}")

    return all_records


if __name__ == "__main__":
    run_hubspot_sync()
