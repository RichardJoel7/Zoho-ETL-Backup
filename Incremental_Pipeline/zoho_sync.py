"""
zoho_sync.py — Incremental Zoho → BigQuery sync.

Orchestration flow per module:
  1. Read watermark from pipeline_metadata.watermarks
  2. Fetch only records modified after watermark (delta)
  3. Load delta records into staging table (WRITE_TRUNCATE on staging only)
  4. MERGE staging → target table (upsert — no data loss)
  5. Write new watermark
  6. Log execution result

Writes to: zoho_incremental dataset (NEVER touches zoho_raw_data)
"""

import logging
import sys
from datetime import datetime

from config.settings import (
    ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN, ZOHO_DOMAIN,
    GCP_CREDENTIALS_JSON, BQ_ZOHO_DATASET, ZOHO_MODULES, validate_config,
    IST,
)
from extractors.zoho_extractor import ZohoExtractor
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
logger = logging.getLogger("zoho_sync")


def run_zoho_sync() -> None:
    start_time = datetime.now(IST)
    total_records = 0
    client = None

    try:
        validate_config()
        client = get_bq_client(GCP_CREDENTIALS_JSON)

        # Ensure all required datasets and metadata tables exist
        ensure_dataset_exists(client, BQ_ZOHO_DATASET)
        ensure_watermark_table(client)

        # Authenticate once
        extractor = ZohoExtractor(
            client_id=ZOHO_CLIENT_ID,
            client_secret=ZOHO_CLIENT_SECRET,
            refresh_token=ZOHO_REFRESH_TOKEN,
            domain=ZOHO_DOMAIN,
        )
        extractor.authenticate()

        # Discover API names for configured modules
        api_name_map = extractor.discover_module_api_names(list(ZOHO_MODULES.keys()))

        # ----------------------------------------------------------------
        # Per-module incremental sync
        # ----------------------------------------------------------------
        block_delta_records = []
        block_new_watermark = None

        for module_label, target_table in ZOHO_MODULES.items():
            api_name = api_name_map.get(module_label, module_label)
            logger.info(f"{'='*60}")
            logger.info(f"Processing module: {module_label} (API: {api_name})")

            # 1. Read watermark
            watermark = get_watermark(client, source="zoho", module=module_label)

            # 2. Fetch delta
            try:
                records, new_watermark = extractor.fetch_module_incremental(
                    module_api_name=api_name,
                    modified_since=watermark,
                )
            except Exception as e:
                if module_label == "Site Splits" and block_delta_records:
                    logger.warning(f"Site Splits API fetch failed. Using Blocks subform fallback: {e}")
                    records = _extract_site_splits_from_blocks(block_delta_records)
                    new_watermark = block_new_watermark or datetime.now(IST)
                else:
                    logger.warning(f"Skipping {module_label} due to API error: {e}")
                    continue

            if not records:
                logger.info(f"{module_label}: 0 new/changed records — skipping BQ write.")
                # Still advance watermark to now so next run window stays tight
                set_watermark(client, "zoho", module_label, datetime.now(IST))
                continue

            total_records += len(records)

            if module_label == "Blocks":
                block_delta_records = records
                block_new_watermark = new_watermark

            # 3. Revenue history tracking (delta computation in Python, write via MERGE)
            if module_label == "Revenue Recognition":
                _handle_revenue_history(client, extractor, records)

            # 4. Stage
            staging_table = f"_staging_{target_table}"
            load_to_staging(client, BQ_ZOHO_DATASET, records, staging_table)

            # 5. MERGE into target
            merged_count = merge_staging_to_target(
                client, BQ_ZOHO_DATASET, staging_table, target_table, id_column="id"
            )

            # 6. Validate
            validate_row_count(client, BQ_ZOHO_DATASET, target_table, expected_min=1)

            # 7. Advance watermark
            set_watermark(client, "zoho", module_label, new_watermark)

            logger.info(
                f"{module_label}: Done. {len(records)} delta rows processed. "
                f"Target table has {merged_count} total rows."
            )

        log_execution(
            client, "zoho", start_time, "SUCCESS", total_records,
            api_calls=extractor.api_call_count,
        )
        logger.info(f"{'='*60}")
        logger.info(f"Zoho incremental sync complete. Total records processed: {total_records}")

    except Exception as e:
        logger.error(f"Zoho sync FAILED: {e}", exc_info=True)
        if client:
            try:
                log_execution(
                    client, "zoho", start_time, "FAILED", total_records,
                    error_message=str(e),
                )
            except Exception:
                pass
        sys.exit(1)


def _handle_revenue_history(client, extractor: ZohoExtractor, current_records: list) -> None:
    """Loads previous revenue snapshot from BQ, computes delta, writes history."""
    from utils.bq_loader import load_to_staging, merge_staging_to_target

    # Fetch the latest snapshot of revenue records from the target table
    target_id = f"{client.project}.{BQ_ZOHO_DATASET}.zoho_Revenue_Recognition"
    previous_snapshot: dict = {}

    try:
        query = f"""
            WITH ranked AS (
                SELECT id, amount, Revenue_date,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY _PARTITIONTIME DESC) AS rn
                FROM `{target_id}`
            )
            SELECT id, amount, Revenue_date FROM ranked WHERE rn = 1 AND id IS NOT NULL
        """
        rows = list(client.query(query).result())
        for row in rows:
            previous_snapshot[str(row.id)] = {
                "amount": row.amount,
                "Revenue_date": row.Revenue_date,
            }
        logger.info(f"Revenue history: loaded {len(previous_snapshot)} previous records from BQ.")
    except Exception as e:
        logger.warning(f"Could not load previous revenue snapshot (first run?): {e}")

    history_records = extractor.build_revenue_history_delta(current_records, previous_snapshot)

    if history_records:
        load_to_staging(client, BQ_ZOHO_DATASET, history_records, "_staging_zoho_revenue_history")
        merge_staging_to_target(
            client, BQ_ZOHO_DATASET,
            "_staging_zoho_revenue_history", "zoho_revenue_history",
            id_column="action_id",
        )
        logger.info(f"Revenue history: {len(history_records)} change records merged.")


def _extract_site_splits_from_blocks(block_records: list) -> list:
    site_splits_records = []
    for block in block_records:
        block_id = str(block.get("id", ""))
        block_name = str(block.get("Name") or block.get("Block_Name") or block.get("name") or "")
        for value in block.values():
            if isinstance(value, list) and value:
                first_item = value[0]
                if isinstance(first_item, dict) and ("Site_Percentage" in first_item or "Site" in first_item):
                    for split in value:
                        split_copy = dict(split)
                        split_copy["Parent_Id_id"] = block_id
                        split_copy["Parent_Id_name"] = block_name
                        site_splits_records.append(split_copy)
                    break
    return site_splits_records


if __name__ == "__main__":
    run_zoho_sync()
