"""
watermark.py — Reads and writes high-water marks in BigQuery.

A watermark is the timestamp of the most recently processed record for a
given source + module pair. On each run:
  1. Read watermark  → "last time we synced Zoho Deals"
  2. Fetch API       → only records modified AFTER that timestamp
  3. Merge into BQ   → upsert, never truncate
  4. Write watermark → store the new high-water mark

This eliminates full-table reloads. The pipeline only moves delta rows.

Table schema (pipeline_metadata.watermarks):
  source        STRING   NOT NULL   -- "zoho" or "hubspot"
  module        STRING   NOT NULL   -- "Deals", "deals", etc.
  last_synced_at TIMESTAMP NOT NULL
  updated_at    TIMESTAMP NOT NULL
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from config.settings import BQ_META_DATASET, INITIAL_LOOKBACK_DAYS, WATERMARK_OVERLAP_MINUTES

logger = logging.getLogger(__name__)

WATERMARK_TABLE = "watermarks"


def ensure_watermark_table(client: bigquery.Client) -> None:
    """Creates the watermarks table if it doesn't exist."""
    dataset_ref = client.dataset(BQ_META_DATASET)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        logger.info(f"Creating metadata dataset: {BQ_META_DATASET}")
        client.create_dataset(bigquery.Dataset(dataset_ref))

    table_id = f"{client.project}.{BQ_META_DATASET}.{WATERMARK_TABLE}"
    schema = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("module", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_synced_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.clustering_fields = ["source", "module"]
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(table)
        logger.info(f"Created watermark table: {table_id}")


def get_watermark(client: bigquery.Client, source: str, module: str) -> datetime:
    """
    Returns the last_synced_at timestamp for source+module.
    If no watermark exists (first run), returns INITIAL_LOOKBACK_DAYS ago
    so the first run becomes a full historical load.
    Subtracts WATERMARK_OVERLAP_MINUTES as a safety buffer against clock skew.
    """
    table_id = f"{client.project}.{BQ_META_DATASET}.{WATERMARK_TABLE}"
    query = f"""
        SELECT last_synced_at
        FROM `{table_id}`
        WHERE source = @source AND module = @module
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", source),
            bigquery.ScalarQueryParameter("module", "STRING", module),
        ]
    )

    try:
        rows = list(client.query(query, job_config=job_config).result())
        if rows:
            ts = rows[0].last_synced_at
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            # Apply overlap buffer to catch late-arriving/clock-skewed updates
            safe_ts = ts - timedelta(minutes=WATERMARK_OVERLAP_MINUTES)
            logger.info(
                f"Watermark [{source}.{module}]: {ts.isoformat()} "
                f"(with {WATERMARK_OVERLAP_MINUTES}m overlap → fetching from {safe_ts.isoformat()})"
            )
            return safe_ts
    except NotFound:
        pass  # Table doesn't exist yet — handled by ensure_watermark_table

    # First run — return epoch beginning to fetch all historical records
    fallback = datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
    logger.info(
        f"No watermark found for [{source}.{module}]. "
        f"First run — fetching from {fallback.isoformat()} ({INITIAL_LOOKBACK_DAYS} days back)."
    )
    return fallback


def set_watermark(
    client: bigquery.Client,
    source: str,
    module: str,
    new_watermark: datetime,
) -> None:
    """
    Upserts the watermark for source+module without DML.
    BigQuery free tier blocks MERGE/UPDATE, so we do a pandas upsert
    and WRITE_TRUNCATE the small metadata table.
    """
    table_id = f"{client.project}.{BQ_META_DATASET}.{WATERMARK_TABLE}"
    now = datetime.now(timezone.utc)

    import pandas as pd

    try:
        df = client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
    except Exception:
        df = pd.DataFrame(columns=["source", "module", "last_synced_at", "updated_at"])

    row_key = (df["source"] == source) & (df["module"] == module) if not df.empty else None
    if row_key is not None and row_key.any():
        df.loc[row_key, "last_synced_at"] = new_watermark
        df.loc[row_key, "updated_at"] = now
    else:
        new_row = pd.DataFrame([{
            "source": source,
            "module": module,
            "last_synced_at": new_watermark,
            "updated_at": now,
        }])
        df = pd.concat([df, new_row], ignore_index=True)

    df = df[["source", "module", "last_synced_at", "updated_at"]].copy()
    df["source"] = df["source"].astype(str)
    df["module"] = df["module"].astype(str)
    df["last_synced_at"] = pd.to_datetime(df["last_synced_at"], utc=True, errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    schema = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("module", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_synced_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    logger.info(f"Watermark updated [{source}.{module}] → {new_watermark.isoformat()}")
