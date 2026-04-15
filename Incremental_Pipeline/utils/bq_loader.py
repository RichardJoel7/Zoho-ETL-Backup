"""
bq_loader.py — Production-grade BigQuery loader.

Key differences from the legacy utils.py:
  - Uses Pandas-based UPSERT to bypass BigQuery Free Tier DML restrictions
  - Schema auto-detection with safe type coercion
  - Dataset existence check (idempotent)
  - Row-count validation after every load
  - Structured load logs written to pipeline_metadata.load_log
"""

import json
import logging
import os
import tempfile
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from config.settings import BQ_META_DATASET

logger = logging.getLogger(__name__)

LOAD_LOG_TABLE = "load_log"


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------
def get_bq_client(credentials_json: Optional[str] = None) -> bigquery.Client:
    """
    Returns an authenticated BigQuery client.
    Prefers credentials_json string (GitHub Secret) over ADC for CI/CD compatibility.
    Falls back to Application Default Credentials for local dev.
    """
    project_id = os.getenv("GCP_PROJECT_ID", "")

    if credentials_json:
        try:
            creds_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            return bigquery.Client(project=project_id, credentials=credentials)
        except (json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Invalid GCP_CREDENTIALS_JSON: {e}") from e

    # Local development — uses `gcloud auth application-default login`
    logger.info("No credentials JSON provided — using Application Default Credentials.")
    return bigquery.Client(project=project_id)


# ---------------------------------------------------------------------------
# Dataset management
# ---------------------------------------------------------------------------
def ensure_dataset_exists(client: bigquery.Client, dataset_id: str) -> None:
    """Creates the dataset if it doesn't exist. Idempotent."""
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info(f"Created BigQuery dataset: {dataset_id}")


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------
def _infer_schema(records: List[Dict]) -> List[bigquery.SchemaField]:
    """
    Infers a BigQuery schema from a list of dicts.
    Samples the first 100 rows for type detection.
    All unknown/mixed types default to STRING (safe fallback).
    """
    type_map: Dict[str, str] = {}
    sample = records[:100]

    for row in sample:
        for key, value in row.items():
            if value is None:
                continue
            # Thorough sanitization: replace $ and other invalid chars
            col = key.replace("$", "").replace("-", "_").replace(".", "_").replace(" ", "_")
            if col and col[0].isdigit():
                col = f"_{col}"
                
            detected = _detect_type(value)
            existing = type_map.get(col)
            if existing is None:
                type_map[col] = detected
            elif existing != detected:
                type_map[col] = "STRING"  # Type conflict → fallback to STRING

    # Any key not sampled (all-null column) → STRING
    all_keys = set()
    for row in records:
        for k in row.keys():
            col = k.replace("$", "").replace("-", "_").replace(".", "_").replace(" ", "_")
            if col and col[0].isdigit():
                col = f"_{col}"
            all_keys.add(col)
            
    for col in all_keys:
        if col not in type_map:
            type_map[col] = "STRING"

    return [
        bigquery.SchemaField(col, bq_type, mode="NULLABLE")
        for col, bq_type in sorted(type_map.items())
    ]


def _detect_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, dict):
        return "STRING"  # Serialize nested dicts as JSON strings
    if isinstance(value, list):
        return "STRING"  # Serialize lists as JSON strings
    return "STRING"


# ---------------------------------------------------------------------------
# Row normalisation
# ---------------------------------------------------------------------------
def flatten_data(records: List[Dict]) -> List[Dict]:
    """
    Flattens top-level nested dictionaries into separate columns.
    Example: {"Owner": {"name": "A", "id": "1"}} -> {"Owner_name": "A", "Owner_id": "1"}
    """
    flattened_list: List[Dict] = []
    for record in records:
        flat_rec: Dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, dict):
                for sk, sv in v.items():
                    flat_rec[f"{k}_{sk}"] = sv
            else:
                flat_rec[k] = v
        flattened_list.append(flat_rec)
    return flattened_list


def _normalise_row(row: Dict) -> Dict:
    """
    Coerces values to BQ-safe types and sanitises column names.
    - Dicts and lists → JSON strings
    - Column names: replace special chars with underscore
    - Strip None values from keys with None to avoid BQ insert errors
    """
    clean = {}
    for k, v in row.items():
        safe_key = k.replace("-", "_").replace(".", "_").replace(" ", "_")
        if isinstance(v, (dict, list)):
            clean[safe_key] = json.dumps(v, default=str)
        elif isinstance(v, bool):
            clean[safe_key] = v
        elif v is None:
            clean[safe_key] = None
        else:
            clean[safe_key] = str(v) if not isinstance(v, (int, float)) else v
    return clean


# ---------------------------------------------------------------------------
# Staging load (for MERGE workflow)
# ---------------------------------------------------------------------------
def load_to_staging(
    client: bigquery.Client,
    dataset_id: str,
    records: List[Dict],
    staging_table: str,
) -> int:
    """
    Loads records into a staging table using WRITE_TRUNCATE.
    The staging table is always fully replaced — it's transient, used only
    for the subsequent MERGE. Returns row count loaded.
    """
    if not records:
        logger.info(f"No records to load into staging table {staging_table}.")
        return 0

    # 1) Flatten nested dictionaries to production-style columns
    records = flatten_data(records)

    # 2) Clean keys before we try to infer schema or load
    cleaned_records = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            # Apply exact same sanitization logic as schema inference
            col = k.replace("$", "").replace("-", "_").replace(".", "_").replace(" ", "_")
            if col and col[0].isdigit():
                col = f"_{col}"
            clean_row[col] = v
        cleaned_records.append(clean_row)

    records = cleaned_records

    normalised = [_normalise_row(r) for r in records]
    table_id = f"{client.project}.{dataset_id}.{staging_table}"

    # Write via NDJSON file for large payloads (avoids streaming insert quotas)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        for row in normalised:
            f.write(json.dumps(row, default=str) + "\n")
        tmp_path = f.name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        ignore_unknown_values=True,
        max_bad_records=10,
    )

    try:
        with open(tmp_path, "rb") as f:
            load_job = client.load_table_from_file(f, table_id, job_config=job_config)
        load_job.result()

        table = client.get_table(table_id)
        loaded = int(table.num_rows or 0)
        logger.info(f"Staging load complete: {table_id} — {loaded} rows")
        return loaded
    except Exception as e:
        logger.error(f"Staging load failed for {table_id}: {e}")
        raise
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# MERGE — upsert staging → target
# ---------------------------------------------------------------------------
def merge_staging_to_target(
    client: bigquery.Client,
    dataset_id: str,
    staging_table: str,
    target_table: str,
    id_column: str = "id",
) -> int:
    """
    Simulates a SQL MERGE by downloading the target table to a Pandas DataFrame,
    updating it with records from the staging table, and then re-uploading via WRITE_TRUNCATE.
    This bypasses the BigQuery Free Tier DML restrictions (no UPDATE/MERGE allowed).
    """
    staging_id = f"{client.project}.{dataset_id}.{staging_table}"
    target_id = f"{client.project}.{dataset_id}.{target_table}"

    # 1. Ensure target table exists (if not, just copy staging to target)
    try:
        client.get_table(target_id)
        target_exists = True
    except NotFound:
        target_exists = False
        logger.info(f"Target table {target_id} not found. Creating from staging.")

    if not target_exists:
        job_config = bigquery.QueryJobConfig(destination=target_id)
        sql = f"SELECT * FROM `{staging_id}`"
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        
        # Get count
        final_count = list(client.query(f"SELECT COUNT(1) as cnt FROM `{target_id}`").result())[0].cnt
        logger.info(f"Created target table: {target_id} with {final_count} rows.")
        return final_count

    # 2. Target exists. Let's do a Pandas Upsert.
    logger.info(f"Target table {target_id} exists. Performing Pandas-based Upsert...")
    
    try:
        # Download Staging
        df_staging = client.query(f"SELECT * FROM `{staging_id}`").to_dataframe()
        if df_staging.empty:
            logger.info("Staging is empty. Nothing to merge.")
            return 0
            
        # Download Target
        df_target = client.query(f"SELECT * FROM `{target_id}`").to_dataframe()
        
        if df_target.empty:
            df_final = df_staging
        else:
            # UPSERT LOGIC:
            # 1. Ensure id_column is string for safe comparison
            if id_column in df_staging.columns:
                df_staging[id_column] = df_staging[id_column].astype(str)
            if id_column in df_target.columns:
                df_target[id_column] = df_target[id_column].astype(str)
                
            # 2. Set index to ID
            df_target.set_index(id_column, inplace=True, drop=False)
            df_staging.set_index(id_column, inplace=True, drop=False)
            
            # 3. Combine them. update() modifies in-place matching indices. 
            # combine_first() fills nulls. We want to completely replace old rows with new rows.
            # The cleanest way: append staging to target, then drop duplicates keeping the LAST (which is from staging).
            df_combined = pd.concat([df_target, df_staging])
            # For deals where the ID is the same, keep the one from staging (the newest one)
            df_final = df_combined[~df_combined.index.duplicated(keep='last')]
            df_final.reset_index(drop=True, inplace=True)

        # 4. Upload back to Target via WRITE_TRUNCATE (allowed on Free Tier)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        
        # Convert any lingering Pandas NA/NaT to standard Python None for BQ
        df_final = df_final.where(pd.notnull(df_final), None)
        object_cols = df_final.select_dtypes(include=["object"]).columns
        for col in object_cols:
            df_final[col] = df_final[col].apply(lambda v: None if v is None else str(v))
        
        load_job = client.load_table_from_dataframe(df_final, target_id, job_config=job_config)
        load_job.result()
        
        # 5. Verify Row Count
        table = client.get_table(target_id)
        final_rows = int(table.num_rows or 0)
        logger.info(f"Pandas Upsert Complete: {target_id} now has {final_rows} rows.")
        return final_rows
        
    except Exception as e:
        logger.error(f"Pandas Upsert failed for {target_table}: {e}")
        raise


def _ensure_target_table_exists(
    client: bigquery.Client,
    staging_id: str,
    target_id: str,
    schema: List[bigquery.SchemaField],
) -> None:
    """Creates the target table from the staging schema if it doesn't exist yet."""
    try:
        client.get_table(target_id)
    except NotFound:
        table = bigquery.Table(target_id, schema=schema)
        client.create_table(table)
        logger.info(f"Created target table: {target_id}")


def _copy_staging_to_target(
    client: bigquery.Client, staging_id: str, target_id: str
) -> None:
    """Full copy fallback when ID column is absent."""
    copy_sql = f"""
        CREATE OR REPLACE TABLE `{target_id}` AS
        SELECT * FROM `{staging_id}`
    """
    client.query(copy_sql).result()


# ---------------------------------------------------------------------------
# Data quality check
# ---------------------------------------------------------------------------
def validate_row_count(
    client: bigquery.Client,
    dataset_id: str,
    table_name: str,
    expected_min: int,
) -> bool:
    """
    Warns (does not fail) if actual row count is below expected_min.
    Returns True if healthy, False if suspicious.
    """
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    try:
        actual = int(client.get_table(table_id).num_rows or 0)
        if actual < expected_min:
            logger.warning(
                f"DATA QUALITY WARNING: {table_name} has {actual} rows "
                f"but expected at least {expected_min}."
            )
            return False
        return True
    except Exception as e:
        logger.warning(f"Row count check failed for {table_name}: {e}")
        return False


# ---------------------------------------------------------------------------
# Execution log
# ---------------------------------------------------------------------------
def log_execution(
    client: bigquery.Client,
    source: str,
    start_time: datetime,
    status: str,
    records_processed: int,
    error_message: Optional[str] = None,
    api_calls: int = 0,
) -> None:
    """Writes a structured run record to pipeline_metadata.load_log."""
    ensure_dataset_exists(client, BQ_META_DATASET)
    table_id = f"{client.project}.{BQ_META_DATASET}.{LOAD_LOG_TABLE}"

    end_time = datetime.now(timezone.utc)
    duration_seconds = (end_time - start_time).total_seconds()

    # Ensure dates are stored as actual datetime objects for pandas to pyarrow conversion
    row = {
        "source": source,
        "status": status,
        "records_processed": int(records_processed),
        "api_calls": int(api_calls),
        "duration_seconds": float(duration_seconds),
        "error_message": str(error_message or ""),
        "started_at": start_time,
        "finished_at": end_time,
    }

    schema = [
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("records_processed", "INTEGER"),
        bigquery.SchemaField("api_calls", "INTEGER"),
        bigquery.SchemaField("duration_seconds", "FLOAT"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:
        client.get_table(table_id)
    except NotFound:
        client.create_table(table)

    # Upload via LoadJobConfig instead of streaming insert (bypasses Free Tier limits)
    try:
        import pandas as pd
        df = pd.DataFrame([row])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        logger.info(
            f"Run log written: source={source} status={status} "
            f"records={records_processed} duration={duration_seconds:.1f}s"
        )
    except Exception as e:
        logger.error(f"Failed to log execution: {e}")
