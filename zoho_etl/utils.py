import os
import json
import logging
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# --- HELPERS ---
def clean_column_name(name):
    """Sanitizes column names for BigQuery."""
    # 1. Remove leading '$' if present (User requested to keep these columns)
    if name.startswith('$'):
        name = name[1:] 
    
    # 2. Replace invalid characters with underscore
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    # 3. Ensure it starts with a letter or underscore
    if clean_name and clean_name[0].isdigit():
        clean_name = f"_{clean_name}"
        
    return clean_name

def clean_records(records):
    """Cleans a list of dicts for BigQuery compatibility."""
    cleaned_records = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            new_key = clean_column_name(key)
            if new_key:
                # Force ID fields to string to prevent scientific notation (e.g. 9.3E+17)
                if new_key.lower().endswith("id") and value is not None:
                    new_record[new_key] = str(value)
                else:
                    new_record[new_key] = value
        cleaned_records.append(new_record)
    return cleaned_records

def flatten_data(records):
    """Flattens nested dictionary values into separate columns (e.g. Owner_name, Owner_id)."""
    flattened_list = []
    for record in records:
        flat_rec = {}
        for k, v in record.items():
            if isinstance(v, dict):
                # Flatten the dictionary
                for sk, sv in v.items():
                    flat_rec[f"{k}_{sk}"] = sv
            else:
                flat_rec[k] = v
        flattened_list.append(flat_rec)
    return flattened_list

# --- BIGQUERY HELPERS ---
def get_bq_client(credentials_json):
    credentials_info = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

def ensure_dataset_exists(client, dataset_id):
    """Checks if the dataset exists, creates it if not."""
    full_dataset_id = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(full_dataset_id)
        logging.info(f"Dataset {full_dataset_id} exists.")
    except Exception:
        logging.info(f"Dataset {full_dataset_id} not found. Creating...")
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = "US" # Default location
        client.create_dataset(dataset, timeout=30)
        logging.info(f"Created dataset {full_dataset_id}.")

def upload_to_bigquery(client, dataset_id, data, table_name):
    """Uploads a list of dictionaries to BigQuery."""
    if not data:
        logging.warning(f"No data to upload for {table_name}.")
        return

    try:
        # 1. Flatten nested dictionaries (Owner -> Owner_name, Owner_id)
        if not table_name.endswith("etl_logs"):
            data = flatten_data(data)
            
        # 2. Clean keys
        if not table_name.endswith("etl_logs"):
            data = clean_records(data)
            
        # Ensure dataset exists before first upload
        ensure_dataset_exists(client, dataset_id)
        
        # Define table reference
        table_id = f"{client.project}.{dataset_id}.{table_name}"
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # CLEANING: Remove NaNs and replace with proper SQL NULLs
        # 1. Convert everything to string to ensure uniform type
        df = df.astype(str)
        
        # 2. Replace text "nan", "None", "<NA>", and empty strings with None
        # This ensures BigQuery registers them as true SQL NULLs instead of empty strings
        df = df.replace(["nan", "None", "<NA>", ""], None)

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            write_disposition="WRITE_APPEND" if table_name in ["etl_logs", "zoho_revenue_history"] else "WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()  # Wait for the job to complete
        
        # Verify the table actually has rows
        table = client.get_table(table_id)
        logging.info(f"Loaded {len(df)} rows. BigQuery reports {table.num_rows} rows in {table_id}.")

    except Exception as e:
        logging.error(f"BigQuery Upload Failed: {e}")
        raise

def log_execution(client, dataset_id, start_time, status, total_records, error_msg="", api_calls=0):
    """Logs the execution details to a BigQuery table."""
    try:
        duration = (datetime.now() - start_time).total_seconds()
        ist_time = start_time + timedelta(hours=5, minutes=30)
        
        log_entry = {
            "timestamp": ist_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration,
            "status": status,
            "api_calls": api_calls,
            "total_records_processed": total_records,
            "error_message": str(error_msg)
        }
        
        logging.info(f"Logging execution stats: {log_entry}")
        upload_to_bigquery(client, dataset_id, [log_entry], "etl_logs")
        
    except Exception as e:
        logging.error(f"Failed to upload logs: {e}")
