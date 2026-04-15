"""
settings.py — Central configuration for the Incremental Pipeline.
All environment variables, dataset names, table names, and retry settings live here.
Touch this file only — nothing else needs to change when you move between environments.
"""

import os
from datetime import timezone, timedelta
from typing import Optional
from dotenv import load_dotenv

IST = timezone(timedelta(hours=5, minutes=30))
load_dotenv()


# ---------------------------------------------------------------------------
# BigQuery — SEPARATE project/dataset from the legacy pipeline
# ---------------------------------------------------------------------------
BQ_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
BQ_ZOHO_DATASET: str = "zoho_incremental"        # NEW — never touches zoho_raw_data
BQ_HUBSPOT_DATASET: str = "hubspot_incremental"  # NEW — never touches hubspot_raw_data
BQ_META_DATASET: str = "pipeline_metadata"       # Watermarks, run logs, data quality

# ---------------------------------------------------------------------------
# Zoho credentials
# ---------------------------------------------------------------------------
ZOHO_CLIENT_ID: str = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET: str = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN: str = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_DOMAIN: str = os.getenv("ZOHO_DOMAIN", "com")

# Zoho modules to sync and their BQ target table names
ZOHO_MODULES: dict = {
    "Accounts": "zoho_Accounts",
    "Deals": "zoho_Deals",
    "Blocks": "zoho_Blocks",
    "Site Splits": "zoho_Site_Splits",
    "Revenue Recognition": "zoho_Revenue_Recognition",
    "Stage History": "zoho_DealHistory",
}

# ---------------------------------------------------------------------------
# HubSpot credentials
# ---------------------------------------------------------------------------
HUBSPOT_ACCESS_TOKEN: str = os.getenv("HUBSPOT_ACCESS_TOKEN", "")

# HubSpot objects to sync
HUBSPOT_OBJECTS: dict = {
    "deals": "hubspot_Deals",
    "companies": "hubspot_Companies",
}

# ---------------------------------------------------------------------------
# GCP Service Account (passed as JSON string in CI/CD)
# ---------------------------------------------------------------------------
GCP_CREDENTIALS_JSON: Optional[str] = os.getenv("GCP_CREDENTIALS_JSON")

# ---------------------------------------------------------------------------
# Retry / resilience settings (used by tenacity in api_client.py)
# ---------------------------------------------------------------------------
RETRY_MAX_ATTEMPTS: int = 5
RETRY_WAIT_MIN_SECONDS: float = 2.0
RETRY_WAIT_MAX_SECONDS: float = 60.0
RETRY_MULTIPLIER: float = 2.0          # Exponential backoff multiplier

# ---------------------------------------------------------------------------
# Incremental watermark settings
# ---------------------------------------------------------------------------
# How far back to look on the very first run (days).
# After the first run, the watermark table drives the window.
INITIAL_LOOKBACK_DAYS: int = 3650      # ~10 years = full historical load on first run

# Safety overlap: re-fetch records modified in the last N minutes before
# the stored watermark to catch any clock-skew or late-arriving updates.
WATERMARK_OVERLAP_MINUTES: int = 15

# ---------------------------------------------------------------------------
# BigQuery load settings
# ---------------------------------------------------------------------------
BQ_WRITE_DISPOSITION: str = "WRITE_TRUNCATE"   # For staging tables
BQ_BATCH_SIZE: int = 500                        # Rows per BQ streaming insert

# ---------------------------------------------------------------------------
# Validation guards
# ---------------------------------------------------------------------------
def validate_config() -> None:
    """Raises ValueError immediately if any required credential is missing."""
    required = {
        "GCP_PROJECT_ID": BQ_PROJECT_ID,
        "ZOHO_CLIENT_ID": ZOHO_CLIENT_ID,
        "ZOHO_CLIENT_SECRET": ZOHO_CLIENT_SECRET,
        "ZOHO_REFRESH_TOKEN": ZOHO_REFRESH_TOKEN,
        "HUBSPOT_ACCESS_TOKEN": HUBSPOT_ACCESS_TOKEN,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Set them in GitHub Secrets or your .env file."
        )
