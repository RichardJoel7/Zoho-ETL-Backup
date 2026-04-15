# Incremental Pipeline — Complete Setup Guide

## Overview
This guide sets up the new incremental pipeline alongside your existing pipeline.
Your current pipeline (zoho_sync.py, hubspot_sync.py, combined_etl.yml) is
**not touched at any point**. Both pipelines run independently.

---

## Step 1 — Add the folder to your repository

In TRAE IDE, open your existing project (the one with zoho_sync.py).
The new `Incremental_Pipeline/` folder should sit at the **same level** as your
existing scripts:

```
your-repo/
├── zoho_sync.py              ← EXISTING — do not touch
├── hubspot_sync.py           ← EXISTING — do not touch
├── combined_etl.yml          ← EXISTING — do not touch
├── utils.py                  ← EXISTING — do not touch
├── requirements.txt          ← EXISTING — do not touch
└── Incremental_Pipeline/     ← NEW folder
    ├── config/
    ├── extractors/
    ├── utils/
    ├── tests/
    ├── zoho_sync.py
    ├── hubspot_sync.py
    ├── requirements.txt
    └── .github/workflows/incremental_etl.yml
```

Copy the `Incremental_Pipeline/` folder into your repo root.

---

## Step 2 — Move the GitHub Actions workflow file

The workflow file must live in `.github/workflows/` at the **repo root**,
not inside the `Incremental_Pipeline/` subfolder.

```bash
# From repo root:
cp Incremental_Pipeline/.github/workflows/incremental_etl.yml \
   .github/workflows/incremental_etl.yml
```

Your existing `combined_etl.yml` stays untouched. Both workflows will
run independently.

---

## Step 3 — Create BigQuery datasets

Open Google Cloud Console → BigQuery → your project.
Create these three datasets (all in US region):

| Dataset name           | Description                        |
|------------------------|------------------------------------|
| `zoho_incremental`     | New Zoho tables (upserted)         |
| `hubspot_incremental`  | New HubSpot tables (upserted)      |
| `pipeline_metadata`    | Watermarks + run logs              |

**Via bq CLI (faster):**
```bash
bq mk --location=US --dataset YOUR_PROJECT_ID:zoho_incremental
bq mk --location=US --dataset YOUR_PROJECT_ID:hubspot_incremental
bq mk --location=US --dataset YOUR_PROJECT_ID:pipeline_metadata
```

The pipeline will auto-create all tables on first run.

---

## Step 4 — Verify GitHub Secrets

Your existing secrets already cover everything this pipeline needs.
Go to: GitHub → your repo → Settings → Secrets and variables → Actions

Confirm these secrets exist (they should — the old pipeline uses them):

| Secret name             | Notes                                |
|-------------------------|--------------------------------------|
| `ZOHO_CLIENT_ID`        | Same as legacy                       |
| `ZOHO_CLIENT_SECRET`    | Same as legacy                       |
| `ZOHO_REFRESH_TOKEN`    | Same as legacy                       |
| `ZOHO_DOMAIN`           | Usually `com`                        |
| `HUBSPOT_ACCESS_TOKEN`  | Same as legacy                       |
| `GCP_CREDENTIALS_JSON`  | Same as legacy — full JSON string    |
| `GCP_PROJECT_ID`        | Same as legacy                       |

No new secrets needed.

---

## Step 5 — Update config/settings.py with your project ID

Open `Incremental_Pipeline/config/settings.py`.
The `GCP_PROJECT_ID` is read from the `GCP_PROJECT_ID` environment variable
automatically — no change needed if that secret is set.

If you want to hardcode it for local dev only, change:
```python
BQ_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "your-project-id-here")
```

---

## Step 6 — Local test run (optional but recommended)

```bash
cd Incremental_Pipeline
pip install -r requirements.txt

# Run unit tests first — no credentials needed
pytest tests/ -v

# For a real test run locally:
cp .env.example .env
# Fill in .env with your actual credentials
# Then run:
python zoho_sync.py
```

On first run, all tables are created automatically and a full historical
load runs (INITIAL_LOOKBACK_DAYS = 3650 days).

---

## Step 7 — Trigger the first run manually

After pushing the new folder to GitHub:

1. Go to GitHub → your repo → Actions tab
2. Find "Incremental CRM → BigQuery ETL"
3. Click "Run workflow" → choose "both"
4. Watch both jobs run in parallel

The first run will be slow (full historical load). All subsequent runs
will be fast (delta only).

---

## Step 8 — Connect BI tools to the new datasets

Once data is flowing, create views in BigQuery that your BI tools query.
Recommended pattern — create a `reporting` dataset with views that JOIN
both pipelines:

```sql
-- Example: unified deals view across both CRMs
CREATE OR REPLACE VIEW `your_project.reporting.all_deals` AS

SELECT
  id,
  dealname AS name,
  amount,
  dealstage AS stage,
  'hubspot' AS source,
  TIMESTAMP(updatedAt) AS last_modified
FROM `your_project.hubspot_incremental.hubspot_Deals`

UNION ALL

SELECT
  id,
  Subject AS name,
  Amount AS amount,
  Stage AS stage,
  'zoho' AS source,
  TIMESTAMP(Modified_Time) AS last_modified
FROM `your_project.zoho_incremental.zoho_Deals`
```

Point Looker / AppScript / Zoho Analytics at these views instead of raw tables.

---

## Step 9 — Monitor the pipeline

Check these tables after each run:

```sql
-- Run log: status, duration, records per run
SELECT source, status, records_processed, duration_seconds, started_at
FROM `your_project.pipeline_metadata.load_log`
ORDER BY started_at DESC
LIMIT 20;

-- Watermark state: how current is each module
SELECT source, module, last_synced_at, updated_at
FROM `your_project.pipeline_metadata.watermarks`
ORDER BY source, module;
```

---

## Troubleshooting

**"No changes since watermark" on every run**
→ Check the watermarks table — if last_synced_at is in the future, manually
  delete the row and let it reset: `DELETE FROM pipeline_metadata.watermarks WHERE module = 'Deals'`

**HubSpot Search API returns 0 results but you know records changed**
→ HubSpot Search has a ~1 min indexing delay. The 15-minute overlap in
  watermark.py handles this. If still missing records, increase
  WATERMARK_OVERLAP_MINUTES in settings.py to 30.

**Zoho 304 Not Modified on all modules**
→ This is correct behaviour — it means no records changed. Not an error.

**BQ MERGE fails with "column not found"**
→ A new custom field was added to your CRM. Re-run — the schema inferencer
  will pick it up and add the column automatically on the next staging load.

**First run is very slow**
→ Expected. INITIAL_LOOKBACK_DAYS = 3650 means it's loading 10 years of data.
  Subsequent runs only process deltas.
