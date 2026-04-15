# TRAE IDE — Project Context Prompt
# Paste this into TRAE's project instructions / system prompt field.
# This gives TRAE full context so it can assist you correctly.

---

## Project: Incremental CRM → BigQuery Pipeline

This is a **production-grade incremental ETL pipeline** that syncs Zoho CRM and HubSpot CRM data into Google BigQuery. It runs on GitHub Actions twice daily.

### What "incremental" means here
Every sync fetches ONLY records modified since the last successful run, using:
- **Zoho**: `If-Modified-Since` header on CRM v2.1 API
- **HubSpot**: Search API with `lastmodifieddate >= watermark` filter
Watermarks are stored in BigQuery table `pipeline_metadata.watermarks`.

### Project structure
```
Incremental_Pipeline/
├── config/
│   └── settings.py          ← All env vars, dataset names, retry config
├── extractors/
│   ├── zoho_extractor.py    ← Zoho auth + incremental fetch
│   └── hubspot_extractor.py ← HubSpot incremental fetch via Search API
├── utils/
│   ├── api_client.py        ← Retry-hardened HTTP base class (tenacity)
│   ├── bq_loader.py         ← BQ client, staging load, MERGE upsert, logging
│   └── watermark.py         ← Read/write watermarks in BQ
├── tests/
│   └── test_pipeline.py     ← Unit tests (no real API/BQ calls)
├── zoho_sync.py             ← Zoho orchestrator (run this directly)
├── hubspot_sync.py          ← HubSpot orchestrator (run this directly)
├── requirements.txt
└── .github/workflows/
    └── incremental_etl.yml  ← Parallel GitHub Actions jobs
```

### BigQuery datasets (SEPARATE from legacy pipeline)
- `zoho_incremental`     — Zoho tables (zoho_Deals, zoho_Blocks, etc.)
- `hubspot_incremental`  — HubSpot tables (hubspot_Deals, hubspot_Companies, etc.)
- `pipeline_metadata`    — watermarks table + load_log table

### Load pattern
1. Fetch delta records from API
2. WRITE_TRUNCATE into `_staging_{table}` (transient)
3. MERGE `_staging_{table}` → `{table}` (upsert by `id`)
4. Update watermark in `pipeline_metadata.watermarks`

### Key design rules
- NEVER modify zoho_raw_data or hubspot_raw_data datasets
- NEVER use WRITE_TRUNCATE on target tables (only on staging)
- ALL API calls go through `utils/api_client.py` (retry + backoff)
- Watermarks live in BQ, not in files or env vars
- `validate_config()` must be called at the top of every main() function

### Running locally
```bash
cd Incremental_Pipeline
cp .env.example .env        # fill in credentials
pip install -r requirements.txt
python zoho_sync.py         # or python hubspot_sync.py
pytest tests/ -v            # run unit tests
```

### Adding a new Zoho module
1. Add `"ModuleName": "zoho_TableName"` to `ZOHO_MODULES` in `config/settings.py`
2. No other changes needed — the sync loop handles it automatically

### Adding a new HubSpot object
1. Add fetch logic in `hubspot_extractor.py`
2. Add sync block in `hubspot_sync.py` following the existing pattern

### Do NOT
- Add debug/investigation code to production scripts
- Use global mutable variables
- Catch Exception and silently pass without logging
- Import from the legacy pipeline (zoho_sync.py / hubspot_sync.py in root)
