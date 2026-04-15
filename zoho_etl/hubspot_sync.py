import os
import requests
import logging
import json
from datetime import datetime
from utils import get_bq_client, upload_to_bigquery, log_execution, ensure_dataset_exists

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
GCP_CREDENTIALS_JSON = os.getenv("GCP_CREDENTIALS_JSON")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_ID = "hubspot_raw_data" 

API_CALLS = 0

def get_all_properties(object_type):
    """
    Dynamically fetches ALL property internal names for a given object type (deals, companies).
    This ensures we get every custom field without hardcoding.
    """
    global API_CALLS
    url = f"https://api.hubapi.com/crm/v3/properties/{object_type}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    properties = []
    try:
        logging.info(f"Discovering properties for {object_type}...")
        response = requests.get(url, headers=headers)
        API_CALLS += 1
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        for prop in results:
            if not prop.get("hidden"): # Optional: skip hidden system fields
                properties.append(prop["name"])
                
        # CONFIRMATION: Check for the 'In Bidding' field explicitly
        if "bidding" in properties:
             logging.info("SUCCESS: Found 'In Bidding' checkbox (Internal Name: 'bidding'). It will be synced to BigQuery column 'bidding'.")
        else:
             logging.warning("WARNING: 'In Bidding' field (internal name 'bidding') NOT found.")

        logging.info(f"Discovered {len(properties)} properties for {object_type}.")
        
    except Exception as e:
        logging.error(f"Property Discovery Failed: {e}")
        # Fallback to defaults if discovery fails to prevent crash
        if object_type == "deals":
            return ["dealname", "amount", "dealstage", "pipeline", "closedate"]
        elif object_type == "companies":
            return ["name", "domain"]
            
    return properties

def fetch_pipelines():
    """Fetches Pipelines and Stages metadata for mapping IDs to Labels."""
    global API_CALLS
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    pipeline_data = []
    
    try:
        logging.info("Fetching Pipeline Definitions...")
        response = requests.get(url, headers=headers)
        API_CALLS += 1
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        for pipe in results:
            p_id = pipe.get("id")
            p_label = pipe.get("label")
            
            # Flatten stages
            for stage in pipe.get("stages", []):
                pipeline_data.append({
                    "pipeline_id": p_id,
                    "pipeline_label": p_label,
                    "stage_id": stage.get("id"),
                    "stage_label": stage.get("label"),
                    "display_order": stage.get("displayOrder")
                })
                
    except Exception as e:
        logging.error(f"Pipeline Fetch Error: {e}")
        
    return pipeline_data

def fetch_hubspot_data(object_type, properties, associations=None):
    """Generic fetcher for Deals and Companies."""
    global API_CALLS
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    all_records = []
    after = None
    
    logging.info(f"Starting Fetch for {object_type}...")
    
    while True:
        params = {"limit": 100, "properties": properties}
        if associations:
            params["associations"] = associations
            
        if after:
            params["after"] = after
            
        try:
            response = requests.get(url, headers=headers, params=params)
            API_CALLS += 1
            response.raise_for_status()
            data = response.json()
            
            results = data.get("results", [])
            for item in results:
                flat = item.get("properties", {}).copy()
                flat["id"] = item.get("id")
                flat["createdAt"] = item.get("createdAt")
                flat["updatedAt"] = item.get("updatedAt")
                flat["archived"] = item.get("archived")
                
                # Extract Associations (Companies specifically for Deals)
                if object_type == "deals":
                    company_assocs = item.get("associations", {}).get("companies", {}).get("results", [])
                    # Store as comma-separated string for easy SQL parsing or just JSON string
                    # Creating a simple list of IDs is usually safest for BQ string
                    ids = [assoc.get("id") for assoc in company_assocs if assoc.get("id")]
                    # Deduplicate IDs
                    unique_ids = sorted(list(set(ids)))
                    flat["associated_company_ids"] = ",".join(unique_ids) if unique_ids else None
                    
                all_records.append(flat)
                
            logging.info(f"Fetched {len(results)} {object_type}. Total: {len(all_records)}")
            
            paging = data.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")
            
            if not after:
                break
                
        except Exception as e:
            logging.error(f"Fetch Error ({object_type}): {e}")
            raise
            
    return all_records

def fetch_hubspot_owners():
    """Fetches all owners (Active AND Archived/Deactivated)."""
    global API_CALLS
    url = "https://api.hubapi.com/crm/v3/owners"
    headers = {"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"}
    
    all_owners = []
    
    # We must make two separate passes: one for active, one for archived.
    for is_archived in [False, True]:
        after = None
        logging.info(f"Fetching {'Archived' if is_archived else 'Active'} Owners...")
        
        while True:
            params = {"limit": 100, "archived": str(is_archived).lower()}
            if after: params["after"] = after
            
            try:
                response = requests.get(url, headers=headers, params=params)
                API_CALLS += 1
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                
                for owner in results:
                    owner_rec = owner.copy()
                    if owner.get("firstName") and owner.get("lastName"):
                        owner_rec["fullName"] = f"{owner.get('firstName')} {owner.get('lastName')}"
                    
                    # Mark as archived explicitly if not present, for clarity in BQ
                    owner_rec["archived"] = is_archived
                    all_owners.append(owner_rec)
                    
                paging = data.get("paging", {})
                after = paging.get("next", {}).get("after")
                if not after: break
            except Exception as e:
                logging.error(f"Owners Fetch Error (archived={is_archived}): {e}")
                # Don't fail completely, try to get at least some
                break
                
    logging.info(f"Total Owners Fetched: {len(all_owners)}")
    return all_owners

def fetch_line_items_for_deals():
    """Fetches Line Items with Deal Associations."""
    global API_CALLS
    url = "https://api.hubapi.com/crm/v3/objects/line_items"
    headers = {"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"}
    
    # Standard props + we can add dynamic if needed, but usually standard 8 are enough
    properties = [
        "name", "price", "quantity", "amount", "hs_product_id", "description", 
        "recurringbillingfrequency", "hs_recurring_billing_start_date"
    ]
    associations = ["deals"]
    
    all_lines = []
    after = None
    
    logging.info("Starting Line Items Fetch...")
    while True:
        params = {"limit": 100, "properties": properties, "associations": associations}
        if after: params["after"] = after
        try:
            response = requests.get(url, headers=headers, params=params)
            API_CALLS += 1
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            for item in results:
                flat = item.get("properties", {}).copy()
                flat["id"] = item.get("id")
                # Extract Deal ID
                deal_assocs = item.get("associations", {}).get("deals", {}).get("results", [])
                flat["associated_deal_id"] = deal_assocs[0].get("id") if deal_assocs else None
                all_lines.append(flat)
                
            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after: break
        except Exception as e:
            logging.error(f"Line Items Fetch Error: {e}")
            # Do NOT crash entire job for line items if user hasn't fixed scopes yet
            logging.warning("Continuing without Line Items...")
            return [] 
            
    return all_lines

def fetch_batch_data(object_type, ids, properties):
    """Fetches specific objects by ID using the HubSpot Batch Read API."""
    global API_CALLS
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/read"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    all_records = []
    # Deduplicate IDs
    unique_ids = list(set(ids))
    total = len(unique_ids)
    
    logging.info(f"Starting Batch Fetch for {total} {object_type}...")
    
    # Process in chunks of 100 (HubSpot Limit)
    chunk_size = 100
    for i in range(0, total, chunk_size):
        chunk_ids = unique_ids[i:i + chunk_size]
        payload = {
            "properties": properties,
            "inputs": [{"id": mid} for mid in chunk_ids]
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            API_CALLS += 1
            if response.status_code == 207:
                # 207 Multi-Status means some might have failed, but we get results
                pass
            else:
                response.raise_for_status()
                
            data = response.json()
            results = data.get("results", [])
            
            for item in results:
                flat = item.get("properties", {}).copy()
                flat["id"] = item.get("id")
                flat["createdAt"] = item.get("createdAt")
                flat["updatedAt"] = item.get("updatedAt")
                flat["archived"] = item.get("archived")
                all_records.append(flat)
                
            logging.info(f"Batch Fetched {len(results)} records. Progress: {min(i + chunk_size, total)}/{total}")
            
        except Exception as e:
            logging.error(f"Batch Fetch Error: {e}")
            # Continue to next chunk even if one fails
            continue
            
    return all_records

def main():
    start_time = datetime.now()
    total_records = 0
    client = None
    
    try:
        if not HUBSPOT_ACCESS_TOKEN:
            raise Exception("HUBSPOT_ACCESS_TOKEN is missing.")
            
        client = get_bq_client(GCP_CREDENTIALS_JSON)
        ensure_dataset_exists(client, BQ_DATASET_ID)
        
        # 1. Pipeline Metadata
        pipelines = fetch_pipelines()
        if pipelines:
            upload_to_bigquery(client, BQ_DATASET_ID, pipelines, "hubspot_raw_Pipelines")
            
        # 2. Owners
        owners = fetch_hubspot_owners()
        if owners:
            upload_to_bigquery(client, BQ_DATASET_ID, owners, "hubspot_raw_Owners")
            
        # 3. Deals (Fetch FIRST to get Company IDs)
        deal_props = get_all_properties("deals")
        deals = fetch_hubspot_data("deals", deal_props, associations=["companies"])
        if deals:
            upload_to_bigquery(client, BQ_DATASET_ID, deals, "hubspot_raw_Deals")
        total_records += len(deals)
        
        # 4. Companies (Optimized Batch Fetch)
        if deals:
            # Extract all Company IDs mentioned in Deals
            company_ids_set = set()
            for d in deals:
                assoc_str = d.get("associated_company_ids")
                if assoc_str:
                    # IDs are comma-separated string
                    for cid in assoc_str.split(","):
                        if cid: company_ids_set.add(cid.strip())
            
            if company_ids_set:
                logging.info(f"Found {len(company_ids_set)} unique companies associated with deals.")
                company_props = get_all_properties("companies")
                companies = fetch_batch_data("companies", list(company_ids_set), company_props)
                if companies:
                    upload_to_bigquery(client, BQ_DATASET_ID, companies, "hubspot_raw_Companies")
            else:
                logging.info("No companies associated with deals.")

        # 5. Line Items
        lines = fetch_line_items_for_deals()
        if lines:
            upload_to_bigquery(client, BQ_DATASET_ID, lines, "hubspot_raw_Line_Items")
        
        log_execution(client, BQ_DATASET_ID, start_time, "SUCCESS", total_records, api_calls=API_CALLS)
        logging.info("HubSpot ETL Job Completed.")
        
    except Exception as e:
        logging.error(f"HubSpot ETL Failed: {e}")
        if client:
            try:
                log_execution(client, BQ_DATASET_ID, start_time, "FAILED", total_records, str(e), api_calls=API_CALLS)
            except:
                pass
        exit(1)

if __name__ == "__main__":
    main()
