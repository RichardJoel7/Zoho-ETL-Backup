import os
import requests
import json
import logging
import random
import string
from datetime import datetime, timedelta
from utils import get_bq_client, upload_to_bigquery, log_execution

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION (Load from Env Vars) ---
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_DOMAIN = os.getenv("ZOHO_DOMAIN") or "com" 
GCP_CREDENTIALS_JSON = os.getenv("GCP_CREDENTIALS_JSON") 
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_ID = "zoho_raw_data" 

# --- GLOBALS ---
API_CALLS = 0

# --- ZOHO AUTHENTICATION ---
def get_zoho_access_token():
    """Exchanges Refresh Token for a new Access Token."""
    global API_CALLS
    url = f"https://accounts.zoho.{ZOHO_DOMAIN}/oauth/v2/token"
    params = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    
    try:
        response = requests.post(url, params=params)
        API_CALLS += 1 
        
        if response.status_code != 200:
             logging.error(f"Zoho Auth Failed: {response.text}")
             
        response.raise_for_status()
        data = response.json()
        
        if 'access_token' in data:
            return data['access_token']
        else:
            logging.error(f"Error getting access token: {data}")
            raise Exception(f"Could not get access token: {data.get('error')}")
    except Exception as e:
        logging.error(f"Failed to refresh Zoho token: {e}")
        raise

# --- ZOHO DISCOVERY ---
def discover_modules(access_token):
    """Fetches all modules to find correct API Names."""
    url = f"https://www.zohoapis.{ZOHO_DOMAIN}/crm/v2.1/settings/modules"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    
    label_map = {
        "Deals": None, 
        "Blocks": None, 
        "Site Splits": None,
        "Revenue Recognition": None,
        "Stage History": None,
        "Blocks_X_Deals": None,
        "Companies": None # explicitly look for this too
    }
        
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            modules = response.json().get("modules", [])
            logging.info(f"DEBUG: Found {len(modules)} modules in CRM. Listing all:")
            
            for m in modules:
                label = m.get("plural_label")
                api_name = m.get("api_name")
                logging.info(f" - Module: {label} (API: {api_name})")
                
                # Check against our target list
                if label in label_map or api_name in label_map:
                    label_map[label] = api_name
                    logging.info(f"CONFIRMED: Label '{label}' -> API Name '{api_name}'")
                    
        else:
            logging.error(f"Failed to discover modules: {response.text}")
            
    except Exception as e:
        logging.error(f"Discovery failed: {e}")
        
    return label_map

def describe_deal_relations(access_token):
    """Asks Deals how it is related to Blocks. Returns the Linking Module Name."""
    url = f"https://www.zohoapis.{ZOHO_DOMAIN}/crm/v2.1/settings/related_lists?module=Deals"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    linking_module = None
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            r_lists = response.json().get("related_lists", [])
            logging.info(f"--- ANALYZING DEALS RELATED LISTS ({len(r_lists)}) ---")
            
            for rl in r_lists:
                label = rl.get("display_label")
                module = rl.get("module")
                api_name = rl.get("api_name")
                
                if module == "Blocks" or label == "Award Blocks" or "Blocks" in str(module):
                    logging.info(f"*** FOUND RELATION TO BLOCKS ***")
                    logging.info(f"FULL DUMP: {json.dumps(rl)}")
                    
                    if module != "Blocks": # It's a linking module!
                         linking_module = module
                    elif api_name != "Related_Blocks":
                         # Sometimes the API Name holds the clue if module is generic
                         logging.info(f"Checking if {api_name} is a module...")
                         linking_module = api_name
                    
    except Exception as e:
        logging.error(f"Relations check failed: {e}")
        
    return linking_module

def inspect_blocks_metadata(access_token, blocks_api_name):
    """Dumps ALL fields in Blocks to find the hidden link."""
    url = f"https://www.zohoapis.{ZOHO_DOMAIN}/crm/v2.1/settings/fields?module={blocks_api_name}"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            fields = response.json().get("fields", [])
            logging.info(f"--- DUMPING ALL {len(fields)} FIELDS IN {blocks_api_name} ---")
            
            # Dump them all to find the mystery field
            for f in fields:
                f_label = f.get("field_label")
                f_api = f.get("api_name")
                f_type = f.get("data_type")
                
                # Highlight suspicious ones
                prefix = "   "
                if f_type in ["lookup", "ownerlookup", "multimodulelookup"]:
                    prefix = ">>>"
                if "deal" in f_api.lower() or "connect" in f_api.lower():
                     prefix = "$$$"
                     
                logging.info(f"{prefix} [{f_type}] {f_label} ({f_api})")
                    
    except Exception as e:
        logging.error(f"Metadata inspection failed: {e}")
        
    return None 

def generate_action_id():
    """Generates a unique action ID starting with 'A' followed by 10 digits."""
    digits = ''.join(random.choices(string.digits, k=10))
    return f"A{digits}"

def fetch_previous_revenue_data(client, dataset_id, history_table_name="zoho_revenue_history"):
    """
    Fetches the MOST RECENT state of the Revenue Recognition records from the HISTORY table.
    We rely on the history table (which has 'Create' and update logs) as the source of truth
    for what we've already seen.
    """
    full_table_id = f"{client.project}.{dataset_id}.{history_table_name}"
    try:
        # Check if history table exists first
        client.get_table(full_table_id)
        
        # We want the LATEST record for each revenue_id from the history table
        # NOTE: BigQuery is case-sensitive for column names. 
        # Using exact column names based on user's manual copy screenshot
        query = f"""
            WITH RankedHistory AS (
                SELECT 
                    revenue_id as id, 
                    amount as Amount, 
                    Revenue_date as Date, 
                    Block_id as Parent_Id_id, 
                    block_name as Parent_Id_name,
                    Type,
                    ROW_NUMBER() OVER(PARTITION BY revenue_id ORDER BY action_timestamp DESC) as rn
                FROM `{full_table_id}`
            )
            SELECT id, Amount, Date, Parent_Id_id, Parent_Id_name, Type
            FROM RankedHistory
            WHERE rn = 1 AND id IS NOT NULL
        """
        query_job = client.query(query)
        results = query_job.result()
        
        # Create a dictionary mapping ID to the full record
        previous_data = {}
        for row in results:
            # We will use string comparison as requested, but strip trailing .0 for consistency
            amount_val = None
            if row.Amount is not None:
                amount_str = str(row.Amount).strip()
                if amount_str.endswith(".0"):
                    amount_str = amount_str[:-2]
                amount_val = amount_str

            previous_data[str(row.id)] = {
                "Amount": amount_val,
                "Date": str(row.Date) if row.Date is not None else None,
                "Parent_Id_id": str(row.Parent_Id_id) if row.Parent_Id_id is not None else None,
                "Parent_Id_name": str(row.Parent_Id_name) if row.Parent_Id_name is not None else None,
                "Type": str(row.Type) if row.Type is not None else None
            }
        return previous_data
    except Exception as e:
        logging.warning(f"Could not fetch previous revenue data from history table (it may be the first run): {e}")
        return {}

def track_revenue_history(client, dataset_id, current_records, previous_data):
    """Compares current records with previous data and uploads changes to a history table."""
    history_records = []
    # Calculate IST time (UTC + 5:30)
    ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    action_timestamp = ist_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Pre-compute deleted records by composite key (Block_id, Revenue_date)
    # This helps identify replacements where ID changed but data is same.
    deleted_records_by_key = {}
    current_record_ids_temp = {str(r.get("id")) for r in current_records}
    
    for old_id, old_data in previous_data.items():
        if old_id not in current_record_ids_temp:
            old_type = str(old_data.get("Type", ""))
            # It's deleted in Zoho
            if old_type != "Delete" and not old_type.startswith("Deleted"): 
                # We use Block_id and Date as the composite key
                comp_key = (old_data.get("Parent_Id_id"), old_data.get("Date"))
                # If there are multiple, keep a list
                if comp_key not in deleted_records_by_key:
                    deleted_records_by_key[comp_key] = []
                deleted_records_by_key[comp_key].append(old_id)

    for record in current_records:
        rec_id = str(record.get("id"))
        
        # The fields might be nested under 'Parent_Id' if not flattened yet, 
        # but fetch_module_data returns raw dicts from Zoho. 
        # Zoho's raw format for lookups is usually a dict: {"id": "...", "name": "..."}
        parent_id_obj = record.get("Parent_Id", {})
        if isinstance(parent_id_obj, dict):
            block_id = str(parent_id_obj.get("id")) if parent_id_obj.get("id") else None
            block_name = str(parent_id_obj.get("name")) if parent_id_obj.get("name") else None
        else:
            # Fallback if already flattened or unexpected format
            block_id = str(record.get("Parent_Id_id", "")) or None
            block_name = str(record.get("Parent_Id_name", "")) or None
            
        # Clean current amount for comparison as string
        current_amount_val = None
        # In Zoho, custom currency fields or multi-currency setups might store the amount in 
        # a slightly different key like 'Amount1' or 'Currency' or as a float.
        # We'll use the raw_amount logic we built, but ensure it captures the right key.
        raw_amount = record.get("Amount")
        
        # If "Amount" isn't present, check for variations.
        if raw_amount is None:
             for key, val in record.items():
                  if "amount" in key.lower() and val is not None:
                       raw_amount = val
                       break
        
        # Fallback check if it's a multi-currency dictionary in Zoho: {"currency": "USD", "value": 500}
        if isinstance(raw_amount, dict):
             raw_amount = raw_amount.get("value")
             
        if raw_amount is not None and str(raw_amount).strip() != "":
            amount_str = str(raw_amount).strip()
            if amount_str.endswith(".0"):
                amount_str = amount_str[:-2]
            current_amount_val = amount_str
                
        current_date = str(record.get("Date")) if record.get("Date") is not None else None
        
        if rec_id in previous_data:
            prev = previous_data[rec_id]
            prev_amount = prev.get("Amount")
            prev_date = prev.get("Date")
            
            changes = []
            
            # Compare as strings
            amount_changed = False
            if current_amount_val != prev_amount:
                amount_changed = True
                
            if amount_changed:
                changes.append("Amount changed")
                # Debug logging to see exactly why it thought it changed
                logging.info(f"Amount mismatch for {rec_id}: Current='{current_amount_val}' vs Previous='{prev_amount}'")
                
            if current_date != prev_date:
                changes.append("Date changed")
                logging.info(f"Date mismatch for {rec_id}: Current='{current_date}' vs Previous='{prev_date}'")
                
            if changes:
                change_type = " and ".join(changes)
                history_entry = {
                    "action_id": generate_action_id(),
                    "action_timestamp": action_timestamp,
                    "Block_id": block_id,
                    "revenue_id": rec_id,
                    "block_name": block_name,
                    "Revenue_date": current_date,
                    "amount": str(current_amount_val) if current_amount_val is not None else None, # Store as string in BQ
                    "Type": change_type
                }
                history_records.append(history_entry)
                logging.info(f"Change detected for Revenue ID {rec_id}: {change_type}")
        else:
            # If the record is NOT in the history table, check if it's replacing a deleted one
            comp_key = (block_id, current_date)
            replaced_old_id = None
            if comp_key in deleted_records_by_key and deleted_records_by_key[comp_key]:
                # Found a matching deleted record!
                replaced_old_id = deleted_records_by_key[comp_key].pop(0) # take the first match
                
            if replaced_old_id:
                # It's a replacement
                change_type = f"Replaced {replaced_old_id}"
                logging.info(f"Replacement detected! New ID {rec_id} replaces Old ID {replaced_old_id}")
                
                # We also need to explicitly mark the old one as deleted so it doesn't stay active
                old_prev = previous_data[replaced_old_id]
                history_records.append({
                    "action_id": generate_action_id(),
                    "action_timestamp": action_timestamp,
                    "Block_id": old_prev.get("Parent_Id_id"),
                    "revenue_id": replaced_old_id,
                    "block_name": old_prev.get("Parent_Id_name"),
                    "Revenue_date": old_prev.get("Date"),
                    "amount": old_prev.get("Amount"),
                    "Type": f"Deleted (Replaced by {rec_id})"
                })
            else:
                change_type = "Create"
                logging.info(f"New Revenue ID detected {rec_id}: Logged as Create")

            history_entry = {
                "action_id": generate_action_id(),
                "action_timestamp": action_timestamp,
                "Block_id": block_id,
                "revenue_id": rec_id,
                "block_name": block_name,
                "Revenue_date": current_date,
                "amount": str(current_amount_val) if current_amount_val is not None else None,
                "Type": change_type
            }
            history_records.append(history_entry)

    # Now, handle any remaining deleted records that were NOT replaced
    for comp_key, old_ids in deleted_records_by_key.items():
        for old_id in old_ids:
            old_prev = previous_data[old_id]
            history_records.append({
                "action_id": generate_action_id(),
                "action_timestamp": action_timestamp,
                "Block_id": old_prev.get("Parent_Id_id"),
                "revenue_id": old_id,
                "block_name": old_prev.get("Parent_Id_name"),
                "Revenue_date": old_prev.get("Date"),
                "amount": old_prev.get("Amount"),
                "Type": "Delete"
            })
            logging.info(f"Revenue ID {old_id} was deleted in Zoho. Logged as Delete.")

    if history_records:
        logging.info(f"Found {len(history_records)} revenue changes. Uploading to history table...")
        upload_to_bigquery(client, dataset_id, history_records, "zoho_revenue_history")
    else:
        logging.info("No changes detected in Revenue Recognition data.")


# --- ZOHO FETCH ---
def fetch_module_data(module_name, access_token):
    """Fetches all records from a Zoho Module using pagination."""
    global API_CALLS
    url = f"https://www.zohoapis.{ZOHO_DOMAIN}/crm/v2.1/{module_name}" 
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }
    
    all_records = []
    page = 1
    more_data = True
    
    logging.info(f"Fetching data for module: {module_name}...")
    
    while more_data:
        params = {"page": page, "per_page": 200} 
        try:
            response = requests.get(url, headers=headers, params=params)
            API_CALLS += 1 
            
            if response.status_code == 204: 
                break
                
            if response.status_code != 200:
                logging.error(f"Error fetching page {page} for {module_name}: {response.status_code} {response.reason}")
                if response.status_code == 404:
                    break
                break
                
            data = response.json()
            
            if "data" in data and data["data"]:
                records = data["data"]
                
                # Debug logging for Revenue Recognition Amount field
                if module_name == "Revenue_Recognition" and records:
                    sample_record = records[0]
                    amount_raw = sample_record.get("Amount")
                    logging.info(f"DEBUG: Raw Amount field from Zoho API for Revenue_Recognition: '{amount_raw}' (Type: {type(amount_raw)})")
                    # Check for other common variations of the field name
                    for key in sample_record.keys():
                        if "amount" in key.lower():
                             logging.info(f"DEBUG: Found potential amount field: {key} = {sample_record[key]}")

                all_records.extend(records)
                logging.info(f"Page {page}: Fetched {len(records)} records.")
                
                info = data.get("info", {})
                more_data = info.get("more_records", False)
                page += 1
            else:
                more_data = False
        except Exception as e:
            logging.error(f"Error fetching page {page} for {module_name}: {e}")
            break
            
    return all_records

# --- MAIN ---
def main():
    start_time = datetime.now()
    total_records = 0
    client = None
    
    try:
        client = get_bq_client(GCP_CREDENTIALS_JSON)
        logging.info("Starting Zoho ETL Job...")
        
        # 1. Auth 
        try:
            access_token = get_zoho_access_token()
        except Exception as e:
            logging.error("CRITICAL: Authentication failed. Please check CLIENT_ID and CLIENT_SECRET.")
            raise e
        
        # 2. DISCOVERY PHASE
        logging.info("--- DISCOVERING MODULES ---")
        discovered_map = discover_modules(access_token)
        
        # Filter found modules
        modules_to_sync = [name for name in discovered_map.values() if name is not None]
        
        if not modules_to_sync:
            logging.warning("Discovery failed. Using default API names.")
            modules_to_sync = ["Deals", "Blocks", "Site_Splits", "Revenue_Recognition"]
        else:
            logging.info(f"Syncing these discovered modules: {modules_to_sync}")
        
        # 3. FIELD INSPECTION and LINKING MODULE
        blocks_api = discovered_map.get("Blocks") or "Blocks"
        if blocks_api in modules_to_sync:
            # Check from the Parent side to find Junction Module
            linking_module = describe_deal_relations(access_token)
            
            if linking_module:
                logging.info(f"--- DETECTED LINKING MODULE: {linking_module} ---")
                if linking_module not in modules_to_sync:
                    modules_to_sync.append(linking_module)
        
        # 4. FETCH & SYNC
        for module in modules_to_sync:
            logging.info(f"Processing {module}...")
            
            records = fetch_module_data(module, access_token)
            
            if not records:
                logging.info(f"No records found for {module}.")
                continue
            
            # Clean table name
            clean_module_name = module.replace("SIte_Splits", "Site_Splits")
            table_name = f"zoho_raw_{clean_module_name}"
            
            # History Tracking for Revenue Recognition
            if module == "Revenue_Recognition":
                logging.info("Checking history for Revenue_Recognition...")
                previous_data = fetch_previous_revenue_data(client, BQ_DATASET_ID)
                track_revenue_history(client, BQ_DATASET_ID, records, previous_data)
            
            # Upload Primary Module
            upload_to_bigquery(client, BQ_DATASET_ID, records, table_name)
            
        # 5. CLEANUP LEGACY DATA
        try:
            legacy_table = f"{client.project}.{BQ_DATASET_ID}.zoho_raw_SIte_Splits"
            client.delete_table(legacy_table, not_found_ok=True)
            logging.info("Cleaned up legacy table zoho_raw_SIte_Splits.")
        except Exception as e:
            logging.warning(f"Legacy cleanup failed: {e}")
        
        log_execution(client, BQ_DATASET_ID, start_time, "SUCCESS", total_records, api_calls=API_CALLS)
        logging.info("ETL Job Completed Successfully.")
        
    except Exception as e:
        logging.error(f"ETL Job Failed: {e}")
        if client:
             try:
                 log_execution(client, BQ_DATASET_ID, start_time, "FAILED", total_records, str(e), api_calls=API_CALLS)
             except:
                 pass
        exit(1)

if __name__ == "__main__":
    main()
