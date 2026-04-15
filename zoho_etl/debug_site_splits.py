import os
import requests
import json
import logging
from dotenv import load_dotenv

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Load environment variables if you have a .env file locally
load_dotenv()

ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_DOMAIN = os.getenv("ZOHO_DOMAIN") or "com"

def get_zoho_access_token():
    url = f"https://accounts.zoho.{ZOHO_DOMAIN}/oauth/v2/token"
    params = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()['access_token']

def inspect_site_splits():
    try:
        access_token = get_zoho_access_token()
        url = f"https://www.zohoapis.{ZOHO_DOMAIN}/crm/v2.1/Site_Splits"
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        
        logging.info("Fetching first 200 Site Splits from Zoho...")
        response = requests.get(url, headers=headers, params={"page": 1, "per_page": 200})
        
        if response.status_code == 204:
            logging.info("No Site Splits found.")
            return
            
        response.raise_for_status()
        records = response.json().get("data", [])
        
        # Find records where Parent_Id is missing or null
        orphans = [r for r in records if not r.get("Parent_Id")]
        
        logging.info(f"\nFound {len(records)} total Site Splits on page 1.")
        logging.info(f"Found {len(orphans)} Site Splits with empty/null 'Parent_Id'.\n")
        
        if orphans:
            logging.info("--- RAW JSON DUMP OF FIRST ORPHANED RECORD ---")
            # Print the first orphaned record nicely to see what other lookup fields exist
            print(json.dumps(orphans[0], indent=2))
        else:
            logging.info("All records on page 1 have a Parent_Id. The 51 orphaned records might be on later pages.")
            
    except Exception as e:
        logging.error(f"Failed to inspect Zoho API: {e}")

if __name__ == "__main__":
    if not ZOHO_CLIENT_ID:
        logging.error("ERROR: Missing Zoho credentials. Make sure you set your environment variables or have a .env file.")
    else:
        inspect_site_splits()
