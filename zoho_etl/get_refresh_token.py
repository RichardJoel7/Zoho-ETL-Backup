import requests
import urllib.parse

print("--- Zoho Refresh Token Generator ---")

# Inputs
client_id = input("Enter your Client ID: ").strip()
client_secret = input("Enter your Client Secret: ").strip()
auth_code = input("Enter the Authorization Code (from the 'Generate Code' step): ").strip()
domain = input("Enter your Zoho Domain (e.g., com, eu, in) [default: com]: ").strip() or "com"

url = f"https://accounts.zoho.{domain}/oauth/v2/token"

data = {
    "client_id": client_id,
    "client_secret": client_secret,
    "code": auth_code,
    "grant_type": "authorization_code"
}

print(f"\nRequesting token from {url}...")

try:
    response = requests.post(url, data=data)
    response_json = response.json()
    
    if "refresh_token" in response_json:
        print("\nSUCCESS! Here is your Refresh Token:")
        print("---------------------------------------------------")
        print(response_json["refresh_token"])
        print("---------------------------------------------------")
        print("Please save this in your GitHub Secrets as ZOHO_REFRESH_TOKEN")
    else:
        print("\nERROR: Could not get refresh token.")
        print("Response from Zoho:")
        print(response_json)
        
except Exception as e:
    print(f"\nAn error occurred: {e}")

input("\nPress Enter to exit...")
