# Zoho CRM to BigQuery ETL

This project automatically syncs data from Zoho CRM to Google BigQuery twice a day.

## Prerequisites

- **Zoho CRM Account** (Standard Edition or higher)
- **Google Cloud Platform (GCP) Account** (Free tier is sufficient)
- **GitHub Account** (For hosting and automation)

---

## Step 1: Zoho CRM Setup (One-time)

You need to get credentials to allow this script to read your data.

1. Go to the [Zoho Developer Console](https://api-console.zoho.com/).
2. Click **"Add Client"** and choose **"Self Client"**.
3. Click **Create**.
4. You will see a **Client ID** and **Client Secret**. Save these.
5. In the "Generate Code" tab, enter the scope:
   `ZohoCRM.modules.ALL`
6. Choose a time duration (e.g., 10 minutes) and click **Create**.
7. Copy the **Authorization Code** generated.
8. **Generate Refresh Token**:
   Instead of running a complex terminal command, I've included a simple script for you.

   Run this command in your terminal:
   ```bash
   python get_refresh_token.py
   ```
   Follow the prompts to enter your Client ID, Secret, and Code. It will print your **Refresh Token**.
   
   **SAVE THIS SECURELY**. It never expires.

---

## Step 2: Google Cloud Platform (BigQuery) Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. **Create a New Project** (e.g., "zoho-etl-project").
3. Search for **"BigQuery API"** and **Enable** it.
4. Go to **IAM & Admin** > **Service Accounts**.
5. Click **Create Service Account**. Name it "etl-runner".
6. **Grant Access**: Give it the role **"BigQuery Admin"** (or "BigQuery Data Editor" + "Job User").
7. Click on the newly created Service Account > **Keys** tab > **Add Key** > **Create new key** > **JSON**.
8. A `.json` file will download to your computer. Open it with a text editor. You will need the *entire content* of this file later.

---

## Step 3: GitHub Setup (Automation)

1. Create a new **Private Repository** on GitHub.
2. Upload the files from this folder (`main.py`, `requirements.txt`, `.github/workflows/etl.yml`) to the repository.
3. Go to **Settings** > **Secrets and variables** > **Actions**.
4. Click **New repository secret** and add the following secrets one by one:

| Name | Value |
|------|-------|
| `ZOHO_CLIENT_ID` | Your Zoho Client ID |
| `ZOHO_CLIENT_SECRET` | Your Zoho Client Secret |
| `ZOHO_REFRESH_TOKEN` | The Refresh Token you generated in Step 1 |
| `ZOHO_DOMAIN` | (Optional) Your Zoho Domain (e.g., `in`, `eu`, `au`). Default is `com`. |
| `GCP_PROJECT_ID` | Your Google Cloud Project ID (e.g., `zoho-etl-project`) |
| `GCP_CREDENTIALS_JSON` | The **entire content** of the JSON file you downloaded in Step 2 |

---

## Step 4: Run & Verify

1. Go to the **Actions** tab in your GitHub repository.
2. You should see "Zoho to BigQuery ETL" listed.
3. Click on it, then click **Run workflow** (button on the right).
4. Wait for it to complete (green checkmark).
5. Go to standard BigQuery Console. You should see a new dataset `zoho_raw_data` and tables like `zoho_raw_Deals` filled with data!

## Troubleshooting

- **Zoho Token Error**: Your refresh token might be invalid. Regenerate it (Step 1).
- **BigQuery Error**: Ensure the API is enabled and the Service Account has "BigQuery Admin" role.
