# Render Deployment Guide for Ringba Pull to Sheets

## Overview
This script continuously pulls call data from Ringba API and writes it to Google Sheets. It runs as a background worker service on Render.

## Environment Variables Setup

Based on your current Render environment variables, you need to add the following:

### Required Variables (Add these to Render):

1. **RINGBA_API_TOKEN**
   - Your Ringba API token
   - Keep this secret (marked as sync: false in render.yaml)

2. **RINGBA_ACCOUNT_ID**
   - Your Ringba account ID (format: `RA...`)
   - Keep this secret (marked as sync: false in render.yaml)

3. **GOOGLE_CREDENTIALS_JSON** ✅ (Already set)
   - Your Google Service Account JSON credentials
   - Should be the full JSON string

4. **MASTER_CPA_DATA** ✅ (Already set)
   - Your Google Sheets ID: `1yPWM2CIjPcAg1pF7xNUDmt22kbS2qrKqs0gWkIJzd9I`

5. **RINGBA_CAMPAIGNS** ✅ (Already set)
   - Comma-separated list of campaigns to track
   - Current value: `SPANISH DEBT | 1.0 STANDARD, SPANISH DEBT | 1.0 STANDARD - External`
   - **Update this** to include all campaigns from your screenshot:
     ```
     SPANISH DEBT | 3.5 STANDARD | 01292025, SPANISH DEBT DQ | 3.5 STANDARD | 01292025, SPANISH DEBT | 3.5 5-8k STANDARD DQ, 0 | LIFE INSURANCE / FINAL EXPENSE | 5.0 POLICY REVIEW | 111320, Spanish Debt | 3.6 C/S | 120325 | STANDARD
     ```

### Optional Variables:

6. **SHEET_NAME** (Optional)
   - Default: `CPA Reporting`
   - The name of the sheet tab to write data to

7. **POLL_INTERVAL** (Optional)
   - Default: `60` (seconds)
   - How often to check for new data

8. **LOOKBACK_HOURS** (Optional)
   - Default: `24` (hours)
   - How far back to look on first run

## Deployment Steps

### Option 1: Using render.yaml (Recommended)

1. **Connect your repository to Render**
   - Go to Render Dashboard
   - Click "New" → "Blueprint"
   - Connect your Git repository
   - Render will detect `render_ringba_pull.yaml`

2. **Set Environment Variables**
   - In Render Dashboard, go to your service
   - Navigate to "Environment" tab
   - Add/update the environment variables listed above
   - **Important**: Make sure `RINGBA_CAMPAIGNS` includes all campaigns you want to track

3. **Deploy**
   - Render will automatically deploy when you push to your repository
   - Or manually trigger a deploy from the Dashboard

### Option 2: Manual Setup

1. **Create a new Worker service**
   - Go to Render Dashboard
   - Click "New" → "Background Worker"
   - Connect your Git repository

2. **Configure the service:**
   - **Name**: `ringba-pull-to-sheets`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install --upgrade pip && pip install -r requirements.txt`
   - **Start Command**: `python ringba_pull_to_sheets.py`
   - **Plan**: `Starter` (or `Free` for testing)

3. **Add Environment Variables**
   - Add all the required variables listed above
   - Make sure to paste the full JSON for `GOOGLE_CREDENTIALS_JSON`

4. **Deploy**

## Campaign Filtering

The script will **only** pull calls that match the campaigns listed in `RINGBA_CAMPAIGNS`. 

Based on your screenshot, update `RINGBA_CAMPAIGNS` to:
```
SPANISH DEBT | 3.5 STANDARD | 01292025, SPANISH DEBT DQ | 3.5 STANDARD | 01292025, SPANISH DEBT | 3.5 5-8k STANDARD DQ, 0 | LIFE INSURANCE / FINAL EXPENSE | 5.0 POLICY REVIEW | 111320, Spanish Debt | 3.6 C/S | 120325 | STANDARD
```

**Note**: Campaign names must match **exactly** as they appear in Ringba (case-sensitive).

## Monitoring

- Check the Render logs to see:
  - Poll iterations
  - Number of new calls found
  - Any errors or warnings
  - Campaign filtering activity

## Troubleshooting

1. **No data appearing in sheets**
   - Check that `RINGBA_CAMPAIGNS` matches exactly (case-sensitive)
   - Verify `RINGBA_API_TOKEN` and `RINGBA_ACCOUNT_ID` are correct
   - Check Render logs for API errors

2. **Duplicate calls**
   - The script automatically prevents duplicates using `call_id`
   - If you see duplicates, check that the sheet's `call_id` column is correct

3. **Script not running**
   - Verify the start command: `python ringba_pull_to_sheets.py`
   - Check that all required environment variables are set
   - Review Render logs for startup errors

## Service Type

This runs as a **Worker** service (not a Web service) because it:
- Runs continuously in the background
- Doesn't need to accept HTTP requests
- Polls the Ringba API at regular intervals

