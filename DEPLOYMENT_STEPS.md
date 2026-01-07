# Deployment Steps for Ringba Pull to Sheets

## Step 1: Commit and Push to GitHub

From your local repository, run these commands:

```bash
# Add the new files
git add ringba_pull_to_sheets.py
git add render_ringba_pull.yaml
git add requirements.txt
git add README.md
git add RENDER_DEPLOYMENT.md

# Commit the changes
git commit -m "Add Ringba pull to sheets script with continuous polling and campaign filtering"

# Push to GitHub
git push origin master
```

## Step 2: Deploy to Render

### Option A: Using Render Blueprint (Recommended)

1. **Go to Render Dashboard**: https://dashboard.render.com
2. **Click "New"** → **"Blueprint"**
3. **Connect your GitHub repository**: `unitedconsumerrelief/master-cpa-data`
4. **Render will detect** `render_ringba_pull.yaml` and create the worker service automatically
5. **Set Environment Variables** (see Step 3 below)

### Option B: Manual Worker Service Creation

1. **Go to Render Dashboard**: https://dashboard.render.com
2. **Click "New"** → **"Background Worker"**
3. **Connect your repository**: `unitedconsumerrelief/master-cpa-data`
4. **Configure the service**:
   - **Name**: `ringba-pull-to-sheets`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install --upgrade pip && pip install -r requirements.txt`
   - **Start Command**: `python ringba_pull_to_sheets.py`
   - **Plan**: `Starter` (or `Free` for testing)
5. **Set Environment Variables** (see Step 3 below)

## Step 3: Set Environment Variables in Render

Go to your service → **Environment** tab and add/update:

### Required Variables:

1. **RINGBA_API_TOKEN**
   - Your Ringba API token
   - Mark as "Secret" (eye icon)

2. **RINGBA_ACCOUNT_ID**
   - Your Ringba account ID (format: `RA...`)
   - Mark as "Secret"

3. **GOOGLE_CREDENTIALS_JSON** ✅ (You already have this)
   - Your Google Service Account JSON
   - Paste the full JSON string
   - Mark as "Secret"

4. **MASTER_CPA_DATA** ✅ (You already have this)
   - Value: `1yPWM2CIjPcAg1pF7xNUDmt22kbS2qrKqs0gWkIJzd9I`

5. **RINGBA_CAMPAIGNS** ✅ (You already have this, but UPDATE it)
   - **Current value**: `SPANISH DEBT | 1.0 STANDARD, SPANISH DEBT | 1.0 STANDARD - External`
   - **Update to** (based on your screenshot):
     ```
     SPANISH DEBT | 3.5 STANDARD | 01292025, SPANISH DEBT DQ | 3.5 STANDARD | 01292025, SPANISH DEBT | 3.5 5-8k STANDARD DQ, 0 | LIFE INSURANCE / FINAL EXPENSE | 5.0 POLICY REVIEW | 111320, Spanish Debt | 3.6 C/S | 120325 | STANDARD
     ```

### Optional Variables:

6. **SHEET_NAME** (Optional)
   - Value: `CPA Reporting`
   - This is the default, so you can skip if using default

7. **POLL_INTERVAL** (Optional)
   - Value: `60`
   - Seconds between polls (default is 60)

8. **LOOKBACK_HOURS** (Optional)
   - Value: `24`
   - Hours to look back on first run (default is 24)

## Step 4: Deploy and Monitor

1. **Click "Save Changes"** after setting environment variables
2. **Render will automatically deploy** (or click "Manual Deploy")
3. **Check the Logs** tab to see:
   - Service starting up
   - Poll iterations
   - New calls being processed
   - Any errors

## Step 5: Verify It's Working

1. **Check Render Logs**:
   - You should see: `Starting Ringba data pull to Google Sheets (continuous mode)`
   - Then: `=== Poll iteration 1 ===`
   - Then: `Found X new calls` or `No new calls to process`

2. **Check Google Sheets**:
   - Open your "CPA Reporting" sheet
   - You should see new rows being added
   - Columns A-P should be populated

## Troubleshooting

### No data appearing:
- Check that `RINGBA_CAMPAIGNS` matches exactly (case-sensitive)
- Verify `RINGBA_API_TOKEN` and `RINGBA_ACCOUNT_ID` are correct
- Check Render logs for API errors

### Service not starting:
- Verify all required environment variables are set
- Check that `requirements.txt` has all dependencies
- Review Render logs for startup errors

### Duplicate calls:
- The script automatically prevents duplicates using `call_id`
- If you see duplicates, check that the sheet's `call_id` column is correct

## Notes

- The script runs continuously and polls every 60 seconds (configurable)
- It only pulls calls matching the campaigns in `RINGBA_CAMPAIGNS`
- It automatically prevents duplicates
- It resumes from the latest timestamp if restarted
- Campaign names must match **exactly** as they appear in Ringba (case-sensitive)

