#!/usr/bin/env python3
"""
Simplified script to pull data from Ringba API and write to Google Sheets
Output matches the screenshot structure (columns A-P, excluding Q and R)
"""

import os
import json
import asyncio
import aiohttp
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
RINGBA_API_TOKEN = os.getenv("RINGBA_API_TOKEN")
RINGBA_ACCOUNT_ID = os.getenv("RINGBA_ACCOUNT_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
MASTER_CPA_DATA = os.getenv("MASTER_CPA_DATA")
SHEET_NAME = os.getenv("SHEET_NAME", "Ringba Raw")  # Default to "Ringba Raw"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))  # Poll every 60 seconds by default
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))  # Look back 24 hours for initial sync

# Parse RINGBA_CAMPAIGNS - comma-separated list of campaigns to filter
def get_ringba_campaigns():
    """Parse RINGBA_CAMPAIGNS environment variable"""
    campaigns = set()
    if os.getenv("RINGBA_CAMPAIGNS"):
        campaigns_str = os.getenv("RINGBA_CAMPAIGNS")
        # Split by comma and strip whitespace
        campaigns = {campaign.strip() for campaign in campaigns_str.split(",") if campaign.strip()}
        logger.info(f"Campaign filter enabled: {len(campaigns)} campaigns")
        logger.debug(f"Campaigns: {campaigns}")
    return campaigns

RINGBA_CAMPAIGNS = get_ringba_campaigns()

def normalize_did(did: str) -> str:
    """Extract last 10 digits from DID"""
    if not did:
        return ""
    digits_only = re.sub(r'\D', '', str(did))
    return digits_only[-10:] if len(digits_only) >= 10 else digits_only

def extract_sheet_id(url_or_id: str) -> str:
    """Extract sheet ID from URL or return as-is if already an ID"""
    if url_or_id.startswith("http"):
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url_or_id)
        return match.group(1) if match else url_or_id
    return url_or_id

def init_google_sheets():
    """Initialize Google Sheets service"""
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is required")
    
    try:
        credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        sheets_service = build('sheets', 'v4', credentials=credentials)
        logger.info("Google Sheets service initialized successfully")
        return sheets_service
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets service: {e}")
        raise

def format_date_time(call_start_utc: str) -> Tuple[str, str]:
    """Convert UTC timestamp to Date and Time columns (EDT format)"""
    try:
        # Parse UTC timestamp
        if 'T' in call_start_utc:
            # ISO format: 2025-09-09T20:43:09Z
            dt = datetime.fromisoformat(call_start_utc.replace('Z', '+00:00'))
        else:
            # Try other formats
            dt = datetime.strptime(call_start_utc, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to EDT (America/New_York)
        # EDT is UTC-4, EST is UTC-5
        edt_tz = timezone(timedelta(hours=-4))  # Using EDT for simplicity
        dt_edt = dt.astimezone(edt_tz)
        
        # Format Date: M/D/YYYY (e.g., "9/9/2025")
        date_str = f"{dt_edt.month}/{dt_edt.day}/{dt_edt.year}"
        
        # Format Time: H:MM AM/PM (e.g., "3:43 PM")
        hour = dt_edt.hour
        minute = dt_edt.minute
        if hour == 0:
            time_str = f"12:{minute:02d} AM"
        elif hour < 12:
            time_str = f"{hour}:{minute:02d} AM"
        elif hour == 12:
            time_str = f"12:{minute:02d} PM"
        else:
            time_str = f"{hour-12}:{minute:02d} PM"
        
        return date_str, time_str
    except Exception as e:
        logger.warning(f"Error parsing date/time '{call_start_utc}': {e}")
        return "", ""

async def fetch_ringba_data(start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """Fetch call-level data from Ringba API"""
    if not RINGBA_API_TOKEN or not RINGBA_ACCOUNT_ID:
        raise ValueError("RINGBA_API_TOKEN and RINGBA_ACCOUNT_ID environment variables are required")
    
    # Log which account we're using for debugging
    logger.info(f"🔍 Using Ringba Account ID: {RINGBA_ACCOUNT_ID}")
    
    url = f"https://api.ringba.com/v2/{RINGBA_ACCOUNT_ID}/insights"
    
    headers = {
        "Authorization": f"Token {RINGBA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Request call-level data grouped by callId
    payload = {
        "reportStart": start_str,
        "reportEnd": end_str,
        "groupByColumns": [{"column": "callId", "displayName": "Call ID"}],
        "valueColumns": [
            {"column": "callStartUtc", "aggregateFunction": None},
            {"column": "did", "aggregateFunction": None},
            {"column": "callerId", "aggregateFunction": None},
            {"column": "callLengthInSeconds", "aggregateFunction": None},
            {"column": "disposition", "aggregateFunction": None},
            {"column": "campaignName", "aggregateFunction": None},
            {"column": "target", "aggregateFunction": None},
            {"column": "publisherId", "aggregateFunction": None},
            {"column": "publisherName", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callStartUtc", "direction": "desc"}],
        "maxResultsPerGroup": 10000,  # Adjust as needed
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    logger.info(f"Fetching Ringba data from {start_str} to {end_str}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully fetched data from Ringba API")
                    
                    # Parse the response
                    calls = []
                    
                    # Try different response structures
                    records = None
                    if "report" in data and "records" in data["report"]:
                        records = data["report"]["records"]
                    elif "data" in data and "rows" in data["data"]:
                        records = data["data"]["rows"]
                    elif "rows" in data:
                        records = data["rows"]
                    
                    if records:
                        for row in records:
                            call_data = {}
                            
                            # Extract call_id from groupByValues or directly from row
                            if "groupByValues" in row and len(row["groupByValues"]) > 0:
                                call_data["call_id"] = row["groupByValues"][0]
                            elif "callId" in row:
                                call_data["call_id"] = row["callId"]
                            else:
                                # Try to get from the row keys
                                call_data["call_id"] = row.get("call_id", "")
                            
                            # Extract value columns - try both structures
                            if "values" in row:
                                # Structure with values array
                                values = row["values"]
                                call_data["call_start_utc"] = values[0] if len(values) > 0 else ""
                                call_data["did_raw"] = values[1] if len(values) > 1 else ""
                                call_data["caller_id"] = values[2] if len(values) > 2 else ""
                                call_data["duration_sec"] = values[3] if len(values) > 3 else ""
                                call_data["disposition"] = values[4] if len(values) > 4 else ""
                                call_data["campaign"] = values[5] if len(values) > 5 else ""
                                call_data["target"] = values[6] if len(values) > 6 else ""
                                call_data["publisher_id"] = values[7] if len(values) > 7 else ""
                                call_data["publisher_name"] = values[8] if len(values) > 8 else ""
                                call_data["payout"] = values[9] if len(values) > 9 else 0
                                call_data["revenue"] = values[10] if len(values) > 10 else 0
                            else:
                                # Structure with direct keys (like monitor.py uses)
                                call_data["call_start_utc"] = row.get("callStartUtc", row.get("call_start_utc", ""))
                                call_data["did_raw"] = row.get("did", row.get("did_raw", ""))
                                call_data["caller_id"] = row.get("callerId", row.get("caller_id", ""))
                                call_data["duration_sec"] = row.get("callLengthInSeconds", row.get("duration_sec", ""))
                                call_data["disposition"] = row.get("disposition", "")
                                call_data["campaign"] = row.get("campaignName", row.get("campaign", ""))
                                call_data["target"] = row.get("target", "")
                                call_data["publisher_id"] = row.get("publisherId", row.get("publisher_id", ""))
                                call_data["publisher_name"] = row.get("publisherName", row.get("publisher_name", ""))
                                call_data["payout"] = row.get("payoutAmount", row.get("payout", 0))
                                call_data["revenue"] = row.get("conversionAmount", row.get("revenue", 0))
                            
                            # Only add if we have a call_id
                            if call_data.get("call_id"):
                                # Filter by campaign if RINGBA_CAMPAIGNS is set
                                campaign = call_data.get("campaign", "")
                                if RINGBA_CAMPAIGNS and campaign not in RINGBA_CAMPAIGNS:
                                    logger.debug(f"Filtered out call {call_data.get('call_id')} - campaign '{campaign}' not in filter list")
                                    continue
                                
                                calls.append(call_data)
                    
                    logger.info(f"Parsed {len(calls)} calls from Ringba API")
                    return calls
                else:
                    error_text = await response.text()
                    logger.error(f"Ringba API error {response.status}: {error_text}")
                    return []
    except Exception as e:
        logger.error(f"Error fetching Ringba data: {e}")
        return []

def prepare_rows_for_sheets(calls: List[Dict[str, Any]]) -> List[List[Any]]:
    """Convert call data to rows for Google Sheets (columns A-P)"""
    rows = []
    
    for call in calls:
        call_id = call.get("call_id", "")
        call_start_utc = call.get("call_start_utc", "")
        did_raw = call.get("did_raw", "")
        did_canon = normalize_did(did_raw)
        caller_id = call.get("caller_id", "")
        duration_sec = call.get("duration_sec", "")
        disposition = call.get("disposition", "")
        campaign = call.get("campaign", "")
        target = call.get("target", "")
        publisher_id = call.get("publisher_id", "")
        publisher_name = call.get("publisher_name", "")
        payout = call.get("payout", 0)
        revenue = call.get("revenue", 0)
        ingested_at = datetime.now(timezone.utc).isoformat()
        
        # Convert call_start_utc to Date and Time
        date_str, time_str = format_date_time(call_start_utc)
        
        # Create row with columns A-P (excluding Q and R)
        row = [
            call_id,              # A: call_id
            call_start_utc,       # B: call_start_utc
            did_raw,              # C: did_raw
            did_canon,            # D: did_canon
            caller_id,            # E: caller_id
            duration_sec,         # F: duration_sec
            disposition,         # G: disposition
            campaign,             # H: campaign
            target,               # I: target
            publisher_id,         # J: publisher_id
            publisher_name,     # K: publisher_name
            payout,               # L: payout
            revenue,              # M: revenue
            ingested_at,          # N: _ingested_at
            date_str,             # O: Date
            time_str              # P: Time
        ]
        
        rows.append(row)
    
    return rows

def get_existing_call_ids(sheets_service, sheet_name: str) -> set:
    """Get set of existing call_ids from the sheet to avoid duplicates"""
    if not MASTER_CPA_DATA:
        return set()
    
    sheet_id = extract_sheet_id(MASTER_CPA_DATA)
    existing_ids = set()
    
    try:
        # Read all data from the sheet (column A contains call_id)
        range_name = f"{sheet_name}!A:A"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if values:
            # Skip header row (first row)
            for row in values[1:]:
                if row and len(row) > 0:
                    call_id = str(row[0]).strip()
                    if call_id:
                        existing_ids.add(call_id)
        
        logger.info(f"Found {len(existing_ids)} existing call_ids in sheet")
    except Exception as e:
        logger.warning(f"Could not read existing call_ids: {e}")
    
    return existing_ids

def get_latest_timestamp(sheets_service, sheet_name: str) -> Optional[datetime]:
    """Get the latest call_start_utc timestamp from the sheet"""
    if not MASTER_CPA_DATA:
        return None
    
    sheet_id = extract_sheet_id(MASTER_CPA_DATA)
    latest = None
    
    try:
        # Read call_start_utc column (column B)
        range_name = f"{sheet_name}!B:B"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if values:
            # Skip header row
            for row in values[1:]:
                if row and len(row) > 0:
                    timestamp_str = str(row[0]).strip()
                    if timestamp_str:
                        try:
                            # Parse timestamp
                            if 'T' in timestamp_str:
                                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            else:
                                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                                dt = dt.replace(tzinfo=timezone.utc)
                            
                            if latest is None or dt > latest:
                                latest = dt
                        except Exception:
                            continue
        
        if latest:
            logger.info(f"Latest timestamp in sheet: {latest}")
    except Exception as e:
        logger.warning(f"Could not read latest timestamp: {e}")
    
    return latest

async def write_to_sheets(sheets_service, rows: List[List[Any]], sheet_name: str):
    """Write rows to Google Sheets"""
    if not MASTER_CPA_DATA:
        raise ValueError("MASTER_CPA_DATA environment variable is required")
    
    sheet_id = extract_sheet_id(MASTER_CPA_DATA)
    
    # Headers for columns A-P
    headers = [
        "call_id", "call_start_utc", "did_raw", "did_canon", "caller_id",
        "duration_sec", "disposition", "campaign", "target",
        "publisher_id", "publisher_name", "payout", "revenue", "_ingested_at",
        "Date", "Time"
    ]
    
    # Check if sheet exists and has headers
    try:
        range_name = f"{sheet_name}!A1:P1"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        existing_values = result.get('values', [])
        if not existing_values or existing_values[0] != headers:
            # Update headers
            body = {'values': [headers]}
            sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            logger.info(f"Updated headers in {sheet_name} sheet")
    except Exception as e:
        # Sheet might not exist or be empty, create headers
        logger.info(f"Creating headers in {sheet_name} sheet")
        body = {'values': [headers]}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{sheet_name}!A1:P1",
            valueInputOption='RAW',
            body=body
        ).execute()
    
    # Append rows
    if rows:
        range_name = f"{sheet_name}!A:P"
        body = {'values': rows}
        sheets_service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        logger.info(f"Appended {len(rows)} rows to {sheet_name} sheet")

async def process_new_data(sheets_service, start_time: datetime, end_time: datetime, existing_call_ids: set) -> int:
    """Fetch and process new data from Ringba, filtering out duplicates"""
    logger.info(f"Fetching data from {start_time} to {end_time} UTC")
    
    # Fetch data from Ringba
    calls = await fetch_ringba_data(start_time, end_time)
    
    if not calls:
        logger.debug("No calls found from Ringba API")
        return 0
    
    # Filter out duplicates
    new_calls = [call for call in calls if call.get("call_id") not in existing_call_ids]
    
    if not new_calls:
        logger.debug(f"All {len(calls)} calls already exist in sheet, skipping")
        return 0
    
    logger.info(f"Found {len(new_calls)} new calls (out of {len(calls)} total)")
    
    # Prepare rows for sheets
    rows = prepare_rows_for_sheets(new_calls)
    
    # Write to Google Sheets
    await write_to_sheets(sheets_service, rows, SHEET_NAME)
    
    # Update existing_call_ids set with new call_ids
    for call in new_calls:
        call_id = call.get("call_id")
        if call_id:
            existing_call_ids.add(call_id)
    
    return len(new_calls)

async def main():
    """Main function - runs continuously, pulling new data as it arrives"""
    logger.info("Starting Ringba data pull to Google Sheets (continuous mode)")
    logger.info(f"Poll interval: {POLL_INTERVAL} seconds")
    
    # Check environment variables
    if not RINGBA_API_TOKEN:
        logger.error("RINGBA_API_TOKEN environment variable is required")
        return
    if not RINGBA_ACCOUNT_ID:
        logger.error("RINGBA_ACCOUNT_ID environment variable is required")
        return
    
    # Log account ID for verification
    logger.info(f"✅ Ringba Account ID configured: {RINGBA_ACCOUNT_ID}")
    logger.info(f"✅ Ringba API Token configured: {'*' * 20}...{RINGBA_API_TOKEN[-10:] if len(RINGBA_API_TOKEN) > 10 else '***'}")
    if not GOOGLE_CREDENTIALS_JSON:
        logger.error("GOOGLE_CREDENTIALS_JSON environment variable is required")
        return
    if not MASTER_CPA_DATA:
        logger.error("MASTER_CPA_DATA environment variable is required")
        return
    
    # Initialize Google Sheets
    try:
        sheets_service = init_google_sheets()
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets: {e}")
        return
    
    # Get existing call_ids to avoid duplicates
    existing_call_ids = get_existing_call_ids(sheets_service, SHEET_NAME)
    
    # Get latest timestamp to start from
    latest_timestamp = get_latest_timestamp(sheets_service, SHEET_NAME)
    
    # Initial sync: if we have existing data, start from latest timestamp
    # Otherwise, do a lookback period
    if latest_timestamp:
        start_time = latest_timestamp - timedelta(minutes=5)  # 5 minute overlap to catch any missed calls
        logger.info(f"Resuming from latest timestamp: {start_time}")
    else:
        # First run: look back specified hours
        start_time = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        logger.info(f"First run: looking back {LOOKBACK_HOURS} hours from {start_time}")
    
    # Main loop: continuously poll for new data
    iteration = 0
    while True:
        try:
            iteration += 1
            logger.info(f"=== Poll iteration {iteration} ===")
            
            # Set end time to now
            end_time = datetime.now(timezone.utc)
            
            # Process new data
            new_count = await process_new_data(sheets_service, start_time, end_time, existing_call_ids)
            
            if new_count > 0:
                logger.info(f"✅ Processed {new_count} new calls")
            else:
                logger.debug("No new calls to process")
            
            # Update start_time for next iteration (with small overlap)
            start_time = end_time - timedelta(minutes=1)  # 1 minute overlap
            
            # Wait before next poll
            logger.info(f"Waiting {POLL_INTERVAL} seconds before next poll...")
            await asyncio.sleep(POLL_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            logger.info(f"Waiting {POLL_INTERVAL} seconds before retry...")
            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())

