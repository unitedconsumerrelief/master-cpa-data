import os
import json
import sqlite3
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any
from urllib.parse import urlparse
import re

from fastapi import FastAPI, HTTPException, Request, Header, Query, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import google.auth
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Ringba Webhook", version="1.0.0")

# Global variables
sheets_service = None
realtime_dids: Set[str] = set()
background_writer_queue = []
background_writer_lock = asyncio.Lock()
db_path = "/tmp/ringba.sqlite"

# Memory optimization: Limit queue size to prevent memory issues
MAX_QUEUE_SIZE = 100

# Environment variables
RINGBA_CAMPAIGNS = set(os.getenv("RINGBA_CAMPAIGNS", "").split(",")) if os.getenv("RINGBA_CAMPAIGNS") else set()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
MASTER_CPA_DATA = os.getenv("MASTER_CPA_DATA")

# Pydantic models
class RingbaWebhookData(BaseModel):
    call_id: str
    call_start_utc: str
    did_raw: str
    caller_id: Optional[str] = None
    duration_sec: Optional[int] = None
    disposition: Optional[str] = None
    campaign: Optional[str] = None
    target: Optional[str] = None
    publisher_id: Optional[str] = None
    publisher_name: Optional[str] = None
    payout: Optional[float] = None
    revenue: Optional[float] = None

class HealthResponse(BaseModel):
    ok: bool
    realtime_dids: int

# Utility functions
def normalize_did(did: str) -> str:
    """Extract last 10 digits from DID"""
    digits_only = re.sub(r'\D', '', did)
    return digits_only[-10:] if len(digits_only) >= 10 else digits_only

def extract_sheet_id(url_or_id: str) -> str:
    """Extract sheet ID from URL or return as-is if already an ID"""
    if url_or_id.startswith("http"):
        # Extract ID from Google Sheets URL
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url_or_id)
        return match.group(1) if match else url_or_id
    return url_or_id

def init_database():
    """Initialize SQLite database for deduplication"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_calls (
            call_id TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def is_duplicate(call_id: str) -> bool:
    """Check if call_id has already been processed"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_calls WHERE call_id = ?", (call_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def mark_processed(call_id: str):
    """Mark call_id as processed"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO processed_calls (call_id) VALUES (?)", (call_id,))
    conn.commit()
    conn.close()

def init_google_sheets():
    """Initialize Google Sheets service"""
    global sheets_service
    
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
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets service: {e}")
        raise

def get_sheet_id():
    """Get the sheet ID from environment variable"""
    if not MASTER_CPA_DATA:
        raise ValueError("MASTER_CPA_DATA environment variable is required")
    return extract_sheet_id(MASTER_CPA_DATA)

async def ensure_headers_exist(sheet_name: str, headers: List[str]):
    """Ensure headers exist in the specified sheet"""
    try:
        sheet_id = get_sheet_id()
        range_name = f"{sheet_name}!A1"
        
        # Run synchronous Google API calls in thread pool with timeout
        loop = asyncio.get_event_loop()
        
        # Check if headers exist
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: sheets_service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=range_name
                ).execute()
            ),
            timeout=5.0
        )
        
        values = result.get('values', [])
        if not values or values[0] != headers:
            # Headers don't exist or are different, add them
            body = {
                'values': [headers]
            }
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: sheets_service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        body=body
                    ).execute()
                ),
                timeout=5.0
            )
            logger.info(f"Added headers to {sheet_name} sheet")
    except asyncio.TimeoutError:
        logger.error(f"Timeout ensuring headers exist for {sheet_name} sheet")
    except Exception as e:
        logger.error(f"Failed to ensure headers exist for {sheet_name}: {e}")

async def append_to_sheet(sheet_name: str, rows: List[List[Any]]):
    """Append rows to the specified sheet with timeout handling"""
    try:
        sheet_id = get_sheet_id()
        range_name = f"{sheet_name}!A:N"
        
        body = {
            'values': rows
        }
        
        # Use asyncio timeout to prevent hanging
        try:
            # Run the synchronous Google API call in a thread pool with timeout
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: sheets_service.spreadsheets().values().append(
                        spreadsheetId=sheet_id,
                        range=range_name,
                        valueInputOption='RAW',
                        insertDataOption='INSERT_ROWS',
                        body=body
                    ).execute()
                ),
                timeout=10.0  # 10 second timeout
            )
            logger.info(f"Appended {len(rows)} rows to {sheet_name} sheet")
        except asyncio.TimeoutError:
            logger.error(f"Timeout writing to {sheet_name} sheet - operation took too long")
            raise
    except Exception as e:
        logger.error(f"Failed to append to {sheet_name} sheet: {e}")
        raise

async def read_sheet_data(sheet_name: str) -> List[List[Any]]:
    """Read all data from the specified sheet"""
    try:
        sheet_id = get_sheet_id()
        range_name = f"{sheet_name}!A:N"
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        return result.get('values', [])
    except Exception as e:
        logger.error(f"Failed to read from {sheet_name} sheet: {e}")
        return []

async def write_sheet_data(sheet_name: str, data: List[List[Any]]):
    """Write data to the specified sheet (overwrites existing data)"""
    try:
        sheet_id = get_sheet_id()
        range_name = f"{sheet_name}!A:N"
        
        body = {
            'values': data
        }
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logger.info(f"Wrote {len(data)} rows to {sheet_name} sheet")
    except Exception as e:
        logger.error(f"Failed to write to {sheet_name} sheet: {e}")

async def background_writer():
    """Background task to flush queued data to Google Sheets"""
    while True:
        try:
            await asyncio.sleep(2)  # Reduced from 5 to 2 seconds for faster processing
            
            async with background_writer_lock:
                # Check if there are any items to process (inside the lock)
                if not background_writer_queue:
                    continue
                
                # Ensure headers exist
                headers = ["call_id","call_start_utc","did_raw","did_canon","caller_id",
                         "duration_sec","disposition","campaign","target",
                         "publisher_id","publisher_name",
                         "payout","revenue","_ingested_at"]
                await ensure_headers_exist("Ringba Raw", headers)
                
                # Flush up to 25 rows (reduced from 50 to prevent memory issues)
                rows_to_flush = background_writer_queue[:25]
                background_writer_queue[:] = background_writer_queue[25:]
                
                if rows_to_flush:
                    logger.info(f"📊 WRITING TO SHEETS: {len(rows_to_flush)} rows")
                    # Log first few call IDs being written
                    call_ids = [row[0] for row in rows_to_flush[:5]]
                    logger.info(f"📋 Call IDs being written: {call_ids}")
                    
                    await append_to_sheet("Ringba Raw", rows_to_flush)
                    logger.info(f"✅ Successfully flushed {len(rows_to_flush)} rows to Google Sheets")
                        
        except Exception as e:
            logger.error(f"❌ Error in background writer: {e}")
            # Add small delay on error to prevent rapid retry loops
            await asyncio.sleep(5)

async def background_cache():
    """Background task to maintain realtime DIDs cache"""
    while True:
        try:
            await asyncio.sleep(300)  # Wait 5 minutes
            
            # Read Real Time tab
            data = await read_sheet_data("Real Time")
            if data and len(data) > 1:  # Skip header row
                global realtime_dids
                realtime_dids = set()
                for row in data[1:]:  # Skip header
                    if row and len(row) > 0:
                        # Assuming first column contains normalized DIDs
                        did = str(row[0]).strip()
                        if did:
                            realtime_dids.add(did)
                
                logger.info(f"Updated realtime DIDs cache: {len(realtime_dids)} DIDs")
                
        except Exception as e:
            logger.error(f"Error in background cache: {e}")

# Security dependencies (removed for simplicity)

# API Endpoints
@app.post("/ringba-webhook")
@app.post("/ringba-webhook1")  # Support both endpoints
async def ringba_webhook(request: Request):
    """Handle incoming Ringba webhook data"""
    try:
        # Get raw body for debugging
        raw_body = await request.body()
        raw_text = raw_body.decode('utf-8', errors='replace')
        logger.info(f"📥 RAW WEBHOOK BODY: {raw_text}")
        
        # Fix malformed JSON from Ringba (empty values without proper JSON formatting)
        # Replace empty values like "durationSec": , with "durationSec": null,
        import re
        
        # Try to parse first, if it fails, apply fixes
        try:
            body = json.loads(raw_text)
        except json.JSONDecodeError:
            # Apply multiple fix patterns for malformed JSON
            fixed_json = raw_text
            # Pattern 1: ": " followed immediately by comma
            fixed_json = re.sub(r':\s*,', ': null,', fixed_json)
            # Pattern 2: ": " followed by closing brace
            fixed_json = re.sub(r':\s*}', ': null}', fixed_json)
            # Pattern 3: ": " at end of line followed by comma/brace on next line
            fixed_json = re.sub(r':\s*\r?\n\s*([,}])', r': null\n\1', fixed_json)
            # Pattern 4: ": " with whitespace before comma/brace (catch remaining cases)
            fixed_json = re.sub(r':\s+([,}])', r': null\1', fixed_json)
            
            # Try parsing again
            try:
                body = json.loads(fixed_json)
            except json.JSONDecodeError as e:
                logger.error(f"❌ Could not fix malformed JSON: {e}")
                logger.error(f"Original: {raw_text[:500]}")
                logger.error(f"Fixed attempt: {fixed_json[:500]}")
                return {"status": "error", "message": "Invalid JSON format - could not fix"}
        
        # Log incoming webhook for debugging
        logger.info(f"📥 WEBHOOK RECEIVED: {json.dumps(body, indent=2)}")
        
        # Extract call data
        call_id = body.get("call_id")
        if not call_id:
            logger.error("❌ Missing call_id in webhook")
            raise HTTPException(status_code=400, detail="Missing call_id")
        
        # Check for duplicates
        if is_duplicate(call_id):
            logger.warning(f"🔄 Duplicate call_id ignored: {call_id}")
            return {"status": "duplicate"}
        
        # Normalize DID
        did_raw = body.get("did", "")
        did_canon = normalize_did(did_raw)
        
        # Get campaign info (no filtering - accept all calls from Ringba)
        campaign_name = body.get("campaignName", "")
        campaign_id = body.get("campaignId", "")
        caller_id = body.get("callerId", "")
        
        # Log call details
        logger.info(f"📞 PROCESSING CALL: ID={call_id}, Caller={caller_id}, Campaign={campaign_name or campaign_id}, Publisher={body.get('publisherName', 'Unknown')}")
        
        # Note: We write ALL calls to the sheet, regardless of value
        # This allows for complete data tracking and analysis
        target = body.get("target", "").strip()
        payout = body.get("payout", 0) or 0
        revenue = body.get("revenue", 0) or 0
        
        # Log call value status but don't filter
        if not target or payout == 0 or revenue == 0:
            logger.info(f"ℹ️  Call with no value - Target: '{target}', Payout: {payout}, Revenue: {revenue} - Writing to sheet anyway")
        else:
            logger.info("✅ Call has value - processing")
        
        # Prepare row data with safe value extraction
        def safe_get(key, default=""):
            value = body.get(key, default)
            # Handle None values and empty strings
            if value is None or value == "":
                return default
            return value
        
        row = [
            call_id,
            safe_get("callStartUtc", ""),
            did_raw,
            did_canon,
            caller_id,
            safe_get("durationSec", ""),
            safe_get("disposition", ""),
            campaign_name or campaign_id,
            safe_get("target", ""),
            safe_get("publisherId", ""),
            safe_get("publisherName", ""),
            safe_get("payout", ""),
            safe_get("revenue", ""),
            datetime.now(timezone.utc).isoformat()
        ]
        
        # Write directly to sheet (no background worker to avoid timeouts and costs)
        try:
            # Ensure headers exist first
            headers = ["call_id","call_start_utc","did_raw","did_canon","caller_id",
                     "duration_sec","disposition","campaign","target",
                     "publisher_id","publisher_name",
                     "payout","revenue","_ingested_at"]
            await ensure_headers_exist("Ringba Raw", headers)
            
            # Write directly to sheet
            await append_to_sheet("Ringba Raw", [row])
            logger.info(f"✅ Successfully wrote call to sheet: {call_id}")
        except Exception as e:
            logger.error(f"❌ Error writing to sheet: {e}")
            # Don't fail the webhook - just log the error
            # The pull script will catch this data later
        
        # Mark as processed
        mark_processed(call_id)
        
        return {"status": "success"}
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON Parse Error: {e}")
        logger.error(f"Raw body: {raw_body.decode('utf-8', errors='replace') if 'raw_body' in locals() else 'Not available'}")
        return {"status": "error", "message": "Invalid JSON format"}
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}")
        logger.error(f"Request body: {body if 'body' in locals() else 'Failed to parse'}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/refresh-map")
async def refresh_map():
    """Build DID→Publisher map and update sheets"""
    try:
        # Read Ringba Raw data
        raw_data = await read_sheet_data("Ringba Raw")
        if not raw_data or len(raw_data) <= 1:
            raise HTTPException(status_code=400, detail="No data found in Ringba Raw sheet")
        
        headers = raw_data[0]
        rows = raw_data[1:]
        
        # Find column indices
        call_id_idx = headers.index("call_id") if "call_id" in headers else 0
        call_start_idx = headers.index("call_start_utc") if "call_start_utc" in headers else 1
        did_canon_idx = headers.index("did_canon") if "did_canon" in headers else 3
        publisher_name_idx = headers.index("publisher_name") if "publisher_name" in headers else 10
        publisher_id_idx = headers.index("publisher_id") if "publisher_id" in headers else 9
        campaign_idx = headers.index("campaign") if "campaign" in headers else 7
        
        # Build DID→Publisher map (latest call per DID)
        did_publisher_map = {}
        publisher_did_counts = {}
        
        for row in rows:
            if len(row) <= max(call_id_idx, call_start_idx, did_canon_idx, publisher_name_idx, publisher_id_idx, campaign_idx):
                continue

            did_canon = str(row[did_canon_idx]).strip()
            campaign = str(row[campaign_idx]).strip()
            
            # Only process DIDs in realtime cache and whitelisted campaigns
            if did_canon in realtime_dids and (not RINGBA_CAMPAIGNS or campaign in RINGBA_CAMPAIGNS):
                call_id = row[call_id_idx]
                call_start = row[call_start_idx]
                publisher_name = str(row[publisher_name_idx]).strip()
                publisher_id = str(row[publisher_id_idx]).strip()
                
                # Keep latest call per DID
                if did_canon not in did_publisher_map or call_start > did_publisher_map[did_canon]["last_seen_call_start"]:
                    did_publisher_map[did_canon] = {
                        "publisher_name": publisher_name,
                        "publisher_id": publisher_id,
                        "last_seen_call_start": call_start
                    }
        
        # Prepare DID Publisher Map data
        did_map_headers = ["did_canon", "publisher_name", "publisher_id", "last_seen_call_start"]
        did_map_data = [did_map_headers]
        
        for did_canon, info in did_publisher_map.items():
            did_map_data.append([
                did_canon,
                info["publisher_name"],
                info["publisher_id"],
                info["last_seen_call_start"]
            ])
            
            # Count DIDs per publisher
            publisher = info["publisher_name"]
            publisher_did_counts[publisher] = publisher_did_counts.get(publisher, 0) + 1
        
        # Prepare Publisher DID Counts data
        counts_headers = ["publisher", "did_count", "last_refreshed"]
        counts_data = [counts_headers]
        
        for publisher, count in publisher_did_counts.items():
            counts_data.append([
                publisher,
                count,
                datetime.now(timezone.utc).isoformat()
            ])
        
        # Write to sheets
        await write_sheet_data("DID Publisher Map", did_map_data)
        await write_sheet_data("Publisher DID Counts", counts_data)
        
        logger.info(f"Refreshed DID map: {len(did_publisher_map)} DIDs, {len(publisher_did_counts)} publishers")
        return {
            "status": "success",
            "did_count": len(did_publisher_map),
            "publisher_count": len(publisher_did_counts)
        }
        
    except Exception as e:
        logger.error(f"Error refreshing map: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthz", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(ok=True, realtime_dids=len(realtime_dids))

@app.get("/debug/stats")
async def debug_stats():
    """Debug endpoint to show current statistics"""
    try:
        import psutil
        
        # Count processed calls from database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_calls")
        processed_count = cursor.fetchone()[0]
        conn.close()
        
        # Get queue size
        async with background_writer_lock:
            queue_size = len(background_writer_queue)
        
        # Get memory usage
        memory_info = psutil.virtual_memory()
        memory_usage_mb = memory_info.used / (1024 * 1024)
        memory_percent = memory_info.percent
        
        return {
            "status": "ok",
            "processed_calls": processed_count,
            "queue_size": queue_size,
            "realtime_dids": len(realtime_dids),
            "campaign_filtering": list(RINGBA_CAMPAIGNS) if RINGBA_CAMPAIGNS else "None",
            "google_sheets_configured": bool(GOOGLE_CREDENTIALS_JSON and MASTER_CPA_DATA),
            "memory_usage_mb": round(memory_usage_mb, 2),
            "memory_percent": memory_percent,
            "memory_warning": memory_usage_mb > 400  # Warning if over 400MB
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        # Initialize database
        init_database()
        
        # Initialize Google Sheets
        init_google_sheets()
        
        # Start background tasks (only cache, no writer - writing is done directly in webhook)
        asyncio.create_task(background_cache())
        
        logger.info("Application started successfully")
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
