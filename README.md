# CPA Monitor

Automated Ringba performance monitoring system that tracks CPA and sends summaries to Slack during business hours (9am-8pm EST, Monday-Saturday).

## Features

- 🕐 **Business Hours Monitoring**: Runs 9am-8pm EST, Monday-Saturday
- 📊 **Dual View Reports**: Shows both 2-hour and daily accumulated data
- 💰 **CPA Calculation**: Revenue ÷ Completed Calls
- 📱 **Slack Integration**: Rich formatted summaries
- 📈 **Performance Tracking**: All Ringba dashboard metrics
- 🏆 **Top Performers**: Highlights best publishers
- ⏰ **Smart Scheduling**: First report at 11am EST, then every 2 hours

## Quick Start

1. **Set Environment Variables:**
   ```bash
   RINGBA_API_TOKEN=your_ringba_api_token
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK
   ```

2. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Monitor:**
   ```bash
   python monitor.py
   ```

## Deployment

### Render.com
1. Connect this repository to Render
2. Set environment variables in Render dashboard
3. Deploy - it will run automatically

### Local/Other
```bash
python start_monitor.py
```

## Files

- `monitor.py` - Main monitoring system
- `ringba_pull_to_sheets.py` - Continuous Ringba data puller to Google Sheets
- `test_monitor.py` - Test script
- `deploy_monitor.py` - Deployment helper
- `start_monitor.py` - Easy startup
- `monitor_config.env` - Configuration template
- `render.yaml` - Render deployment config (for monitor.py)
- `render_ringba_pull.yaml` - Render deployment config (for ringba_pull_to_sheets.py)

## Documentation

See `MONITOR_README.md` for complete documentation.

## Environment Variables

### For monitor.py:
| Variable | Required | Description |
|----------|----------|-------------|
| `RINGBA_API_TOKEN` | Yes | Your Ringba API token |
| `SLACK_WEBHOOK_URL` | Yes | Slack webhook URL |
| `RINGBA_ACCOUNT_ID` | No | Ringba account ID (defaults to provided) |

### For ringba_pull_to_sheets.py:
| Variable | Required | Description |
|----------|----------|-------------|
| `RINGBA_API_TOKEN` | Yes | Your Ringba API token |
| `RINGBA_ACCOUNT_ID` | Yes | Your Ringba account ID |
| `GOOGLE_CREDENTIALS_JSON` | Yes | Google Service Account JSON credentials |
| `MASTER_CPA_DATA` | Yes | Google Sheets ID or URL |
| `RINGBA_CAMPAIGNS` | Yes | Comma-separated list of campaigns to track |
| `SHEET_NAME` | No | Sheet tab name (default: "CPA Reporting") |
| `POLL_INTERVAL` | No | Poll interval in seconds (default: 60) |
| `LOOKBACK_HOURS` | No | Hours to look back on first run (default: 24) |

## Support

This monitoring system works alongside your existing Ringba webhook system without any conflicts.