# Test — Backfill for Main Entrypoin

- Manually fetch historical Google Ads data outside predefined `MODE` window
- Read required environment variables `COMPANY`, `PROJECT`, `DEPARTMENT`, `ACCOUNT`
- Accept `start_date` and `end_date` from CLI
- CLI usage example: 

```bash
$env:PROJECT ="seer-digital-ads"; 
$env:COMPANY="kids"; 
$env:PLATFORM="facebook"; 
$env:DEPARTMENT="marketing"; 
$env:ACCOUNT="main"; 
python -m backfill.backfill_campaign_insights --start_date=2026-02-02 --end_date=2026-02-02
```