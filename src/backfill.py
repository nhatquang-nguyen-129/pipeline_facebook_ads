"""
==================================================================
FACEBOOK BACKFILL MODULE
------------------------------------------------------------------
This module handles **historical data ingestion** for Facebook  
Ads campaign-level insights, designed to backfill gaps in daily  
campaign data into Google BigQuery raw tables.

It scans a given date range, checks for missing records by date,  
and fetches only the required data via Facebook Marketing API to  
ensure storage efficiency and API quota optimization.

✔️ Detects and ingests only missing daily records in BigQuery  
✔️ Ingests associated campaign metadata for updated campaigns  
✔️ Automatically rebuilds staging and mart tables upon ingestion  

⚠️ This module is intended for **historical data recovery** only.  
It does **not** handle schema migration, freshness revalidation,  
or partial updates for existing records (see `update_campaign_insights`).
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))  

# Add utilities for logging and time management
from time import time

# Add utilities for logging and error tracking
import logging

# Add external Python Pandas libraries for data processing
import pandas as pd

# Add external Google Cloud SDK libraries for cloud computation
from google.cloud.exceptions import NotFound

# Add internal Google BigQuery module for integration
from infrastructure.bigquery.reader import get_date_existing

# Add internal Facebook Ads module for handling
from src.ingest import (
    ingest_campaign_insights,
    ingest_ad_insights,
    ingest_campaign_metadata,
    ingest_ad_creatives
)
from src.staging import (
    staging_campaign_insights,
    staging_ad_insights
)
from src.mart import (
    mart_campaign_spend,
    mart_campaign_performance,
    mart_creative_daily
)
from collections import defaultdict

# Get Google Cloud Project ID environment variable
PROJECT = os.getenv("GCP_PROJECT_ID")

# Get Facebook service environment variable for Brand
COMPANY = os.getenv("COMPANY") 

# Get Facebook service environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get Facebook service environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# 1. BACKFILL FACEBOOK ADS DATA FOR A GIVEN DATE RANGE

# 1.1. Backfill historical Facebook campaign insights for a given date range
def backfill_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [BACKFILL] Starting Facebook Ads campaign insights backfill from {start_date} to {end_date}...")
    logging.info(f"🚀 [BACKFILL] Starting Facebook Ads campaign insights backfill from {start_date} to {end_date}...")

    # 1.1.1. Start timing the update process to measure total execution duration
    start_time = time.time()

    # 1.1.2. Validate environment variables
    if not all([COMPANY, PLATFORM, ACCOUNT]):
        raise ValueError("❌ [BACKFILL] Missing Facebook Ads BRAND/PLATFORM/ACCOUNT in environment variables.")
    try:
        _ = MAPPING_FACEBOOK_SECRET[COMPANY][PLATFORM]["account"][ACCOUNT]
    except KeyError:
        raise ValueError("❌ [BACKFILL] Invalid BRAND/PLATFORM/ACCOUNT in MAPPING_FACEBOOK_SECRET.")

    # 1.1.3. Initialize Google BigQuery client and Facebook SDK session
    try:
        client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
    init_facebook_sdk()

    # 1.1.4. Iterate over input date range to verify table existance
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_months = set()
    updated_campaign_ids = set()

    # 1.1.5. Iterate over input date range to verify date existance
    existing_dates_map = defaultdict(set)
    for date in date_range:
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{get_facebook_dataset('raw')}.campaign_all_m{m:02d}{y}"

        print(f"🔎 [BACKFILL] Evaluating existing date(s) in Facebook campaign insights table {table_id}...")
        logging.info(f"🔎 [BACKFILL] Evaluating existing date(s) in Facebook campaign insights table {table_id}...")

        if (y, m) not in existing_dates_map:
            try:
                existing_dates_map[(y, m)] = get_date_existing(client, table_id, "date_range")
                print(f"✅ [BACKFILL] Successfully loaded existing dates for Facebook campaign insights table {table_id}.")
                logging.info(f"✅ [BACKFILL] Successfully loaded existing dates for Facebook campaign insights table {table_id}.")
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to load existing dates for Facebook campaign insights table {table_id} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to load existing dates for Facebook campaign insights table {table_id} due to {e}.")
                existing_dates_map[(y, m)] = set()

    # 1.1.6. Iterate missing date(s) to ingest
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        key = f"{day_str}_to_{day_str}"
        y, m = date.year, date.month

        if key in existing_dates_map[(y, m)]:
            print(f"🔄 [BACKFILL] Skipping {day_str} existing date(s) for Facebook campaign insights table that already exists...")
            logging.info(f"🔄 [BACKFILL] Skipping {day_str} existing date(s) for Facebook campaign insights table that already exists...")
            continue
    
    # 1.1.7. Ingest Facebook campaign insights from Facebook API to Google BigQuery raw tables
        print(f"🔄 [BACKFILL] Triggering Facebook campaign insights ingestion for {day_str}...")
        logging.info(f"🔄 [BACKFILL] Triggering Facebook campaign insights ingestion for {day_str}...")
        
        try:
            df = ingest_campaign_insights(
                start_date=day_str,
                end_date=day_str,
                write_disposition="WRITE_APPEND"
            )
            updated_months.add((y, m))
            if "campaign_id" in df.columns:
                updated_campaign_ids.update(df["campaign_id"].dropna().unique())
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")

    # 1.1.8. Ingest Facebook campaign metadata from Facebook API to Google BigQuery raw tables
    if updated_campaign_ids:
        print(f"🔍 [BACKFILL] Triggering Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s)...")
        logging.info(f"🔍 [BACKFILL] Triggering Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s)...")
        try:
            ingest_campaign_metadata(campaign_id_list=list(updated_campaign_ids))
            print(f"✅ [BACKFILL] Successfully ingested Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s).")
            logging.info(f"✅ [BACKFILL] Successfully ingested Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s).")
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to ingest Facebook campaign metadata ingest for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to ingest Facebook campaign metadata ingest for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
    else:
        print("⚠️ [BACKFILL] No campaign_ids found for Facebook campaign metadata backfill then ingestion will be skipped.")
        logging.warning("⚠️ [BACKFILL] No campaign_ids found for Facebook campaign metadata backfill then ingestion will be skipped.")
    
    # 1.1.9 Rebuild Facebook campaign insights staging table
    if updated_months:
        print("🔄 [BACKFILL] Triggering to rebuild staging table for Facebook campaign insights...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild staging table for Facebook campaign insights...")
        try:
            staging_campaign_insights()
            print("✅ [BACKFILL] Successfully rebuilt staging table for Facebook campaign insights: campaign_all_insights.")
            logging.info("✅ [BACKFILL] Successfully rebuilt staging table for Facebook campaign insights: campaign_all_insights.")
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to rebuild staging table: {e}")
    else:
        print("⚠️ [BACKFILL] No updated for Facebook campaign insights then staging table rebuild is skipped.")
        logging.warning("⚠️ [BACKFILL] No updated for Facebook campaign insights then staging table rebuild is skipped.")

    # 1.1.10. Rebuild Facebook materialized tables for campaign performance
    if updated_months:
        print("🔄 [BACKFILL] Triggering to rebuild materialized table for Facebook campaign performance...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild materialized table for Facebook campaign performance...")
        try:
            mart_campaign_performance()
            print("✅ [BACKFILL] Successfully rebuilt materialized table for Facebook campaign performance: mart_campaign_spend.")
            logging.info("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign performance: mart_campaign_spend.")
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to rebuild materialized table for Facebook campaign performance due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to rebuild materialized table for Facebook campaign performance due to {e}.")
    
    # 1.1.11. Rebuild Facebook materialized tables for campaign spending      
        print("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook campaign spending...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook campaign spending...")
        try:
            mart_campaign_spend()
            print("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign spending: mart_campaign_spending.")
            logging.info("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign spending: mart_campaign_spending.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook campaign spending due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook campaign spending due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook campaign insights then skip building materialized tables.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook campaign insights then skip building materialized tables.")

    # 1.1.12. Measure the total execution time of Facebook campaign insights update process
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [BACKFILL] Successfully completed Facebook Ads campaign insights update in {elapsed}s.")
    logging.info(f"✅ [BACKFILL] Successfully completed Facebook Ads campaign insights update in {elapsed}s.")

# Hoàn thành
def run_backfill_ad(start_date: str, end_date: str):
    """
    Hàm backfill toàn diện cho dữ liệu Facebook Ads gồm 3 tầng:
    1. Raw (ad_all)
    2. Creative (creative_all)
    3. Staging (ad staging)
    
    Quy trình xử lý:
    1. Kiểm tra dữ liệu raw theo ngày (date_range)
       → Nếu thiếu thì gọi ingest
    2. Luôn kiểm tra sự tồn tại bảng creative & staging
       → Nếu thiếu bảng thì ingest/truncate
    3. Tạo lại staging theo tháng đã update
    4. Rebuild MART cuối cùng
    """

    print("\n📡 [BACKFILL] Starting ad-level backfill process...")
    logging.info("📡 [BACKFILL] Starting ad-level backfill process...")

    # === [1] Khởi tạo thông tin môi trường và SDK ===
    try:
        client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
    brand = os.environ.get("BRAND")
    platform = os.environ.get("PLATFORM")
    account_key = os.environ.get("ACCOUNT_KEY")

    if not all([brand, platform, account_key]):
        raise ValueError("❌ Thiếu biến môi trường BRAND / PLATFORM / ACCOUNT_KEY")

    try:
        ad_account_id = MAPPING_FACEBOOK_SECRET[brand][platform]["accounts"][account_key]["ad_account_id"]
    except KeyError:
        raise ValueError("❌ Sai thông tin mapping từ MAPPING_FACEBOOK_SECRET")

    init_facebook_sdk()

    # === [2] Lấy danh sách ngày và nhóm theo tháng ===
    date_range = pd.date_range(start=start_date, end=end_date)
    print(f"🗓️  Ingesting range: {date_range[0].date()} → {date_range[-1].date()}")
    logging.info(f"🗓️  Ingesting range: {date_range[0].date()} → {date_range[-1].date()}")

    raw_dataset = get_facebook_dataset("raw")
    staging_dataset = get_facebook_dataset("staging")

    # === [3] Kiểm tra dữ liệu đã tồn tại trong bảng raw ===
    existing_dates_map = defaultdict(set)

    # Lấy danh sách các cặp (năm, tháng) duy nhất từ date_range
    unique_months = sorted(set((d.year, d.month) for d in date_range))

    for y, m in unique_months:
        key = (y, m)
        table_id = f"{PROJECT}.{raw_dataset}.ad_all_m{m:02d}{y}"
        try:
            existing_dates_map[key] = get_date_existing(client, table_id, "date_range")
            print(f"✅ Retrieved existing dates for {table_id}")
            logging.info(f"✅ Retrieved existing dates for {table_id}")
        except Exception as e:
            print(f"⚠️ Failed to load existing dates for {table_id}: {e}")
            logging.warning(f"⚠️ Failed to load existing dates for {table_id}: {e}")
            existing_dates_map[key] = set()

    # === [4] Ingest dữ liệu raw nếu thiếu từng ngày ===
    ingest_count = 0
    updated_months = set()
    start_timer = time()

    for date in date_range:
        y, m = date.year, date.month
        day_str = date.strftime("%Y-%m-%d")
        key = (y, m)
        date_key = f"{day_str}_to_{day_str}"

        if date_key not in existing_dates_map[key]:
            print(f"🚀 Ingesting raw ad for {date_key}")
            try:
                ingest_ad_insights(
                    start_date=day_str,
                    end_date=day_str,
                    write_disposition="WRITE_APPEND"
                )
                ingest_count += 1
                updated_months.add(key)
            except Exception as e:
                print(f"❌ Ingest failed for {day_str}: {e}")
        else:
            print(f"✅ Skipped {date_key} (already exists)")

    elapsed = round(time() - start_timer, 2)
    print(f"⏱️ Raw ingest completed in {elapsed}s with {ingest_count} new days")

    # === [5] Kiểm tra thiếu bảng creative / staging → thêm vào updated_months
    for y, m in {(d.year, d.month) for d in date_range}:
        table_creative = f"{PROJECT}.{raw_dataset}.creative_all_m{m:02d}{y}"
        table_staging = f"{PROJECT}.{staging_dataset}.ad_all_m{m:02d}{y}"

        try:
            client.get_table(table_creative)
        except NotFound:
            print(f"⚠️ Missing creative table m{m:02d}{y}, will ingest")
            updated_months.add((y, m))

        try:
            client.get_table(table_staging)
        except NotFound:
            print(f"⚠️ Missing staging table m{m:02d}{y}, will rebuild")
            updated_months.add((y, m))

    # === [6] Ingest lại toàn bộ creative theo từng tháng bị ảnh hưởng ===
    for y, m in sorted(updated_months):
        print(f"🔄 Re-ingesting creatives for m{m:02d}{y} (WRITE_TRUNCATE)...")
        logging.info(f"🔄 Re-ingesting creatives for m{m:02d}{y} (WRITE_TRUNCATE)...")
        try:
            ingest_ad_creatives(year=y, month=m, write_disposition="WRITE_TRUNCATE")
        except Exception as e:
            print(f"❌ Failed to ingest creative m{m:02d}{y}: {e}")
            logging.info(f"❌ Failed to ingest creative m{m:02d}{y}: {e}")

    # === [7] Rebuild staging cho các tháng đã cập nhật ===
    for y, m in sorted(updated_months):
        print(f"📦 Rebuilding staging table for m{m:02d}{y}...")
        try:
            staging_ad_insights(year=y, month=m)
        except Exception as e:
            print(f"❌ Failed to rebuild staging for m{m:02d}{y}: {e}")

    # === [8] Rebuild MART cuối cùng ===
    print("📊 Rebuilding ad creative MART...")
    try:
        mart_creative_daily(client)
    except Exception as e:
        print(f"❌ Failed to rebuild MART: {e}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Facebook Campaign Backfill")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    # Validate ENV
    brand = os.environ.get("BRAND")
    platform = os.environ.get("PLATFORM")
    account_key = os.environ.get("ACCOUNT_KEY")

    if not all([brand, platform, account_key]):
        raise EnvironmentError("❌ Please set BRAND, PLATFORM, and ACCOUNT_KEY environment variables.")

    # Run backfill
    run_backfill_ad(start_date=args.start_date, end_date=args.end_date)
