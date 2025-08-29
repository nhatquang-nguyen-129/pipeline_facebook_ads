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

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add external Google Cloud SDK libraries for cloud computation
from google.cloud.exceptions import NotFound

# Add Google Cloud libraries for integration
from google.cloud import bigquery

# Add internal Facebook module for handling
from src.update import (
    update_campaign_insights,
    update_ad_insights
)

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

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
    print(f"🚀 [BACKFILL] Starting backfill for Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [BACKFILL] Starting backfill for Facebook campaign insights from {start_date} to {end_date}...")

    # 1. Tạo date_range
    date_range = pd.date_range(start=start_date, end=end_date)
    missing_dates = []

    # 2. Check trong BQ xem ngày nào đã có dữ liệu
    client = bigquery.Client(project=PROJECT)
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"

    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"

        query = f"""
            SELECT COUNT(1) as cnt
            FROM `{table_id}`
            WHERE date_start = @day_str
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("day_str", "STRING", day_str)]
        )

        try:
            result = list(client.query(query, job_config=job_config).result())
            count = result[0]["cnt"] if result else 0
            if count == 0:
                print(f"⚠️ [BACKFILL] Missing data for {day_str} in {table_id}, will ingest...")
                logging.warning(f"⚠️ [BACKFILL] Missing data for {day_str} in {table_id}, will ingest...")
                missing_dates.append(day_str)
            else:
                print(f"✅ [BACKFILL] Data for {day_str} already exists in {table_id}, skip.")
                logging.info(f"✅ [BACKFILL] Data for {day_str} already exists in {table_id}, skip.")
        except NotFound:
            print(f"⚠️ [BACKFILL] Table {table_id} not found, ingesting {day_str}...")
            logging.warning(f"⚠️ [BACKFILL] Table {table_id} not found, ingesting {day_str}...")
            missing_dates.append(day_str)
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to check table {table_id} for {day_str} due to {e}, will force ingest...")
            logging.error(f"❌ [BACKFILL] Failed to check table {table_id} for {day_str} due to {e}, will force ingest...")
            missing_dates.append(day_str)

    # 3. Ingest bù những ngày thiếu
    if missing_dates:
        for d in missing_dates:
            try:
                update_campaign_insights(d, d)  # dùng lại hàm update 1 ngày
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to backfill {d} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to backfill {d} due to {e}.")
    else:
        print("✅ [BACKFILL] No missing dates detected, nothing to backfill.")
        logging.info("✅ [BACKFILL] No missing dates detected, nothing to backfill.")

# 1.2. Backfill historical Facebook ad insights for a given date range
def backfill_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [BACKFILL] Starting backfill for Facebook ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [BACKFILL] Starting backfill for Facebook ad insights from {start_date} to {end_date}...")

    # 1. Tạo date_range
    date_range = pd.date_range(start=start_date, end=end_date)
    missing_dates = []

    # 2. Check trong BQ xem ngày nào đã có dữ liệu
    client = bigquery.Client(project=PROJECT)
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"

    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"

        query = f"""
            SELECT COUNT(1) as cnt
            FROM `{table_id}`
            WHERE date_start = @day_str
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("day_str", "STRING", day_str)]
        )

        try:
            result = list(client.query(query, job_config=job_config).result())
            count = result[0]["cnt"] if result else 0
            if count == 0:
                print(f"⚠️ [BACKFILL] Missing data for {day_str} in {table_id}, will ingest...")
                logging.warning(f"⚠️ [BACKFILL] Missing data for {day_str} in {table_id}, will ingest...")
                missing_dates.append(day_str)
            else:
                print(f"✅ [BACKFILL] Data for {day_str} already exists in {table_id}, skip.")
                logging.info(f"✅ [BACKFILL] Data for {day_str} already exists in {table_id}, skip.")
        except NotFound:
            print(f"⚠️ [BACKFILL] Table {table_id} not found, ingesting {day_str}...")
            logging.warning(f"⚠️ [BACKFILL] Table {table_id} not found, ingesting {day_str}...")
            missing_dates.append(day_str)
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to check table {table_id} for {day_str} due to {e}, will force ingest...")
            logging.error(f"❌ [BACKFILL] Failed to check table {table_id} for {day_str} due to {e}, will force ingest...")
            missing_dates.append(day_str)

    # 3. Ingest bù những ngày thiếu
    if missing_dates:
        for d in missing_dates:
            try:
                update_ad_insights(d, d)  # dùng lại hàm update 1 ngày
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to backfill {d} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to backfill {d} due to {e}.")
    else:
        print("✅ [BACKFILL] No missing dates detected, nothing to backfill.")
        logging.info("✅ [BACKFILL] No missing dates detected, nothing to backfill.")

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
    backfill_campaign_insights(start_date=args.start_date, end_date=args.end_date)
