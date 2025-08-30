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

# Add external logging libraries for integration
import logging

# Add external Python Pandas libraries for integration
import pandas as pd

# Add external Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add external Google Cloud SDK libraries for integration
from google.cloud.exceptions import NotFound

# Add external Google Cloud libraries for integration
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

# 1. BACKFILL FACEBOOK ADS DATA FOR A GIVEN DATE RANGE

# 1.1. Backfill historical Facebook campaign insights for a given date range
def backfill_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [BACKFILL] Starting Facebook campaign insights backfill from {start_date} to {end_date}...")
    logging.info(f"🚀 [BACKFILL] Starting Facebook campaign insights backfill from {start_date} to {end_date}...")

    # 1.1.1. Validate date range
    date_range = pd.date_range(start=start_date, end=end_date)
    missing_dates = []

    # 1.1.2. Iterate over input date range to verify data existence
    try:
        print(f"🔍 [BACKFILL]] Initializing Google BigQuery client for project {PROJECT}...")
        logging.info(f"🔍 [BACKFILL]] Initializing Google BigQuery client for project {PROJECT}..")
        client = bigquery.Client(project=PROJECT)
        print(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
        logging.info(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Exception as e:
        print(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {str(e)}.")
        logging.error(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {str(e)}.")
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
                print(f"⚠️ [BACKFILL] Facebook campaign insights for {day_str} not found in table {table_id} then ingestion will be proceeding...")
                logging.warning(f"⚠️ [BACKFILL] Facebook campaign insights for {day_str} not found in table {table_id} then ingestion will be proceeding...")
                missing_dates.append(day_str)
            else:
                print(f"✅ [BACKFILL] Facebook campaign insights for {day_str} already exists in {table_id} then ingestion is skipped.")
                logging.info(f"✅ [BACKFILL] Facebook campaign insights for {day_str} already exists in {table_id} then ingestion is skipped.")
        except NotFound:
            print(f"⚠️ [BACKFILL] Facebook campaign insights table {table_id} not found then ingestion for {day_str} will be proceeding...")
            logging.warning(f"⚠️ [BACKFILL] Facebook campaign insights table {table_id} not found then ingestion for {day_str} will be proceeding...")
            missing_dates.append(day_str)
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to verify Facebook campaign insights table {table_id} existence for {day_str} due to {e} then forced ingestion will be proceeding...")
            logging.error(f"❌ [BACKFILL] Failed to verify Facebook campaign insights table {table_id} existence for {day_str} due to {e} then forced ingestion will be proceeding...")
            missing_dates.append(day_str)

    # 1.1.3. Ingest Facebook campaign insights for missing date(s)
    if missing_dates:
        for d in missing_dates:
            try:
                print(f"🔄 [BACKFILL] Triggering Facebook campaign insights update for {d}...")
                logging.info(f"🔄 [BACKFILL] Triggering Facebook campaign insights update for {d}...")
                update_campaign_insights(d, d)
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to trigger Facebook campaign insights update for {d} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to trigger Facebook campaign insights update for {d} due to {e}.")
    else:
        print(f"✅ [BACKFILL] No missing dates detected for Facebook campaign insights from {start_date} to {end_date} then backfill is skipped.")
        logging.info(f"✅ [BACKFILL] No missing dates detected for Facebook campaign insights from {start_date} to {end_date} then backfill is skipped.")

# 1.2. Backfill historical Facebook ad insights for a given date range
def backfill_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [BACKFILL] Starting Facebook ad insights backfill from {start_date} to {end_date}...")
    logging.info(f"🚀 [BACKFILL] Starting Facebook ad insights backfill from {start_date} to {end_date}...")

    # 1.2.1. Validate date range
    date_range = pd.date_range(start=start_date, end=end_date)
    missing_dates = []

    # 1.2.2. Iterate over input date range to verify data existence
    try:
        print(f"🔍 [BACKFILL]] Initializing Google BigQuery client for project {PROJECT}...")
        logging.info(f"🔍 [BACKFILL]] Initializing Google BigQuery client for project {PROJECT}..")
        client = bigquery.Client(project=PROJECT)
        print(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
        logging.info(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Exception as e:
        print(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {str(e)}.")
        logging.error(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {str(e)}.")
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
                print(f"⚠️ [BACKFILL] Facebook ad insights for {day_str} not found in table {table_id} then ingestion will be proceeding...")
                logging.warning(f"⚠️ [BACKFILL] Facebook ad insights for {day_str} not found in table {table_id} then ingestion will be proceeding...")
                missing_dates.append(day_str)
            else:
                print(f"✅ [BACKFILL] Facebook ad insights for {day_str} already exists in {table_id} then ingestion is skipped.")
                logging.info(f"✅ [BACKFILL] Facebook ad insights for {day_str} already exists in {table_id} then ingestion is skipped.")
        except NotFound:
            print(f"⚠️ [BACKFILL] Facebook ad insights table {table_id} not found then ingestion for {day_str} will be proceeding...")
            logging.warning(f"⚠️ [BACKFILL] Facebook ad insights table {table_id} not found then ingestion for {day_str} will be proceeding...")
            missing_dates.append(day_str)
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to verify Facebook ad insights table {table_id} existence for {day_str} due to {e} then forced ingestion will be proceeding...")
            logging.error(f"❌ [BACKFILL] Failed to verify Facebook ad insights table {table_id} existence for {day_str} due to {e} then forced ingestion will be proceeding...")
            missing_dates.append(day_str)

    # 1.2.3. Ingest Facebook campaign insights for missing date(s)
    if missing_dates:
        for d in missing_dates:
            try:
                print(f"🔄 [BACKFILL] Triggering Facebook ad insights update for {d}...")
                logging.info(f"🔄 [BACKFILL] Triggering Facebook ad insights update for {d}...")
                update_ad_insights(d, d)
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to trigger Facebook ad insights update for {d} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to trigger Facebook ad insights update for {d} due to {e}.")
    else:
        print(f"✅ [BACKFILL] No missing dates detected for Facebook ad insights from {start_date} to {end_date} then backfill is skipped.")
        logging.info(f"✅ [BACKFILL] No missing dates detected for Facebook ad insights from {start_date} to {end_date} then backfill is skipped.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Facebook Campaign Backfill")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    backfill_campaign_insights(start_date=args.start_date, end_date=args.end_date)
