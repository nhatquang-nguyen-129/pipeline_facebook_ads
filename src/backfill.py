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

# Add logging capability for tracking process execution and errors
import logging

# Add Python Pandas library for data processing
import pandas as pd

# Add Python "re" library for expression matching
import re

# Add Python 'time' library for tracking execution time and implementing delays
import time

# Add Python 'datetime' library for datetime manipulation and timezone handling
from datetime import (
    datetime,
    timedelta,
    timezone
)

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud libraries for integration
from google.cloud import bigquery

# Add Google Secret Manager libraries for integration
from google.cloud import secretmanager

# Add Facebook Business libraries for integration
from facebook_business.api import FacebookAdsApi

# Add internal Facebook module for data handling
from src.ingest import (
    ingest_campaign_metadata,
    ingest_adset_metadata,
    ingest_ad_metadata,
    ingest_ad_creative,
    ingest_campaign_insights,
    ingest_ad_insights,
)
from src.staging import (
    staging_campaign_insights,
    staging_ad_insights
)
from src.mart import (
    mart_spend_all,
    mart_campaign_all,
    mart_creative_all,
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
    logging.info(f"🚀 [BACKFILL] Starting Facebook Ads campaign insights backfill from {start_date} to {end_date}...")

    # 1.1.1. Start timing the backfill process
    start_time = time.time()

    # 1.1.2. Initialize Facebook SDK session
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        DEFAULT_SECRET_ID = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        name = f"projects/{PROJECT}/secrets/{DEFAULT_SECRET_ID}/versions/latest"
        response = secret_client.access_secret_version(name=name)
        access_token = response.payload.data.decode("utf-8") if response.payload.data else None
        if not access_token:
            print(f"❌ [BACKFILL] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
            logging.error(f"❌ [BACKFILL] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
            raise RuntimeError(f"❌ [BACKFILL] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
        print(f"🔍 [BACKFILL] Initializing Facebook SDK session...")
        logging.info(f"🔍 [BACKFILL] Initializing Facebook SDK session...")
        FacebookAdsApi.init(access_token=access_token, timeout=180)
        print("✅ [BACKFILL] Successfully initialized Facebook SDK session.")
        logging.info("✅ [BACKFILL] Successfully initialized Facebook SDK session.")
    except Exception as e:
        print(f"❌ [BACKFILL] Failed to initialize Facebook SDK session due to {str(e)}.")
        logging.error(f"❌ [BACKFILL] Failed to initialize Facebook SDK session due to {str(e)}.")
        raise

    # 1.1.3. Initialize Google BigQuery session
    try:
        print(f"🔍 [BACKFILL] Initializing Google BigQuery client for project {PROJECT}...")
        logging.info(f"🔍 [BACKFILL] Initializing Google BigQuery client for project {PROJECT}...")
        client = bigquery.Client(project=PROJECT)
        print(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
        logging.info(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Exception as e:
        print(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {str(e)}.")
        logging.error(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {str(e)}.")

    # 1.1.4. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    print(f"🔍 [BACKFILL] Proceeding to backfill Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [BACKFILL] Proceeding to backfill Facebook campaign insights from {start_date} to {end_date}...")

    # 1.1.5. Iterate over input date range to verify data existence
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_campaign_ids = set()
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
        print(f"🔎 [BACKFILL] Evaluating {day_str} in Facebook campaign insights table {table_id}...")
        logging.info(f"🔎 [BACKFILL] Evaluating {day_str} in Facebook campaign insights table {table_id}...")
        should_ingest = False
        try:
            client.get_table(table_id)
        except NotFound:
            print(f"⚠️ [BACKFILL] Facebook campaign insights table {table_id} not found then ingestion will be starting...")
            logging.warning(f"⚠️ [BACKFILL] Facebook campaign insights table {table_id} not found then ingestion will be starting...")
            should_ingest = True
        else:
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
                    print(f"⚠️ [BACKFILL] Facebook campaign insights for {day_str} was not found then ingestion will be starting...")
                    logging.warning(f"⚠️ [BACKFILL] Facebook campaign insights for {day_str} was not found then ingestion will be starting...")
                    should_ingest = True
                else:
                    print(f"✅ [BACKFILL] Facebook campaign insights for {day_str} already exists in {table_id} then ingestion is skipped.")
                    logging.info(f"✅ [BACKFILL] Facebook campaign insights for {day_str} already exists in {table_id} then ingestion is skipped.")
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to verify Facebook campaign insights data for {day_str} due to {e} then ingestion will be starting...")
                logging.error(f"❌ [BACKFILL] Failed to verify Facebook campaign insights data for {day_str} due to {e} then ingestion will be starting...")
                should_ingest = True

    # 1.1.6. Ingest Facebook campaign insights
        if should_ingest:
            try:
                print(f"🔄 [BACKFILL] Triggering to ingest Facebook campaign insights for {day_str}...")
                logging.info(f"🔄 [BACKFILL] Triggering to ingest Facebook campaign insights for {day_str}...")
                df = ingest_campaign_insights(
                    start_date=day_str,
                    end_date=day_str,
                    write_disposition="WRITE_APPEND"
                )
                if "campaign_id" in df.columns:
                    updated_campaign_ids.update(df["campaign_id"].dropna().unique())
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")

    # 1.1.7. Ingest Facebook campaign metadata
    if updated_campaign_ids:
        print(f"🔄 [BACKFILL] Triggering to ingest Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
        logging.info(f"🔄 [BACKFILL] Triggering to ingest Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
        try:
            ingest_campaign_metadata(campaign_id_list=list(updated_campaign_ids))
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
    else:
        print("⚠️ [BACKFILL] No updated campaign_ids for Facebook campaign metadata then ingestion is skipped.")
        logging.warning("⚠️ [BACKFILL] No updated campaign_ids for Facebook campaign metadata then ingestion is skipped.")

    # 1.1.8. Rebuild staging Facebook campaign insights table
    if updated_campaign_ids:
        print("🔄 [BACKFILL] Triggering to rebuild staging Facebook campaign insights table...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild staging Facebook campaign insights table...")
        try:
            staging_campaign_insights()
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger staging table rebuild for Facebook campaign insights due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger staging table rebuild for Facebook campaign insights due to {e}.")

    # 1.1.9. Rebuild materialized Facebook campaign spend table
        print("🔄 [BACKFILL] Triggering to rebuild materialized Facebook campaign spend table...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild materialized Facebook campaign spend table...")
        try:
            mart_spend_all()
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger materialized table rebuild for Facebook campaign spend due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger materialized table rebuild for Facebook campaign spend due to {e}.")

    # 1.1.10. Rebuild materialized Facebook campaign performance table
        print("🔄 [BACKFILL] Triggering to rebuild materialized Facebook campaign performance table...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild materialized Facebook campaign performance table...")
        try:
            mart_campaign_all()
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger materialized table rebuild for Facebook campaign performance due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger materialized table rebuild for Facebook campaign performance due to {e}.")
    else:
        print("⚠️ [BACKFILL] No updates for Facebook campaign insights then staging/mart rebuild is skipped.")
        logging.warning("⚠️ [BACKFILL] No updates for Facebook campaign insights then staging/mart rebuild is skipped.")

    # 1.1.11. Measure the total execution time
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [BACKFILL] Successfully completed Facebook campaign insights backfill in {elapsed}s.")
    logging.info(f"✅ [BACKFILL] Successfully completed Facebook campaign insights backfill in {elapsed}s.")

# 1.2. Backfill historical Facebook ad insights for a given date range
def backfill_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [BACKFILL] Starting Facebook ad insights backfill from {start_date} to {end_date}...")
    logging.info(f"🚀 [BACKFILL] Starting Facebook Ads ad insights backfill from {start_date} to {end_date}...")

    # 2.1. Start timing the backfill process
    start_time = time.time()

    # 2.2. Initialize Facebook SDK session
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        DEFAULT_SECRET_ID = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        name = f"projects/{PROJECT}/secrets/{DEFAULT_SECRET_ID}/versions/latest"
        response = secret_client.access_secret_version(name=name)
        access_token = response.payload.data.decode("utf-8") if response.payload.data else None
        if not access_token:
            raise RuntimeError(f"❌ [BACKFILL] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
        print("🔍 [BACKFILL] Initializing Facebook SDK session...")
        logging.info("🔍 [BACKFILL] Initializing Facebook SDK session...")
        FacebookAdsApi.init(access_token=access_token, timeout=180)
        print("✅ [BACKFILL] Successfully initialized Facebook SDK session.")
        logging.info("✅ [BACKFILL] Successfully initialized Facebook SDK session.")
    except Exception as e:
        print(f"❌ [BACKFILL] Failed to initialize Facebook SDK session due to {e}.")
        logging.error(f"❌ [BACKFILL] Failed to initialize Facebook SDK session due to {e}.")
        raise

    # 2.3. Initialize Google BigQuery session
    try:
        print(f"🔍 [BACKFILL] Initializing Google BigQuery client for project {PROJECT}...")
        logging.info(f"🔍 [BACKFILL] Initializing Google BigQuery client for project {PROJECT}...")
        client = bigquery.Client(project=PROJECT)
        print(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
        logging.info(f"✅ [BACKFILL] Successfully initialized Google BigQuery client for {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [BACKFILL] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Exception as e:
        print(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {e}.")
        logging.error(f"❌ [BACKFILL] Failed to initialize Google BigQuery client due to {e}.")
        raise

    # 2.4. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    print(f"🔍 [BACKFILL] Proceeding to backfill Facebook ad insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [BACKFILL] Proceeding to backfill Facebook ad insights from {start_date} to {end_date}...")

    # 2.5. Backfill date range with evaluation
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_ad_ids = set()
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"

        print(f"🔎 [BACKFILL] Evaluating {day_str} in Facebook ad insights table {table_id}...")
        logging.info(f"🔎 [BACKFILL] Evaluating {day_str} in Facebook ad insights table {table_id}...")

        should_ingest = False
        try:
            client.get_table(table_id)
        except NotFound:
            print(f"⚠️ [BACKFILL] Facebook ad insights table {table_id} not found then ingestion will be starting...")
            logging.warning(f"⚠️ [BACKFILL] Facebook ad insights table {table_id} not found then ingestion will be starting...")
            should_ingest = True
        else:
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
                    print(f"⚠️ [BACKFILL] Facebook ad insights for {day_str} was not found then ingestion will be starting...")
                    logging.warning(f"⚠️ [BACKFILL] Facebook ad insights for {day_str} was not found then ingestion will be starting...")
                    should_ingest = True
                else:
                    print(f"✅ [BACKFILL] Facebook ad insights for {day_str} already exists in {table_id} then ingestion is skipped.")
                    logging.info(f"✅ [BACKFILL] Facebook ad insights for {day_str} already exists in {table_id} then ingestion is skipped.")
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to verify Facebook ad insights data for {day_str} due to {e} then ingestion will be starting...")
                logging.error(f"❌ [BACKFILL] Failed to verify Facebook ad insights data for {day_str} due to {e} then ingestion will be starting...")
                should_ingest = True

        # 2.6. Ingest Facebook ad insights
        if should_ingest:
            try:
                print(f"🔄 [BACKFILL] Triggering to ingest Facebook ad insights for {day_str}...")
                logging.info(f"🔄 [BACKFILL] Triggering to ingest Facebook ad insights for {day_str}...")
                df = ingest_ad_insights(
                    start_date=day_str,
                    end_date=day_str,
                    write_disposition="WRITE_APPEND"
                )
                if df is not None and "ad_id" in df.columns:
                    updated_ad_ids.update(df["ad_id"].dropna().unique())
                print(f"✅ [BACKFILL] Successfully ingested {len(df)} row(s) for {day_str}.")
                logging.info(f"✅ [BACKFILL] Successfully ingested {len(df)} row(s) for {day_str}.")
            except Exception as e:
                print(f"❌ [BACKFILL] Failed to ingest Facebook ad insights for {day_str} due to {e}.")
                logging.error(f"❌ [BACKFILL] Failed to ingest Facebook ad insights for {day_str} due to {e}.")

    # 2.7. Ingest Facebook ad metadata
    if updated_ad_ids:
        print(f"🔄 [BACKFILL] Triggering to ingest Facebook ad metadata for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [BACKFILL] Triggering to ingest Facebook ad metadata for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_metadata(ad_id_list=list(updated_ad_ids))
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger Facebook ad metadata ingestion due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger Facebook ad metadata ingestion due to {e}.")

    # 2.8. Ingest Facebook adset metadata
        try:
            print(f"🔄 [BACKFILL] Triggering to ingest Facebook adset metadata from {len(updated_ad_ids)} ad_id(s)...")
            logging.info(f"🔄 [BACKFILL] Triggering to ingest Facebook adset metadata from {len(updated_ad_ids)} ad_id(s)...")
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            tables = client.list_tables(dataset=raw_dataset)
            pattern = rf"^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m\d{{2}}\d{{4}}$"
            ad_tables = [
                f"{PROJECT}.{raw_dataset}.{table.table_id}"
                for table in tables if re.match(pattern, table.table_id)
            ]
            union_query = " UNION DISTINCT ".join([
                f"""
                SELECT DISTINCT CAST(adset_id AS STRING) AS adset_id
                FROM `{tbl}` WHERE ad_id IN UNNEST(@ad_ids) AND adset_id IS NOT NULL
                """ for tbl in ad_tables
            ])
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("ad_ids", "STRING", list(updated_ad_ids))]
            )
            adset_ids_df = client.query(union_query, job_config=job_config).to_dataframe()
            adset_id_list = adset_ids_df["adset_id"].dropna().unique().tolist()
            ingest_adset_metadata(adset_id_list=adset_id_list)
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger Facebook adset metadata ingestion due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger Facebook adset metadata ingestion due to {e}.")

    # 2.9. Ingest Facebook ad creative
        print(f"🔄 [BACKFILL] Triggering to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [BACKFILL] Triggering to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_creative(ad_id_list=list(updated_ad_ids))
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to trigger Facebook ad creative ingestion due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to trigger Facebook ad creative ingestion due to {e}.")
    else:
        print("⚠️ [BACKFILL] No updated ad_id(s) for Facebook ad metadata then ingestion is skipped.")
        logging.warning("⚠️ [BACKFILL] No updated ad_id(s) for Facebook ad metadata then ingestion is skipped.")

    # 2.10. Rebuild staging ad insights
    if updated_ad_ids:
        print("🔄 [BACKFILL] Triggering to rebuild staging table for Facebook ad insights...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild staging table for Facebook ad insights...")
        try:
            staging_ad_insights()
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to rebuild staging table for Facebook ad insights due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to rebuild staging table for Facebook ad insights due to {e}.")
    else:
        print("⚠️ [BACKFILL] No updates for Facebook ad insights then staging table rebuild is skipped.")
        logging.warning("⚠️ [BACKFILL] No updates for Facebook ad insights then staging table rebuild is skipped.")

    # 2.11. Rebuild mart creative performance
    if updated_ad_ids:
        print("🔄 [BACKFILL] Triggering to rebuild materialized table for Facebook creative performance...")
        logging.info("🔄 [BACKFILL] Triggering to rebuild materialized table for Facebook creative performance...")
        try:
            mart_creative_all()
        except Exception as e:
            print(f"❌ [BACKFILL] Failed to rebuild materialized creative performance table due to {e}.")
            logging.error(f"❌ [BACKFILL] Failed to rebuild materialized creative performance table due to {e}.")
    else:
        print("⚠️ [BACKFILL] No updates for Facebook ad insights then skip building creative materialized table.")
        logging.warning("⚠️ [BACKFILL] No updates for Facebook ad insights then skip building creative materialized table.")

    # 2.12. Measure execution time
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [BACKFILL] Successfully completed Facebook Ads ad insights backfill in {elapsed}s.")
    logging.info(f"✅ [BACKFILL] Successfully completed Facebook Ads ad insights backfill in {elapsed}s.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Facebook Campaign Backfill")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    backfill_ad_insights(start_date=args.start_date, end_date=args.end_date)
