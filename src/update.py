#services/facebook/update.py
"""
==================================================================
FACEBOOK UPDATE MODULE
------------------------------------------------------------------
This module performs **incremental updates** to Facebook Ads data  
at the raw layer, enabling day-by-day ingestion for specific layers  
such as campaigns, ads, or creatives without reprocessing entire months.

It is designed to support near-real-time refresh, daily sync jobs,  
or ad-hoc patching of recent Facebook Ads data.

✔️ Supports selective layer updates via parameterized control  
✔️ Reloads data by day to maintain data freshness and accuracy  
✔️ Reuses modular ingest functions for consistency across layers  

⚠️ This module is responsible for *RAW layer updates only*. It does  
not transform data or generate staging/MART tables directly.
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
from services.facebook.auth import init_sdk_session
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

# 1. UPDATE FACEBOOK ADS DATA FOR A GIVEN DATE RANGE

# 1.1. Update Facebook campaign insights data for a given date range
def update_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting Facebook campaign insights update from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting Facebook Ads campaign insights update from {start_date} to {end_date}...")

    # 1.1.1. Start timing the update process to measure total execution duration
    start_time = time.time()

    # 1.1.2. Validate environment variables
    if not all([COMPANY, PLATFORM, DEPARTMENT, ACCOUNT]):
        raise ValueError("❌ [UPDATE] Missing Facebook Ads BRAND/PLATFORM/ACCOUNT in environment variables.")
    try:
        config_account_confirmation = MAPPING_FACEBOOK_SECRET[COMPANY][PLATFORM]["account"][ACCOUNT]
    except KeyError:
        raise ValueError("❌ [UPDATE] Invalid BRAND/PLATFORM/ACCOUNT in MAPPING_FACEBOOK_SECRET.")

    # 1.1.3. Initialize Facebook SDK session onc
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        DEFAULT_SECRET_ID = f"{COMPANY}_secret_default_facebook_token_access_user"
        name = f"projects/{PROJECT}/secrets/{DEFAULT_SECRET_ID}/versions/latest"
        response = secret_client.access_secret_version(name=name)
        access_token = response.payload.data.decode("utf-8") if response.payload.data else None
        if not access_token:
            msg = f"❌ [AUTH] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}."
            print(msg)
            logging.error(msg)
            raise RuntimeError(msg)
        FacebookAdsApi.init(access_token=access_token, timeout=180)
        print(f"✅ [AUTH] Successfully initialized Facebook SDK session with timeout 180s.")
        logging.info(f"✅ [AUTH] Successfully initialized Facebook SDK session with timeout 180s.")
    except Exception as e:
        print(f"❌ [AUTH] Failed to initialize Facebook SDK session due to {str(e)}.")
        logging.error(f"❌ [AUTH] Failed to initialize Facebook SDK session due to {str(e)}.")
        raise
    
    # 1.1.4. Initialize Google BigQuery session
    try:
        client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e

    # 1.1.3. Prepare raw_table_id in BigQuery  
    raw_dataset = get_facebook_dataset("raw")
    print(f"🔍 [UPDATE] Proceeding to update Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook campaign insights from {start_date} to {end_date}...")

    # 1.1.4. Iterate over input date range to verify data freshness
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_months = set()
    updated_campaign_ids = set()
    updated_date = pd.DataFrame(columns=["date", "company", "platform", "department", "account", "row"])
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_campaign_m{m:02d}{y}"
        print(f"🔎 [UPDATE] Evaluating {day_str} in Facebook campaign insights table {table_id}...")
        logging.info(f"🔎 [UPDATE] Evaluating {day_str} in Facebook campaign insights table {table_id}...")     
        should_ingest = False
        try:
            client.get_table(table_id)
        except NotFound:
            print(f"⚠️ [UPDATE] Facebook campaign insights table {table_id} not found then ingestion will be starting...")
            logging.warning(f"⚠️ [UPDATE] Facebook campaign insights table {table_id} not found then ingestion will be starting...")
            should_ingest = True
        else:
            query = f"""
                SELECT MAX(last_updated_at) as last_updated
                FROM `{table_id}`
                WHERE date_start = @day_str
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("day_str", "STRING", day_str)]
            )
            try:
                result = client.query(query, job_config=job_config).result()
                last_updated = list(result)[0]["last_updated"]
                if not last_updated:
                    print(f"⚠️ [UPDATE] Facebook campaign insights for day {day_str} was not found then ingestion will be starting...")
                    logging.warning(f"⚠️ [UPDATE] Facebook campaign insights for day {day_str} was not found then ingestion will be starting...")
                    should_ingest = True
                else:
                    delta = datetime.now(timezone.utc) - last_updated
                    if delta > timedelta(hours=1):
                        print(f"⚠️ [UPDATE] Facebook campaign insights is outdated with last update was {last_updated} then ingestion will be starting...")
                        logging.warning(f"⚠️ [UPDATE] Facebook campaign insights is outdated with last update was {last_updated} then ingestion will be starting....")
                        should_ingest = True
                    else:
                        print(f"✅ [UPDATE] Facebook campaign insights for day {day_str} is fresh then ingestion is skipped.")
                        logging.info(f"✅ [UPDATE] Facebook campaign insights for day {day_str} is fresh then ingestion is skipped.")
            except Exception as e:
                print(f"❌ [UPDATE] Failed to verify Facebook campaign insights data freshness for {day_str} due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to verify Facebook campaign insights data freshness for {day_str} due to {e}.")
                should_ingest = True

        # 1.1.6. Ingest Facebook campaign insights from Facebook API to Google BigQuery raw tables
        if should_ingest:
            try:
                print(f"🔄 [UPDATE] Triggering Facebook campaign insights ingestion for {day_str}...")
                logging.info(f"🔄 [UPDATE] Triggering Facebook campaign insights ingestion for {day_str}...")
                df = ingest_campaign_insights(
                    start_date=day_str,
                    end_date=day_str,
                    write_disposition="WRITE_APPEND"
                )
                updated_months.add((y, m))
                if "campaign_id" in df.columns:
                    updated_campaign_ids.update(df["campaign_id"].dropna().unique())
                print(f"✅ [UPDATE] Successfully ingested Facebook campaign insights with {len(df)} row(s) for day {day_str}.")
                logging.info(f"✅ [UPDATE] Successfully ingested Facebook campaign insights with {len(df)} row(s) for day {day_str}.")
                ingested_record = pd.DataFrame([{
                    "date": day_str,
                    "company": COMPANY,
                    "platform": PLATFORM,
                    "department": DEPARTMENT,
                    "account": ACCOUNT,
                    "row": len(df)
                }])
                updated_date = pd.concat([updated_date, ingested_record], ignore_index=True)
            except Exception as e:
                print(f"❌ [UPDATE] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")
    # 1.1.7. Ingest Facebook campaign metadata from Facebook API to Google BigQuery raw tables
    if updated_campaign_ids:
        print(f"🔄 [UPDATE] Triggering Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s)...")
        try:
            ingest_campaign_metadata(campaign_id_list=list(updated_campaign_ids))
            print(f"✅ [UPDATE] Successfully ingested Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s).")
            logging.info(f"✅ [UPDATE] Successfully ingested Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s).")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to ingest Facebook campaign metadata ingest for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to ingest Facebook campaign metadata ingest for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated campaign_ids for Facebook campaign metadata then ingestion is skipped.")
        logging.warning("⚠️ [UPDATE] No updated campaign_ids for Facebook campaign metadata then ingestion is skipped.")

    # 1.1.8 Rebuild Facebook campaign insights staging table
    if updated_months:
        print("🔄 [UPDATE] Triggering to rebuild staging table for Facebook campaign insights...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging table for Facebook campaign insights...")
        try:
            staging_campaign_insights(updated_date=updated_date)
            print("✅ [UPDATE] Successfully rebuilt staging table for Facebook campaign insights.")
            logging.info("✅ [UPDATE] Successfully rebuilt staging table for Facebook campaign insights.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild staging table for Facebook campaign insights due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild staging table for Facebook campaign insights due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook campaign insights then staging table rebuild is skipped.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook campaign insights then staging table rebuild is skipped.")

    # 1.1.9. Rebuild Facebook materialized tables for campaign spending
    if updated_months:
        print("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook campaign spending...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook campaign spending...")
        try:
            mart_spend_all()
            print("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign spending.")
            logging.info("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign spending.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook campaign spending due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook campaign spending due to {e}.")

    # 1.1.10. Rebuild Facebook materialized tables for campaign performance
        print("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook campaign performance...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook campaign performance...")
        try:
            mart_campaign_all()
            print("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign performance.")
            logging.info("✅ [UPDATE] Successfully rebuilt materialized table for Facebook campaign performance.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook campaign performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook campaign performance due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook campaign insights then skip building materialized tables.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook campaign insights then skip building materialized tables.")

    # 1.1.11. Measure the total execution time of Facebook campaign insights update process
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [UPDATE] Successfully completed Facebook Ads campaign insights update in {elapsed}s.")
    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads campaign insights update in {elapsed}s.")

# 1.2. Update Facebook ad insights data for a given date range
def update_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting Facebook ad insights update from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting Facebook ad insights update from {start_date} to {end_date}...")

    # 1.2.1. Start timing the update process to measure total execution duration
    start_time = time.time()

    # 1.2.3. Validate environment variables
    if not all([COMPANY, PLATFORM, ACCOUNT]):
        raise ValueError("❌ [UPDATE] Missing Facebook Ads COMPANY/PLATFORM/ACCOUNT in environment variables.")

    try:
        config_account_confirmation = MAPPING_FACEBOOK_SECRET[COMPANY][PLATFORM]["account"][ACCOUNT]
    except KeyError:
        raise ValueError("❌ [UPDATE] Invalid Facebook Ads COMPANY/PLATFORM/ACCOUNT in MAPPING_FACEBOOK_SECRET")

    # 1.2.3. Prepare raw_table_id in BigQuery  
    raw_dataset = get_facebook_dataset("raw")
    print(f"🔍 [UPDATE] Proceeding to update Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook campaign insights from {start_date} to {end_date}...")

    # 1.2.4. Iterate over input date range to verify data freshness
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_months = set()
    updated_ad_ids = set()
    init_sdk_session()
    try:
        client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_ad_m{m:02d}{y}"
        print(f"🔎 [UPDATE] Evaluating {day_str} in Facebook ad insights table {table_id}...")
        logging.info(f"🔎 [UPDATE] Evaluating {day_str} in Facebook ad insights table {table_id}...")      
        should_ingest = False
        try:
            client.get_table(table_id)
        except NotFound:
            print(f"⚠️ [UPDATE] Facebook ad insights table {table_id} not found then ingestion will be starting...")
            logging.warning(f"⚠️ [UPDATE] Facebook ad insights table {table_id} not found then ingestion will be starting...")
            should_ingest = True
        else:
            query = f"""
                SELECT MAX(last_updated_at) as last_updated
                FROM `{table_id}`
                WHERE date_start = @day_str
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("day_str", "STRING", day_str)]
            )
            try:
                result = client.query(query, job_config=job_config).result()
                last_updated = list(result)[0]["last_updated"]
                if not last_updated:
                    print(f"⚠️ [UPDATE] Facebook ad insights for day {day_str} was not found then ingestion will be starting...")
                    logging.warning(f"⚠️ [UPDATE] Facebook ad insights for day {day_str} was not found then ingestion will be starting...")
                    should_ingest = True
                else:
                    delta = datetime.now(timezone.utc) - last_updated
                    if delta > timedelta(hours=1):
                        print(f"⚠️ [UPDATE] Facebook ad insights is outdated with last update was {last_updated} then ingestion will be starting...")
                        logging.warning(f"⚠️ [UPDATE] Facebook ad insights is outdated with last update was {last_updated} then ingestion will be starting...")
                        should_ingest = True
                    else:
                        print(f"✅ [UPDATE] Facebook ad insights for day {day_str} is fresh then ingestion is skipped.")
                        logging.info(f"✅ [UPDATE] Facebook ad insights for day {day_str} is fresh then ingestion is skipped.")
            except Exception as e:
                print(f"❌ [UPDATE] Failed to verify Facebook ad insights data freshness for {day_str} due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to verify Facebook ad insights data freshness for {day_str} due to {e}.")
                should_ingest = True

        # 1.2.5. Ingest Facebook ad insights from Facebook API to Google BigQuery raw tables
        if should_ingest:
            try:
                print(f"🔄 [UPDATE] Triggering Facebook ad insights ingestion for {day_str}...")
                logging.info(f"🔄 [UPDATE] Triggering Facebook ad insights ingestion for {day_str}...")
                df = ingest_ad_insights(
                    start_date=day_str,
                    end_date=day_str,
                    write_disposition="WRITE_APPEND"
                )
                updated_months.add((y, m))
                if "ad_id" in df.columns:
                    updated_ad_ids.update(df["ad_id"].dropna().unique())
                print(f"✅ [UPDATE] Successfully ingested Facebook ad insights with {len(df)} row(s) for day {day_str}.")
                logging.info(f"✅ [UPDATE] Successfully ingested Facebook ad insights with {len(df)} row(s) for day {day_str}.")
            except Exception as e:
                print(f"❌ [UPDATE] Failed to ingest Facebook ad insights for {day_str} due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to ingest Facebook ad insights for {day_str} due to {e}.")

    # 1.2.6. Ingest Facebook ad metadata from Facebook API to Google BigQuery raw tables
    if updated_ad_ids:
        print(f"🔄 [UPDATE] Triggering Facebook ad metadata ingestion for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering Facebook ad metadata ingestion for {len(updated_ad_ids)} ad_id(s)...")

        try:
            ingest_ad_metadata(ad_id_list=list(updated_ad_ids))
            print(f"✅ [UPDATE] Successfully ingested Facebook ad metadata for {len(updated_ad_ids)} ad_id(s).")
            logging.info(f"✅ [UPDATE] Successfully ingested Facebook ad metadata for {len(updated_ad_ids)} ad_id(s).")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to ingest Facebook ad metadata ingest for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to ingest Facebook ad metadata ingest for {len(updated_ad_ids)} ad_id(s) due to {e}.")

    # 1.2.7. Ingest Facebook adset metadata from Facebook API to Google BigQuery raw tables
        try:
            print(f"🔄 [UPDATE] Triggering Facebook adset metadata ingestion for {len(updated_ad_ids)} ad_id(s)...")
            logging.info(f"🔄 [UPDATE] Triggering Facebook adset metadata ingestion for {len(updated_ad_ids)} ad_id(s)...")
            raw_dataset = get_facebook_dataset("raw")
            tables = client.list_tables(dataset=raw_dataset)
            pattern = rf"^{COMPANY}_table_{PLATFORM}_ad_m\d{{2}}\d{{4}}$"
            ad_tables = [
                f"{PROJECT}.{raw_dataset}.{table.table_id}"
                for table in tables if re.match(pattern, table.table_id)
            ]
            union_query = " UNION DISTINCT ".join([
                f"""
                SELECT DISTINCT CAST(adset_id AS STRING) AS adset_id
                FROM `{tbl}` WHERE ad_id IN UNNEST(@ad_ids) AND adset_id IS NOT NULL
                """
                for tbl in ad_tables
            ])
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("ad_ids", "STRING", list(updated_ad_ids))
                ]
            )
            adset_ids_df = client.query(union_query, job_config=job_config).to_dataframe()
            adset_id_list = adset_ids_df["adset_id"].dropna().unique().tolist()
            ingest_adset_metadata(adset_id_list=adset_id_list)
            print(f"✅ [UPDATE] Successfully ingested Facebook adset metadata for {len(updated_ad_ids)} ad_id(s).")
            logging.info(f"✅ [UPDATE] Successfully ingested Facebook adset metadata for {len(updated_ad_ids)} ad_id(s).")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to ingest Facebook adset metadata ingest for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to ingest Facebook adset metadata ingest for {len(updated_ad_ids)} ad_id(s) due to {e}.")

    # 1.2.8. Ingest Facebook ad creative from Facebook API to Google BigQuery raw tables
        print(f"🔄 [UPDATE] Triggering Facebook ad creative ingestion for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering Facebook ad creative ingestion for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_creative(ad_id_list=list(updated_ad_ids))
            print(f"✅ [UPDATE] Successfully ingested Facebook ad creative for {len(updated_ad_ids)} ad_id(s).")
            logging.info(f"✅ [UPDATE] Successfully ingested Facebook ad creative for {len(updated_ad_ids)} ad_id(s).")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s) due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated ad_id(s) for Facebook campaign metadata then ingestion is skipped.")
        logging.info("⚠️ [UPDATE] No updated ad_id(s) for Facebook campaign metadata then ingestion is skipped.")

    # 1.2.9. Rebuild Facebook ad insights staging table
    if updated_months:
        print("🔄 [UPDATE] Triggering to rebuild staging table for Facebook ad insights...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging table for Facebook ad insights...")
        try:
            staging_ad_insights()
            print("✅ [UPDATE] Successfully rebuilt staging table for Facebook ad insights.")
            logging.info("✅ [UPDATE] Successfully rebuilt staging table for Facebook ad insights.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild staging table for Facebook ad insights due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild staging table for Facebook ad insights due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then staging table rebuild is skipped.")
        logging.info("⚠️ [UPDATE] No updated for Facebook ad insights then staging table rebuild is skipped.")

    # 1.2.10. Rebuild Facebook materialized table for creative performance
        print("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook creative performance...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized table for Facebook creative performance...")
    try:
        mart_creative_all()
        print("✅ [UPDATE] Successfully rebuilt materialized table for Facebook creative performance.")
        logging.info("✅ [UPDATE] Successfully rebuilt materialized table for Facebook creative performance.")
    except Exception as e:
        print(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook creative performance due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for Facebook creative performance due to {e}.")

    # 1.2.11. Measure the total execution time of Facebook ad insights update process
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [UPDATE] Successfully completed Facebook Ads ad insights update in {elapsed}s.")
    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads ad insights update in {elapsed}s.")

import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update Facebook campaign insights for a given date range.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    update_campaign_insights(start_date=args.start_date, end_date=args.end_date)
