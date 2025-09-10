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
    mart_campaign_all,
    mart_campaign_supplier,
    mart_campaign_festival,
    mart_creative_all,
    mart_creative_supplier,
    mart_creative_festival
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

    # 1.1.1. Start timing the update process
    start_time = time.time()

    # 1.1.2. Initialize Facebook SDK session
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        DEFAULT_SECRET_ID = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        name = f"projects/{PROJECT}/secrets/{DEFAULT_SECRET_ID}/versions/latest"
        response = secret_client.access_secret_version(name=name)
        access_token = response.payload.data.decode("utf-8") if response.payload.data else None
        if not access_token:
            print(f"❌ [UPDATE] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
            logging.error(f"❌ [UPDATE] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
            raise RuntimeError(f"❌ [UPDATE] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
        print(f"🔍 [UPDATE] Initializing Facebook SDK session...")
        logging.info(f"🔍 [UPDATE] Initializing Facebook SDK session...")
        FacebookAdsApi.init(access_token=access_token, timeout=180)
        print("✅ [UPDATE] Successfully initialized Facebook SDK session.")
        logging.info("✅ [UPDATE] Successfully initialized Facebook SDK session.")
    except Exception as e:
        print(f"❌ [UPDATE] Failed to initialize Facebook SDK session due to {str(e)}.")
        logging.error(f"❌ [UPDATE] Failed to initialize Facebook SDK session due to {str(e)}.")
        raise
    
    # 1.1.3. Initialize Google BigQuery session
    try:
        print(f"🔍 [UPDATE] Initializing Google BigQuery client for project {PROJECT}...")
        logging.info(f"🔍 [UPDATE] Initializing Google BigQuery client for project {PROJECT}...")
        client = bigquery.Client(project=PROJECT)
        print(f"✅ [UPDATE] Successfully initialized Google BigQuery client for {PROJECT}.")
        logging.info(f"✅ [UPDATE] Successfully initialized Google BigQuery client for {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError(f"❌ [UPDATE] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Exception as e:
        print(f"❌ [UPDATE] Failed to initialize Google BigQuery client due to {str(e)}.")
        logging.error(f"❌ [UPDATE] Failed to initialize Google BigQuery client due to {str(e)}.")

    # 1.1.4. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    print(f"🔍 [UPDATE] Proceeding to update Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook campaign insights from {start_date} to {end_date}...")

    # 1.1.5. Iterate over input date range to verify data freshness
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_campaign_ids = set()
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
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
                if "campaign_id" in df.columns:
                    updated_campaign_ids.update(df["campaign_id"].dropna().unique())
            except Exception as e:
                print(f"❌ [UPDATE] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to ingest Facebook campaign insights for {day_str} due to {e}.")

    # 1.1.7. Ingest Facebook campaign metadata
    if updated_campaign_ids:
        print(f"🔄 [UPDATE] Triggering to ingest Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
        try:
            ingest_campaign_metadata(campaign_id_list=list(updated_campaign_ids))
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger Facebook campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated campaign_ids for Facebook campaign metadata then ingestion is skipped.")
        logging.warning("⚠️ [UPDATE] No updated campaign_ids for Facebook campaign metadata then ingestion is skipped.")

    # 1.1.8 Rebuild staging Facebook campaign insights table
    if updated_campaign_ids:
        print("🔄 [UPDATE] Triggering to rebuild staging Facebook campaign insights table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging Facebook campaign insights table...")
        try:
            staging_campaign_insights()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger staging table rebuild for Facebook campaign insights due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger staging table rebuild for Facebook campaign insights due to {e}.")
    else:
        print("⚠️ [UPDATE] No updates for Facebook campaign insights then staging table rebuild is skipped.")
        logging.warning("⚠️ [UPDATE] No updates for Facebook campaign insights then staging table rebuild is skipped.")

    # 1.1.9. Rebuild materialized Facebook campaign performance table
    if updated_campaign_ids:
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook campaign performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook campaign performance table...")
        try:
            mart_campaign_all()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook campaign performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook campaign performance due to {e}.")

    # 1.1.10. Rebuild materialized Facebook supplier campaign performance table
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook supplier campaign performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook supplier campaign performance table...")
        try:
            mart_campaign_supplier()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook supplier campaign performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook supplier campaign performance due to {e}.")

    # 1.1.11. Rebuild materialized Facebook festival campaign performance table
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook festival campaign performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook festival campaign performance table...")
        try:
            mart_campaign_festival()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook festival campaign performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook festival campaign performance due to {e}.")
    else:
        print("⚠️ [UPDATE] No updates for Facebook campaign insights then skip building materialized table(s).")
        logging.warning("⚠️ [UPDATE] No updates for Facebook campaign insights then skip building materialized table(s).")

    # 1.1.11. Measure the total execution time
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [UPDATE] Successfully completed Facebook Ads campaign insights update in {elapsed}s.")
    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads campaign insights update in {elapsed}s.")

# 1.2. Update Facebook ad insights data for a given date range
def update_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting Facebook ad insights update from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting Facebook Ads ad insights update from {start_date} to {end_date}...")

    # 1.2.1. Start timing the update process
    start_time = time.time()

    # 1.2.2. Initialize Facebook SDK session
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        DEFAULT_SECRET_ID = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        name = f"projects/{PROJECT}/secrets/{DEFAULT_SECRET_ID}/versions/latest"
        response = secret_client.access_secret_version(name=name)
        access_token = response.payload.data.decode("utf-8") if response.payload.data else None
        if not access_token:
            print(f"❌ [UPDATE] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
            logging.error(f"❌ [UPDATE] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
            raise RuntimeError(f"❌ [UPDATE] Failed to retrieve Facebook access token from Secret Manager secret_id {DEFAULT_SECRET_ID}.")
        print(f"🔍 [UPDATE] Initializing Facebook SDK session...")
        logging.info(f"🔍 [UPDATE] Initializing Facebook SDK session...")
        FacebookAdsApi.init(access_token=access_token, timeout=180)
        print("✅ [UPDATE] Successfully initialized Facebook SDK session.")
        logging.info("✅ [UPDATE] Successfully initialized Facebook SDK session.")
    except Exception as e:
        print(f"❌ [UPDATE] Failed to initialize Facebook SDK session due to {str(e)}.")
        logging.error(f"❌ [UPDATE] Failed to initialize Facebook SDK session due to {str(e)}.")
        raise

    # 1.2.3. Initialize Google BigQuery session
    try:
        print(f"🔍 [UPDATE] Initializing Google BigQuery client for project {PROJECT}...")
        logging.info(f"🔍 [UPDATE] Initializing Google BigQuery client for project {PROJECT}...")
        client = bigquery.Client(project=PROJECT)
        print(f"✅ [UPDATE] Successfully initialized Google BigQuery client for {PROJECT}.")
        logging.info(f"✅ [UPDATE] Successfully initialized Google BigQuery client for {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError(f"❌ [UPDATE] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Exception as e:
        print(f"❌ [UPDATE] Failed to initialize Google BigQuery client due to {str(e)}.")
        logging.error(f"❌ [UPDATE] Failed to initialize Google BigQuery client due to {str(e)}.")

    # 1.2.4. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    print(f"🔍 [UPDATE] Proceeding to update Facebook ad insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook ad insights from {start_date} to {end_date}...")

    # 1.2.5. Iterate over input date range to verify data freshness
    date_range = pd.date_range(start=start_date, end=end_date)
    updated_ad_ids = set()
    for date in date_range:
        day_str = date.strftime("%Y-%m-%d")
        y, m = date.year, date.month
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
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

    # 1.2.6. Ingest Facebook ad insights
        if should_ingest:
            try:
                print(f"🔄 [UPDATE] Triggering to ingest Facebook ad insights for {day_str}...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook ad insights for {day_str}...")
                df = ingest_ad_insights(
                    start_date=day_str,
                    end_date=day_str,
                    write_disposition="WRITE_APPEND"
                )
                if "ad_id" in df.columns:
                    updated_ad_ids.update(df["ad_id"].dropna().unique())
            except Exception as e:
                print(f"❌ [UPDATE] Failed to trigger Facebook ad insights ingestion for {day_str} due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to trigger Facebook ad insights ingestion for {day_str} due to {e}.")

    # 1.2.7. Ingest Facebook ad metadata
    if updated_ad_ids:
        print(f"🔄 [UPDATE] Triggering to ingest Facebook ad metadata for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook ad metadata for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_metadata(ad_id_list=list(updated_ad_ids))
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger Facebook ad metadata ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger Facebook ad metadata ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")

    # 1.2.8. Ingest Facebook adset metadata
        try:
            print(f"🔄 [UPDATE] Triggering to ingest Facebook adset metadata for {len(updated_ad_ids)} ad_id(s)...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook adset metadata for {len(updated_ad_ids)} ad_id(s)...")
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
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger Facebook adset metadata ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger Facebook adset metadata ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")

    # 1.2.9. Ingest Facebook ad creative
        print(f"🔄 [UPDATE] Triggering to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_creative(ad_id_list=list(updated_ad_ids))
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger Facebook ad creative ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger Facebook ad creative ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated ad_id(s) for Facebook ad metadata then ingestion is skipped.")
        logging.warning("⚠️ [UPDATE] No updated ad_id(s) for Facebook ad metadata then ingestion is skipped.")

    # 1.2.10. Rebuild staging Facebook ad insights table
    if updated_ad_ids:
        print("🔄 [UPDATE] Triggering to rebuild staging Facebook ad insights table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging Facebook ad insights table...")
        try:
            staging_ad_insights()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger staging table rebuild for Facebook ad insights due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger staging table rebuild for Facebook ad insights due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then staging table rebuild is skipped.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook ad insights then staging table rebuild is skipped.")

    # 1.2.11. Rebuild materialized Facebook creative performance
    if updated_ad_ids:
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook creative performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook creative performance table...")
        try:
            mart_creative_all()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook creative performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook creative performance due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then skip building creative materialized table.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook ad insights then skip building creative materialized table.")

    # 1.2.12. Rebuild materialized Facebook supplier creative performance
    if updated_ad_ids:
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook supplier creative performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook supplier creative performance table...")
        try:
            mart_creative_supplier()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook supplier creative performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook supplier creative performance due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then skip building creative materialized table.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook ad insights then skip building creative materialized table.")

    # 1.2.12. Rebuild materialized Facebook festival creative performance
    if updated_ad_ids:
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook festival creative performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook festival creative performance table...")
        try:
            mart_creative_festival()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook festival creative performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook festival creative performance due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then skip building festival creative materialized table.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook ad insights then skip building festival creative materialized table.")

    # 1.2.13. Measure the total execution time
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [UPDATE] Successfully completed Facebook Ads ad insights update in {elapsed}s.")
    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads ad insights update in {elapsed}s.")