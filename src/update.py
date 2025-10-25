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

# Add logging ultilities for integration
import logging

# Add Python 'datetime' libraries for integration
from datetime import (
    datetime,
    timedelta,
    timezone
)

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" libraries for integration
import re

# Add Python 'time' libraries for integration
import time

# Add Google Authentication modules for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google API Core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add Facebook Business modules for integration
from facebook_business.api import FacebookAdsApi

# Add internal Facebook module for handling
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

# 1. UPDATE FACEBOOK ADS INSIGHTS A FOR A GIVEN DATE RANGE

# 1.1. Update Facebook Ads campaign insights for a given date range
def update_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting to update Facebook Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting to update Facebook Ads campaign insights from {start_date} to {end_date}...")

    # 1.1.1. Start timing Facebook Ads campaign insights
    update_time_start = time.time()
    update_sections_status = {}
    print(f"🔍 [UPDATE] Proceeding to update TikTok campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [UPDATE] Proceeding to update TikTok campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    try:

    # 1.1.2. Trigger to ingest Facebook Ads campaign insights
        print(f"🔄 [UPDATE] Triggering to ingest Facebook Ads campaign insights ingestion from {start_date} to {end_date}...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook Ads campaign insights ingestion from {start_date} to {end_date}...")
        update_results_ingested = ingest_campaign_insights(start_date=start_date, end_date=end_date)
        update_df_ingested = update_results_ingested["ingest_df_final"]
        update_status_ingested = update_results_ingested["ingest_status_final"]
        update_summary_ingested = update_results_ingested["ingest_summary_final"]
        updated_campaign_ids = set()
        updated_campaign_ids.update(update_df_ingested["campaign_id"].dropna().unique())
        if update_status_ingested == "ingest_succeed_all":
            print(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {update_summary_ingested['ingest_dates_output']} uploaded day(s) on {update_summary_ingested['ingest_dates_input']} total day(s) and {update_summary_ingested['ingest_rows_uploaded']} uploaded row(s) in {update_summary_ingested['ingest_time_elapsed']}s.")
            logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {update_summary_ingested['ingest_dates_output']}/{update_summary_ingested['ingest_dates_input']} day(s) and {update_summary_ingested['ingest_rows_uploaded']} uploaded row(s) in {update_summary_ingested['ingest_time_elapsed']}s.")
            update_sections_status["1.1.2. Trigger to ingest Facebook Ads campaign insights"] = "succeed"
        elif update_status_ingested == "fetch_success_partial":
            print(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {update_summary_ingested['ingest_dates_failed']} failed day(s) on {update_summary_ingested['ingest_dates_input']} total day(s) and {update_summary_ingested['ingest_rows_uploaded']} uploaded row(s) in {update_summary_ingested['ingest_time_elapsed']}s.")
            logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {update_summary_ingested['ingest_dates_failed']} failed day(s) on {update_summary_ingested['ingest_dates_input']} total day(s) and {update_summary_ingested['ingest_rows_uploaded']} uploaded row(s) in {update_summary_ingested['ingest_time_elapsed']}s.")
            ingest_sections_status["2.2.4. Trigger to fetch Facebook Ads ad insights "] = "partial"
        else:
            ingest_sections_status["2.2.4. Trigger to fetch Facebook Ads ad insights "] = "failed"
            print(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights fetching for {ingest_date_separated} due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights fetching for {ingest_date_separated} due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            raise RuntimeError(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights fetching for for {ingest_date_separated} due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")

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

    # 1.2.12. Rebuild materialized Facebook supplier creative performance
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook supplier creative performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook supplier creative performance table...")
        try:
            mart_creative_supplier()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook supplier creative performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook supplier creative performance due to {e}.")

    # 1.2.12. Rebuild materialized Facebook festival creative performance
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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Facebook Campaign Backfill")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    update_campaign_insights(start_date=args.start_date, end_date=args.end_date)
