"""
==================================================================
FACEBOOK UPDATE MODULE
------------------------------------------------------------------
This module performs incremental updates to Facebook Ads data at  
the raw layer, providing an efficient mechanism for refreshing  
recent or specific-date datasets without the need for full reloads.

By supporting targeted updates (per day, layer, or entity), it  
enables faster turnaround for near-real-time dashboards and daily  
data sync jobs while maintaining historical accuracy and integrity.

✔️ Handles incremental data ingestion from the Facebook Marketing API  
✔️ Supports selective updates for campaigns, adsets, ads or creatives  
✔️ Preserves schema alignment with staging and MART layers  
✔️ Implements error handling and retry logic for partial failures  
✔️ Designed for integration in daily or on-demand sync pipelines  

⚠️ This module is strictly responsible for *RAW layer updates only*.  
It does not perform transformations, enrichment, or aggregations.  
Processed data is consumed by the STAGING and MART modules.
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

# Add Python Tabulate for integration
from tabulate import tabulate

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
    mart_creative_all,
    mart_creative_supplier,
)

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Platform Project ID
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

# 1. UPDATE FACEBOOK ADS INSIGHTS FOR A GIVEN DATE RANGE

# 1.1. Update Facebook Ads campaign insights
def update_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting to update Facebook Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting to update Facebook Ads campaign insights from {start_date} to {end_date}...")

    # 1.1.1. Start timing Facebook Ads campaign insights update
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update Facebook Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    try:

    # 1.1.2. Trigger to ingest Facebook Ads campaign insights
        update_section_name = "[UPDATE] Trigger to ingest Facebook Ads campaign insights"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest Facebook Ads campaign insights ingestion from {start_date} to {end_date}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook Ads campaign insights ingestion from {start_date} to {end_date}...")
            ingest_results_insights = ingest_campaign_insights(start_date=start_date, end_date=end_date)
            ingest_df_insights = ingest_results_insights["ingest_df_final"]
            ingest_status_insights = ingest_results_insights["ingest_status_final"]
            ingest_summary_insights = ingest_results_insights["ingest_summary_final"]
            updated_ids_campaign = set(ingest_df_insights["campaign_id"].dropna().unique())
            if ingest_status_insights == "ingest_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            elif ingest_status_insights == "ingest_succeed_partial":
                print(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "partial"
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign insights ingestion from {start_date} to {end_date} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign insights ingestion from {start_date} to {end_date} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.3. Trigger to ingest Facebook Ads campaign metadata
        update_section_name = "[UPDATE] Trigger to ingest Facebook Ads campaign metadata"
        update_section_start = time.time()
        try:
            if updated_ids_campaign:
                print(f"🔄 [UPDATE] Triggering to ingest Facebook Ads campaign metadata for {len(updated_ids_campaign)} campaign_id(s)...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook Ads campaign metadata for {len(updated_ids_campaign)} campaign_id(s)...")
                ingest_results_metadata = ingest_campaign_metadata(ingest_ids_campaign=list(updated_ids_campaign))
                ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                if ingest_status_metadata == "ingest_succeed_all":
                    print(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif ingest_status_metadata == "ingest_success_partial":
                    print(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "partial"
                else:
                    print(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No updates for any campaign_id then Facebook Ads campaign metadata ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any campaign_id then Facebook Ads campaign metadata ingestion is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.4. Trigger to build staging Facebook Ads campaign insights
        update_section_name = "[UPDATE] Trigger to build staging Facebook Ads campaign insights"
        update_section_start = time.time()
        try:
            if updated_ids_campaign:
                print("🔄 [UPDATE] Triggering to create or overwrite staging Facebook Ads campaign insights table...")
                logging.info("🔄 [UPDATE] Triggering to create or overwrite staging Facebook Ads campaign insights table...")
                staging_results_campaign = staging_campaign_insights()
                staging_status_campaign = staging_results_campaign["staging_status_final"]
                staging_summary_campaign = staging_results_campaign["staging_summary_final"]
                if staging_status_campaign == "staging_succeed_all":
                    print(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif staging_status_campaign == "staging_failed_partial":
                    print(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "partial"
                else:
                    print(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No updates for any campaign_id then Facebook Ads campaign insights staging is marked as failed.")
                logging.error("⚠️ [UPDATE] No updates for any campaign_id then Facebook Ads campaign insights staging is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.5. Trigger to materialize Facebook Ads campaign performance table
        update_section_name = "[UPDATE] Trigger to materialize Facebook Ads campaign performance table"
        update_section_start = time.time()
        try:
            if staging_status_campaign in ["staging_succeed_all", "staging_failed_partial"]:
                print("🔄 [UPDATE] Triggering to build materialized Facebook Ads campaign performance table...")
                logging.info("🔄 [UPDATE] Triggering to build materialized Facebook Ads campaign performance table...")               
                mart_results_all = mart_campaign_all()
                mart_status_all = mart_results_all["mart_status_final"]
                mart_summary_all = mart_results_all["mart_summary_final"]                
                if mart_status_all == "mart_succeed_all":
                    print(f"✅ [UPDATE] Successfully completed Facebook Ads campaign performance materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads campaign performance materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif mart_status_all == "mart_failed_all":
                    print(f"❌ [UPDATE] Failed to complete Facebook Ads campaign performance materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    logging.error(f"❌ [UPDATE] Failed to complete Facebook Ads campaign performance materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No data returned from Facebook Ads campaign insights staging then materialization is skipped.")
                logging.warning("⚠️ [UPDATE] No data returned from Facebook Ads campaign insights staging then materialization is skipped.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)
    
    # 1.1.6. Summarize update result(s) for Facebook Ads campaign performance
    finally:
        update_time_total = round(time.time() - update_time_start, 2)
        print("\n📊 [UPDATE] FACEBOOK ADS CAMPAIGN PERFORMANCE UPDATE SUMMARY")
        print("=" * 110)
        print(f"{'Step':<80} | {'Status':<10} | {'Time (s)'}")
        print("-" * 110)
        summary_map = {
            "[UPDATE] Trigger to ingest Facebook Ads campaign insights": "ingest_results_insights",
            "[UPDATE] Trigger to ingest Facebook Ads campaign metadata": "ingest_results_metadata",
            "[UPDATE] Trigger to build staging Facebook Ads campaign insights": "staging_results_campaign",
            "[UPDATE] Trigger to materialize Facebook Ads campaign performance table": "mart_results_all",
        }
        locals_dict = locals()
        for update_step_name, update_step_status in update_sections_status.items():
            summary_obj = None
            if update_step_name in summary_map and summary_map[update_step_name] in locals_dict:
                summary_obj = locals_dict[summary_map[update_step_name]]
            nested_summary = None
            if summary_obj and isinstance(summary_obj, dict):
                for k in summary_obj.keys():
                    if k.endswith("_summary_final"):
                        nested_summary = summary_obj[k]
                        break
            candidate_dict = (nested_summary or summary_obj or {})
            step_time = None
            for key in ["ingest_time_elapsed", "staging_time_elapsed", "mart_time_elapsed"]:
                if key in candidate_dict and candidate_dict[key] is not None:
                    step_time = candidate_dict[key]
                    break
            if step_time is None:
                step_time = "-"
            time_str = "-" if step_time == "-" else f"{step_time:>8.2f}"
            print(f"• {update_step_name:<76} | {update_step_status:<10} | {time_str}")
            if nested_summary:
                for detail_key in [k for k in nested_summary.keys() if k.endswith("_sections_detail")]:
                    detail_dict = nested_summary[detail_key]
                    for idx, (sub_step, sub_info) in enumerate(detail_dict.items(), start=1):
                        sub_status = sub_info.get("status", "-")
                        sub_time_section = sub_info.get("time", 0.0)
                        sub_loop_time = sub_info.get("loop_time", 0.0)
                        sub_total = round(sub_time_section + sub_loop_time, 2)
                        print(f"    {idx:>2}. {sub_step:<70} | {sub_status:<10} | {sub_total:>8.2f}")
        print("-" * 110)
        print(f"{'Total execution time':<80} | {'-':<10} | {update_time_total:>8.2f}s")
        print("=" * 110)

# 1.2. Update Facebook Ads ad insights
def update_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting to update Facebook Ads ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting to update Facebook Ads ad insights from {start_date} to {end_date}...")

    # 1.2.1. Start timing Facebook Ads ad insights update
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update Facebook Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    try:

    # 1.2.2. Initialize Google BigQuery client
        update_section_name = "[UPDATE] Initialize Google BigQuery client"
        update_section_start = time.time()    
        try:
            print(f"🔍 [UPDATE] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [UPDATE] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [UPDATE] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [UPDATE] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            update_sections_status[update_section_name] = "succeed"
        except Exception as e:
            update_sections_status[update_section_name] = "failed"
            print(f"❌ [UPDATE] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.3. Trigger to ingest Facebook Ads ad insights
        update_section_name = "[UPDATE] Trigger to ingest Facebook Ads ad insights"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest Facebook Ads ad insights ingestion from {start_date} to {end_date}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook Ads ad insights ingestion from {start_date} to {end_date}...")
            ingest_results_insights = ingest_ad_insights(start_date=start_date, end_date=end_date)
            ingest_df_insights = ingest_results_insights["ingest_df_final"]
            ingest_status_insights = ingest_results_insights["ingest_status_final"]
            ingest_summary_insights = ingest_results_insights["ingest_summary_final"]
            updated_ids_ad = set(ingest_df_insights["ad_id"].dropna().unique())
            if ingest_status_insights == "ingest_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Facebook Ads ad insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads ad insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            elif ingest_status_insights == "ingest_succeed_partial":
                print(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "partial"
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger Facebook Ads ad insights ingestion from {start_date} to {end_date} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads ad insights ingestion from {start_date} to {end_date} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_uploaded']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.4. Trigger to ingest Facebook Ads ad metadata
        update_section_name = "[UPDATE] Trigger to ingest Facebook Ads ad metadata"
        update_section_start = time.time()
        try:
            if updated_ids_ad:
                print(f"🔄 [UPDATE] Triggering to ingest Facebook Ads ad metadata for {len(updated_ids_ad)} ad_id(s)...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook Ads ad metadata for {len(updated_ids_ad)} ad_id(s)...")
                ingest_results_metadata = ingest_ad_metadata(ingest_ids_ad=list(updated_ids_ad))
                ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                if ingest_status_metadata == "ingest_succeed_all":
                    print(f"✅ [UPDATE] Successfully triggered Facebook Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif ingest_status_metadata == "ingest_success_partial":
                    print(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "partial"
                else:
                    print(f"❌ [UPDATE] Failed to trigger Facebook Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads ad metadata ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads ad metadata ingestion is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.5. Trigger to ingest Facebook Ads adset metadata
        update_section_name = "[UPDATE] Trigger to ingest Facebook Ads adset metadata"
        update_section_start = time.time()
        try:
            if updated_ids_ad:
                print(f"🔄 [UPDATE] Triggering to ingest Facebook adset metadata for {len(updated_ids_ad)} ad_id(s)...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook adset metadata for {len(updated_ids_ad)} ad_id(s)...")
                raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
                raw_tables_all = google_bigquery_client.list_tables(dataset=raw_dataset)
                raw_table_pattern = rf"^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m\d{{2}}\d{{4}}$"
                raw_tables_ad = [
                    f"{PROJECT}.{raw_dataset}.{raw_table_all.table_id}"
                    for raw_table_all in raw_tables_all if re.match(raw_table_pattern, raw_table_all.table_id)
                ]
                if not raw_tables_ad:
                    raise ValueError("❌ [UPDATE] No matching ad raw tables found for Facebook Ads adset metadata ingestion.")
                union_query = " UNION DISTINCT ".join([
                    f"""
                    SELECT DISTINCT CAST(adset_id AS STRING) AS adset_id
                    FROM `{tbl}`
                    WHERE ad_id IN UNNEST(@ad_ids)
                    AND adset_id IS NOT NULL
                    """
                    for tbl in raw_tables_ad
                ])
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("ad_ids", "STRING", list(updated_ids_ad))]
                )
                adset_ids_df = google_bigquery_client.query(union_query, job_config=job_config).to_dataframe()
                adset_id_list = adset_ids_df["adset_id"].dropna().unique().tolist()
                if adset_id_list:
                    ingest_results_metadata = ingest_adset_metadata(adset_id_list=adset_id_list)
                    ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                    ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                    if ingest_status_metadata == "ingest_succeed_all":
                        print(f"✅ [UPDATE] Successfully triggered Facebook Ads adset metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                        logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads adset metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                        update_sections_status[update_section_name] = "succeed"
                    elif ingest_status_metadata == "ingest_success_partial":
                        print(f"⚠️ [UPDATE] Partially triggered Facebook Ads adset metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                        logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads adset metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                        update_sections_status[update_section_name] = "partial"
                    else:
                        print(f"❌ [UPDATE] Failed to trigger Facebook Ads adset metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                        logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads adset metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                        update_sections_status[update_section_name] = "failed"
                else:
                    print("⚠️ [UPDATE] No adset_id found for given ad_id(s), skipping adset metadata ingestion.")
                    logging.warning("⚠️ [UPDATE] No adset_id found for given ad_id(s), skipping adset metadata ingestion.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads adset metadata ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads adset metadata ingestion is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger Facebook adset metadata ingestion due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger Facebook adset metadata ingestion due to {e}.")
            update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.6. Trigger to ingest Facebook Ads ad creative
        update_section_name = "[UPDATE] Trigger to ingest Facebook Ads ad creative"
        update_section_start = time.time()
        try:
            if updated_ids_ad:
                print(f"🔄 [UPDATE] Triggering to ingest Facebook ad creative for {len(updated_ids_ad)} ad_id(s)...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook ad creative for {len(updated_ids_ad)} ad_id(s)...")
                ingest_results_metadata = ingest_ad_creative(ingest_ids_ad=list(updated_ids_ad))
                ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                if ingest_status_metadata == "ingest_succeed_all":
                    print(f"✅ [UPDATE] Successfully triggered Facebook Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif ingest_status_metadata == "ingest_success_partial":
                    print(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "partial"
                else:
                    print(f"❌ [UPDATE] Failed to trigger Facebook Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads ad creative ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads ad creative ingestion is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.7. Trigger to build staging Facebook Ads ad insights
        update_section_name = "[UPDATE] Trigger to build staging Facebook Ads ad insights"
        update_section_start = time.time()
        try:
            if updated_ids_ad:
                print("🔄 [UPDATE] Triggering to create or overwrite staging Facebook Ads ad insights table...")
                logging.info("🔄 [UPDATE] Triggering to create or overwrite staging Facebook Ads ad insights table...")
                staging_results_ad = staging_ad_insights()
                staging_status_ad = staging_results_ad["staging_status_final"]
                staging_summary_ad = staging_results_ad["staging_summary_final"]
                if staging_status_ad == "staging_succeed_all":
                    print(f"✅ [UPDATE] Successfully triggered Facebook Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif staging_status_ad == "staging_failed_partial":
                    print(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "partial"
                else:
                    print(f"❌ [UPDATE] Failed to trigger Facebook Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads ad insights staging is marked as failed.")
                logging.error("⚠️ [UPDATE] No updates for any ad_id then Facebook Ads ad insights staging is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.8. Trigger to materialize Facebook Ads creative performance table
        update_section_name = "[UPDATE] Trigger to materialize Facebook Ads creative performance table"
        update_section_start = time.time()
        try:
            if staging_status_ad in ["staging_succeed_all", "staging_failed_partial"]:
                print("🔄 [UPDATE] Triggering to build materialized Facebook Ads creative performance table...")
                logging.info("🔄 [UPDATE] Triggering to build materialized Facebook Ads creative performance table...")
                mart_results_all = mart_creative_all()
                mart_status_all = mart_results_all["mart_status_final"]
                mart_summary_all = mart_results_all["mart_summary_final"]                
                if mart_status_all == "mart_succeed_all":
                    print(f"✅ [UPDATE] Successfully completed Facebook Ads creative performance materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads creative performance materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif mart_status_all == "mart_failed_all":
                    print(f"❌ [UPDATE] Failed to complete Facebook Ads creative performance materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    logging.error(f"❌ [UPDATE] Failed to complete Facebook Ads creative performance materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No data returned from Facebook Ads ad insights staging then materialization is skipped.")
                logging.warning("⚠️ [UPDATE] No data returned from Facebook Ads ad insights staging then materialization is skipped.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.9. Summarize update result(s) for Facebook Ads ad insights pipeline
    finally:
        update_time_total = round(time.time() - update_time_start, 2)
        print("\n📊 [UPDATE] FACEBOOK ADS CREATIVE PERFORMANCE SUMMARY")
        print("=" * 120)
        print(f"{'Step':<80} | {'Status':<10} | {'Time (s)'}")
        print("-" * 120)
        summary_map = {
            "[UPDATE] Trigger to ingest Facebook Ads ad insights": "ingest_results_insights",
            "[UPDATE] Trigger to ingest Facebook Ads ad metadata": "ingest_results_metadata",
            "[UPDATE] Trigger to ingest Facebook Ads adset metadata": "ingest_results_metadata",
            "[UPDATE] Trigger to ingest Facebook Ads ad creative": "ingest_results_metadata",
            "[UPDATE] Trigger to build staging Facebook Ads ad insights": "staging_results_ad",
            "[UPDATE] Trigger to materialize Facebook Ads creative performance table": "mart_results_all",
        }
        locals_dict = locals()
        for update_step_name, update_step_status in update_sections_status.items():
            summary_obj = None
            if update_step_name in summary_map and summary_map[update_step_name] in locals_dict:
                summary_obj = locals_dict[summary_map[update_step_name]]
            nested_summary = None
            if summary_obj and isinstance(summary_obj, dict):
                for k in summary_obj.keys():
                    if k.endswith("_summary_final"):
                        nested_summary = summary_obj[k]
                        break
            candidate_dict = (nested_summary or summary_obj or {})
            step_time = None
            for key in ["ingest_time_elapsed", "staging_time_elapsed", "mart_time_elapsed"]:
                if key in candidate_dict and candidate_dict[key] is not None:
                    step_time = candidate_dict[key]
                    break
            time_str = "-" if step_time is None else f"{step_time:>8.2f}"
            print(f"• {update_step_name:<76} | {update_step_status:<10} | {time_str}")
            if nested_summary:
                for detail_key in [k for k in nested_summary.keys() if k.endswith("_sections_detail")]:
                    detail_dict = nested_summary[detail_key]
                    for idx, (sub_step, sub_info) in enumerate(detail_dict.items(), start=1):
                        sub_status = sub_info.get("status", "-")
                        sub_time_section = sub_info.get("time", 0.0)
                        sub_loop_time = sub_info.get("loop_time", 0.0)
                        sub_total = round(sub_time_section + sub_loop_time, 2)
                        print(f"    {idx:>2}. {sub_step:<70} | {sub_status:<10} | {sub_total:>8.2f}")
        print("-" * 120)
        print(f"{'Total execution time':<80} | {'-':<10} | {update_time_total:>8.2f}s")
        print("=" * 120)