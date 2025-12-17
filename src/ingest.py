"""
==================================================================
FACEBOOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Facebook Ads fetching module 
into Google BigQuery, establishing the foundational raw layer used 
for centralized storage and historical retention.

It manages the complete ingestion flow from authentication to 
data fetching, schema validation and loading into Google BigQuery 
tables segmented by campaign, ad, creative and metadata.

✔️ Supports both append and truncate modes via write_disposition
✔️ Validates data structure using centralized schema utilities
✔️ Applies lightweight normalization required for raw-layer loading
✔️ Implements granular logging and CSV-based error traceability
✔️ Ensures pipeline reliability through retry and checkpoint logic

⚠️ This module is dedicated solely to raw-layer ingestion.  
It does not handle advanced transformations, metric modeling 
or aggregated data processing beyond the ingestion boundary.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities forintegration
import logging

# Add Python time ultilities for integration
import time

# Add Python UUID ultilities for integration
import uuid

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google API core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Facebook Ads modules for handling
from src.fetch import (
    fetch_campaign_metadata, 
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights, 
    fetch_ad_insights,
)
from src.schema import enforce_table_schema

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

# 1. INGEST FACEBOOK ADS METADATA

# 1.1. Ingest Facebook Ads campaign metadata
def ingest_campaign_metadata(ingest_campaign_ids: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")

    # 1.1.1. Start timing Facebook Ads campaign metadata ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.1.2. Trigger to fetch Facebook Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads campaign metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")
            ingest_results_fetched = fetch_campaign_metadata(fetch_campaign_ids=ingest_campaign_ids)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]            
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")               
            elif ingest_status_fetched == "fetch_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.3. Trigger to enforce schema for Facebook Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads campaign metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads campaign metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads campaign metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_campaign_metadata")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]            
            if ingest_status_enforced == "schema_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            elif ingest_status_enforced == "schema_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.4. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
            ingest_sections_status[ingest_section_name] = "succeed"   
            print(f"🔍 [INGEST] Preparing to ingest Facebook campaign metadata for {len(ingest_df_enforced)} enforced row(s) to Google BigQuery table_id {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest Facebook campaign metadata for {len(ingest_df_enforced)} enforced row(s) to Google BigQuery table_id {raw_table_campaign}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.5. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.6. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["account_id", "campaign_id"]
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads campaign metadata table {raw_table_campaign} existence...")
                logging.info(f"🔍 [INGEST] Checking TikFacebookTok Ads campaign metadata table {raw_table_campaign} existence...")
                google_bigquery_client.get_table(raw_table_campaign)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception as e:
                print(f"❌ [INGEST] Failed to check Facebook Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
            if not ingest_table_existed:
                try:
                    print(f"⚠️ [INGEST] Facebook Ads campaign metadata table {raw_table_campaign} not found then table creation will be proceeding...")
                    logging.info(f"⚠️ [INGEST] Facebook Ads campaign metadata table {raw_table_campaign} not found then table creation will be proceeding...")
                    for col, dtype in ingest_df_deduplicated.dtypes.items():
                        if dtype.name.startswith("int"):
                            bq_type = "INT64"
                        elif dtype.name.startswith("float"):
                            bq_type = "FLOAT64"
                        elif dtype.name == "bool":
                            bq_type = "BOOL"
                        elif "datetime" in dtype.name:
                            bq_type = "TIMESTAMP"
                        else:
                            bq_type = "STRING"
                        table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                    table_configuration_defined = bigquery.Table(raw_table_campaign, schema=table_schemas_defined)
                    table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                    if table_partition_effective:
                        table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_effective
                        )
                    table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                    if table_clusters_filtered:  
                        table_configuration_defined.clustering_fields = table_clusters_filtered                  
                    query_table_create = google_bigquery_client.create_table(table_configuration_defined)
                    query_table_id = query_table_create.full_table_id
                    print(f"✅ [INGEST] Successfully created Facebook Ads campaign metadata table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads campaign metadata table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads campaign metadata table {raw_table_campaign} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads campaign metadata table {raw_table_campaign} due to {e}.")
            else:
                print(f"🔄 [INGEST] Found Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion will be proceeding...")
                ingest_keys_unique = ingest_df_deduplicated[["account_id", "campaign_id"]].dropna().drop_duplicates()
                if not ingest_keys_unique.empty:
                    try:
                        print(f"🔍 [INGEST] Creating temporary table contains duplicated Facebook Ads campaign metadata for batch deletion...")
                        logging.info(f"🔍 [INGEST] Creating temporary table contains duplicated Facebook Ads campaign metadata for batch deletion...")
                        temporary_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                        job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                        job_load_load = google_bigquery_client.load_table_from_dataframe(
                            ingest_keys_unique, 
                            temporary_table_id, 
                            job_config=job_load_config
                            )
                        job_load_result = job_load_load.result()
                        created_table_id = f"{job_load_load.destination.project}.{job_load_load.destination.dataset_id}.{job_load_load.destination.table_id}"
                        print(f"✅ [INGEST] Successfully created temporary Facebook Ads campaign metadata table {created_table_id} for batch deletion.")
                        logging.info(f"✅ [INGEST] Successfully created temporary Facebook Ads campaign metadata table {created_table_id} for batch deletion.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create temporary Facebook Ads campaign metadata table {temporary_table_id} for batch deletion due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create temporary Facebook Ads campaign metadata table {temporary_table_id} for batch deletion due to {e}.")
                    try:
                        query_delete_condition = " AND ".join([
                            f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                            for col in ["account_id", "campaign_id"]
                            ])
                        query_delete_config = f"""
                            DELETE FROM `{raw_table_campaign}` AS main
                            WHERE EXISTS (
                                SELECT 1 FROM `{created_table_id}` AS temp
                                WHERE {query_delete_condition}
                                )
                            """
                        query_delete_load = google_bigquery_client.query(query_delete_config)
                        query_delete_result = query_delete_load.result()
                        ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                        google_bigquery_client.delete_table(
                            created_table_id, 
                            not_found_ok=True
                            )                    
                        print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads campaign metadata table {raw_table_campaign}.")
                        logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads campaign metadata table {raw_table_campaign}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads campaign metadata table {raw_table_campaign} by batch deletion due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads campaign metadata table {raw_table_campaign} by batch deletion due to {e}.")
                else:
                    print(f"⚠️ [INGEST] No unique account_id and campaign_id keys found in Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique account_id and campaign_id keys found in Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion is skipped.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign metadata due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.7. Upload Facebook Ads campaign metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload Facebook Ads campaign metadata to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job_load_load = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated, 
                raw_table_campaign, 
                job_config=job_load_config
                )
            job_load_result = job_load_load.result()
            ingest_rows_uploaded = job_load_load.output_rows
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.8. Summarize ingestion results for Facebook Ads campaign metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ingest_campaign_ids)
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
            ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
                }
            for ingest_section_summary in ingest_sections_summary
            }     
        if ingest_sections_failed:
            ingest_status_final = "ingest_failed_all"
            print(f"❌ [INGEST] Failed to complete Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")            
        elif ingest_rows_output == ingest_rows_input:
            ingest_status_final = "ingest_succeed_all"
            print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")    
        else:
            ingest_status_final = "ingest_succeed_partial"
            print(f"⚠️ [INGEST] Partially completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")       
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
                },
            }
    return ingest_results_final

# 1.2. Ingest Facebook Ads adset metadata to Google BigQuery
def ingest_adset_metadata(ingest_adset_ids: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads adset metadata for {len(ingest_adset_ids)} adset_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads adset metadata for {len(ingest_adset_ids)} adset_id(s)...")

    # 1.2.1. Start timing Facebook Ads adset metadata ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads adset metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads adset metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.2.2. Trigger to fetch Facebook Ads adset metadata
        ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads adset metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads adset metadata for {len(ingest_adset_ids)} adset_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads adset metadata for {len(ingest_adset_ids)} adset_id(s)...")
            ingest_results_fetched = fetch_adset_metadata(fetch_adset_ids=ingest_adset_ids)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]            
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")               
            elif ingest_status_fetched == "fetch_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.3. Trigger to enforce schema for Facebook Ads adset metadata
        ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads adset metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads adset metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads adset metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_adset_metadata")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]            
            if ingest_status_enforced == "schema_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            elif ingest_status_enforced == "schema_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger TikFacebookTok Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.4. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_adset = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"🔍 [INGEST] Preparing to ingest Facebook Ads adset metadata for {len(ingest_df_enforced)} enforced row(s) with Google BigQuery table_id {raw_table_adset}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest Facebook Ads adset metadata for {len(ingest_df_enforced)} enforced row(s) with Google BigQuery table_id {raw_table_adset}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.5. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.6. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["account_id", "adset_id"]
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads adset metadata table {raw_table_adset} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads adset metadata table {raw_table_adset} existence...")
                google_bigquery_client.get_table(raw_table_adset)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception as e:
                print(f"❌ [INGEST] Failed to check Facebook Ads adset metadata table {raw_table_adset} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads adset metadata table {raw_table_adset} existence due to {e}.")
            if not ingest_table_existed:
                try:
                    print(f"⚠️ [INGEST] Facebook Ads adset metadata table {raw_table_adset} not found then table creation will be proceeding...")
                    logging.info(f"⚠️ [INGEST] Facebook Ads adset metadata table {raw_table_adset} not found then table creation will be proceeding...")
                    for col, dtype in ingest_df_deduplicated.dtypes.items():
                        if dtype.name.startswith("int"):
                            bq_type = "INT64"
                        elif dtype.name.startswith("float"):
                            bq_type = "FLOAT64"
                        elif dtype.name == "bool":
                            bq_type = "BOOL"
                        elif "datetime" in dtype.name:
                            bq_type = "TIMESTAMP"
                        else:
                            bq_type = "STRING"
                        table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                    table_configuration_defined = bigquery.Table(raw_table_adset, schema=table_schemas_defined)
                    table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                    if table_partition_effective:
                        table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_effective
                        )
                    table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                    if table_clusters_filtered:  
                        table_configuration_defined.clustering_fields = table_clusters_filtered  
                    query_table_create = google_bigquery_client.create_table(table_configuration_defined)
                    query_table_id = query_table_create.full_table_id
                    print(f"✅ [INGEST] Successfully created Facebook Ads adset metadata table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads adset metadata table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads adset metadata table {raw_table_adset} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads adset metadata table {raw_table_adset} due to {e}.")
            else:
                print(f"🔄 [INGEST] Found Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion will be proceeding...")
                ingest_keys_unique = ingest_df_deduplicated[["account_id", "adset_id"]].dropna().drop_duplicates()
                if not ingest_keys_unique.empty:
                    try:
                        print(f"🔍 [INGEST] Creating temporary table contains duplicated Facebook Ads adset metadata for batch deletion...")
                        logging.info(f"🔍 [INGEST] Creating temporary table contains duplicated Facebook Ads adset metadata for batch deletion...")
                        temporary_table_id = f"{PROJECT}.{raw_dataset}.temp_table_adset_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                        job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                        job_load_load = google_bigquery_client.load_table_from_dataframe(
                            ingest_keys_unique, 
                            temporary_table_id, 
                            job_config=job_load_config
                            )
                        job_load_result = job_load_load.result()
                        created_table_id = f"{job_load_load.destination.project}.{job_load_load.destination.dataset_id}.{job_load_load.destination.table_id}"
                        print(f"✅ [INGEST] Successfully created temporary Facebook Ads adset metadata table {created_table_id} for batch deletion.")
                        logging.info(f"✅ [INGEST] Successfully created temporary Facebook Ads adset metadata table {created_table_id} for batch deletion.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create temporary Facebook Ads adset metadata table {temporary_table_id} for batch deletion due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create temporary Facebook Ads adset metadata table {temporary_table_id} for batch deletion due to {e}.")                        
                    try:
                        query_delete_condition = " AND ".join([
                            f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                            for col in ["account_id", "adset_id"]
                            ])
                        query_delete_config = f"""
                            DELETE FROM `{raw_table_adset}` AS main
                            WHERE EXISTS (
                                SELECT 1 FROM `{temporary_table_id}` AS temp
                                WHERE {query_delete_condition}
                                )
                            """
                        query_delete_load = google_bigquery_client.query(query_delete_config)
                        query_delete_result = query_delete_load.result()
                        ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                        google_bigquery_client.delete_table(
                            temporary_table_id, 
                            not_found_ok=True
                            )                    
                        print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads adset metadata table {raw_table_adset}.")
                        logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads adset metadata table {raw_table_adset}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads adset metadata table {raw_table_adset} by batch deletion due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads adset metadata table {raw_table_adset} by batch deletion due to {e}.")
                else:
                    print(f"⚠️ [INGEST] No unique account_id and adset_id keys found in Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique account_id and adset_id keys found in Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion is skipped.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_adset} if it not exist for Facebook Ads adset metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_adset} if it not exist for Facebook Ads adset metadata due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.7. Upload Facebook Ads adset metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload Facebook Ads adset metadata to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}...")
            job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job_load_load = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated, 
                raw_table_adset, 
                job_config=job_load_config
                )
            job_load_result = job_load_load.result()
            ingest_rows_uploaded = job_load_load.output_rows
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebook Ads adset metadata to Google BigQuery table {raw_table_adset} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads adset metadata to Google BigQuery table {raw_table_adset} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.8. Summarize ingestion results for Facebook Ads adset metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ingest_adset_ids)
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
            ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
                }
            for ingest_section_summary in ingest_sections_summary
            }     
        if ingest_sections_failed:
            ingest_status_final = "ingest_failed_all"
            print(f"❌ [INGEST] Failed to complete Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")            
        elif ingest_rows_output == ingest_rows_input:
            ingest_status_final = "ingest_succeed_all"
            print(f"🏆 [INGEST] Successfully completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")    
        else:
            ingest_status_final = "ingest_succeed_partial"
            print(f"⚠️ [INGEST] Partially completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.") 
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 1.3. Ingest Facebook Ads ad metadata to Google BigQuery
def ingest_ad_metadata(ingest_ad_ids: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads ad metadata for {len(ingest_ad_ids)} ad_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads ad metadata for {len(ingest_ad_ids)} ad_id(s)...")

    # 1.3.1. Start timing Facebook Ads ad metadata ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.3.2. Trigger to fetch Facebook Ads ad metadata
        ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads ad metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad metadata for {len(ingest_ad_ids)} ad_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad metadata for {len(ingest_ad_ids)} ad_id(s)...")
            ingest_results_fetched = fetch_ad_metadata(fetch_ids_ad=ingest_ad_ids)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]            
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")               
            elif ingest_status_fetched == "fetch_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.3.3. Trigger to enforce schema for Facebook Ads ad metadata
        ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads ad metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_ad_metadata")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]            
            if ingest_status_enforced == "schema_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            elif ingest_status_enforced == "schema_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Facebook Ads ad metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Facebook Ads ad metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads ad metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger TikFacebookTok Ads ad metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.3.4. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_ad = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad metadata for {len(ingest_df_enforced)} enforced row(s) with Google BigQuery table_id {raw_table_ad}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad metadata for {len(ingest_df_enforced)} enforced row(s) with Google BigQuery table_id {raw_table_ad}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)      

    # 1.3.5. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.3.6. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["account_id", "ad_id"]
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads ad metadata table {raw_table_ad} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads ad metadata table {raw_table_ad} existence...")
                google_bigquery_client.get_table(raw_table_ad)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception as e:
                print(f"❌ [INGEST] Failed to check Facebook Ads ad metadata table {raw_table_ad} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads ad metadata table {raw_table_ad} existence due to {e}.")
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] Facebook Ads ad metadata table {raw_table_ad} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads ad metadata table {raw_table_ad} not found then table creation will be proceeding...")
                for col, dtype in ingest_df_deduplicated.dtypes.items():
                    if dtype.name.startswith("int"):
                        bq_type = "INT64"
                    elif dtype.name.startswith("float"):
                        bq_type = "FLOAT64"
                    elif dtype.name == "bool":
                        bq_type = "BOOL"
                    elif "datetime" in dtype.name:
                        bq_type = "TIMESTAMP"
                    else:
                        bq_type = "STRING"
                    table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                table_configuration_defined = bigquery.Table(raw_table_ad, schema=table_schemas_defined)
                table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                if table_partition_effective:
                    table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=table_partition_effective
                    )
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:  
                    table_configuration_defined.clustering_fields = table_clusters_filtered  
                try:    
                    print(f"🔍 [INGEST] Creating Facebook Ads ad metadata table defined name {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads ad metadata table defined name {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    query_table_create = google_bigquery_client.create_table(table_configuration_defined)
                    query_table_id = query_table_create.full_table_id
                    print(f"✅ [INGEST] Successfully created Facebook Ads ad metadata table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad metadata table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads ad metadata table {raw_table_ad} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads ad metadata table {raw_table_ad} due to {e}.")
            else:
                print(f"🔄 [INGEST] Found Facebook Ads ad metadata table {raw_table_ad} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads ad metadata table {raw_table_ad} then existing row(s) deletion will be proceeding...")
                ingest_keys_unique = ingest_df_deduplicated[["account_id", "ad_id"]].dropna().drop_duplicates()
                if not ingest_keys_unique.empty:
                    table_id_temporary = f"{PROJECT}.{raw_dataset}.temp_table_ad_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    job_load_load = google_bigquery_client.load_table_from_dataframe(
                        ingest_keys_unique, 
                        table_id_temporary, 
                        job_config=job_load_config
                        )
                    job_load_result = job_load_load.result()
                    query_delete_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["account_id", "ad_id"]
                        ])
                    query_delete_config = f"""
                        DELETE FROM `{raw_table_ad}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{table_id_temporary}` AS temp
                            WHERE {query_delete_condition}
                            )
                        """
                    query_delete_load = google_bigquery_client.query(query_delete_config)
                    query_delete_result = query_delete_load.result()
                    ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                    google_bigquery_client.delete_table(
                        table_id_temporary, 
                        not_found_ok=True
                        )                    
                    print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad metadata table {raw_table_ad}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad metadata table {raw_table_ad}.")
                else:
                    print(f"⚠️ [INGEST] No unique account_id and ad_id keys found in Facebook Ads ad metadata table {raw_table_ad} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique account_id and ad_id keys found in Facebook Ads ad metadata table {raw_table_ad} then existing row(s) deletion is skipped.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad metadata due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.3.7. Upload Facebook Ads ad metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload Facebook Ads ad metadata to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}...")
            job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job_load_load = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated, 
                raw_table_ad, 
                job_config=job_load_config
                )
            job_load_result = job_load_load.result()
            ingest_rows_uploaded = job_load_load.output_rows
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebook Ads ad metadata to Google BigQuery table {raw_table_ad} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads ad metadata to Google BigQuery table {raw_table_ad} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.3.8. Summarize ingestion results for Facebook Ads ad metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ingest_ad_ids)
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
            ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
                }
            for ingest_section_summary in ingest_sections_summary
            }     
        if ingest_sections_failed:
            ingest_status_final = "ingest_failed_all"
            print(f"❌ [INGEST] Failed to complete Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")            
        elif ingest_rows_output == ingest_rows_input:
            ingest_status_final = "ingest_succeed_all"
            print(f"🏆 [INGEST] Successfully completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")    
        else:
            ingest_status_final = "ingest_succeed_partial"
            print(f"⚠️ [INGEST] Partially completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.") 
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 1.4. Ingest Facebook Ads ad creative to Google BigQuery
def ingest_ad_creative(ingest_ad_ids: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads ad creative for {len(ingest_ad_ids)} ad_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads ad creative for {len(ingest_ad_ids)} ad_id(s)...")

    # 1.4.1. Start timing Facebook Ads ad creative ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad creative at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad creative at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:
  
    # 1.4.2. Trigger to fetch Facebook Ads ad creative
        ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads ad creative"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad creative for {len(ingest_ad_ids)} ad_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad creative for {len(ingest_ad_ids)} ad_id(s)...")
            ingest_results_fetched = fetch_ad_creative(fetch_ad_ids=ingest_ad_ids)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]            
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")               
            elif ingest_status_fetched == "fetch_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.4.3. Trigger to enforce schema for Facebook Ads ad creative
        ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads ad creative"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad creative with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad creative with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_ad_creative")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]            
            if ingest_status_enforced == "schema_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad creative schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad creative schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            elif ingest_status_enforced == "schema_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Facebook Ads ad creative schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Facebook Ads ad creative schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger TikFacebookTok Ads ad creative schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.4.4. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_creative = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_creative"
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad creative for {len(ingest_df_enforced)} enforced row(s) with Google BigQuery table_id {raw_table_creative}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad creative for {len(ingest_df_enforced)} enforced row(s) with Google BigQuery table_id {raw_table_creative}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)         

    # 1.4.5. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.4.6. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["account_id", "ad_id"]
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads ad creative table {raw_table_creative} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads ad creative table {raw_table_creative} existence...")
                google_bigquery_client.get_table(raw_table_creative)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception as e:
                print(f"❌ [INGEST] Failed to check Facebook Ads ad creative table {raw_table_creative} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads ad creative table {raw_table_creative} existence due to {e}.")
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] Facebook Ads ad creative table {raw_table_creative} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads ad creative table {raw_table_creative} not found then table creation will be proceeding...")
                for col, dtype in ingest_df_deduplicated.dtypes.items():
                    if dtype.name.startswith("int"):
                        bq_type = "INT64"
                    elif dtype.name.startswith("float"):
                        bq_type = "FLOAT64"
                    elif dtype.name == "bool":
                        bq_type = "BOOL"
                    elif "datetime" in dtype.name:
                        bq_type = "TIMESTAMP"
                    else:
                        bq_type = "STRING"
                    table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                table_configuration_defined = bigquery.Table(raw_table_creative, schema=table_schemas_defined)
                table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                if table_partition_effective:
                    table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=table_partition_effective
                    )
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:  
                    table_configuration_defined.clustering_fields = table_clusters_filtered  
                try:    
                    print(f"🔍 [INGEST] Creating Facebook Ads ad creative table defined name {raw_table_creative} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads ad creative table defined name {raw_table_creative} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    query_table_create = google_bigquery_client.create_table(table_configuration_defined)
                    query_table_id = query_table_create.full_table_id
                    print(f"✅ [INGEST] Successfully created Facebook Ads ad creative table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad creative table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads ad creative table {raw_table_creative} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads ad creative table {raw_table_creative} due to {e}.")
            else:
                print(f"🔄 [INGEST] Found Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion will be proceeding...")
                ingest_keys_unique = ingest_df_deduplicated[["account_id", "ad_id"]].dropna().drop_duplicates()
                if not ingest_keys_unique.empty:
                    table_id_temporary = f"{PROJECT}.{raw_dataset}.temp_table_ad_creative_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    job_load_load = google_bigquery_client.load_table_from_dataframe(
                        ingest_keys_unique, 
                        table_id_temporary, 
                        job_config=job_load_config
                        )
                    job_load_result = job_load_load.result()
                    query_delete_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["account_id", "ad_id"]
                        ])
                    query_delete_config = f"""
                        DELETE FROM `{raw_table_creative}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{table_id_temporary}` AS temp
                            WHERE {query_delete_condition}
                            )
                        """
                    query_delete_load = google_bigquery_client.query(query_delete_config)
                    query_delete_result = query_delete_load.result()
                    ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                    google_bigquery_client.delete_table(
                        table_id_temporary, 
                        not_found_ok=True
                        )                    
                    print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad creative table {raw_table_creative}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad creative table {raw_table_creative}.")
                else:
                    print(f"⚠️ [INGEST] No unique account_id and ad_id keys found in Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique account_id and ad_id keys found in Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion is skipped.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_creative} if it not exist for Facebook Ads ad creative due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_creative} if it not exist for Facebook Ads ad creative due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.4.7. Upload Facebook Ads ad creative to Google BigQuery
        ingest_section_name = "[INGEST] Upload Facebook Ads ad creative to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}...")
            job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job_load_load = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated, 
                raw_table_creative, 
                job_config=job_load_config
                )
            job_load_result = job_load_load.result()
            ingest_rows_uploaded = job_load_load.output_rows
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebook Ads ad creative to Google BigQuery table {raw_table_creative} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads ad creative to Google BigQuery table {raw_table_creative} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.4.8. Summarize ingestion results for Facebook Ads ad creative
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ingest_ad_ids)
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
            ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
                }
            for ingest_section_summary in ingest_sections_summary
            }     
        if ingest_sections_failed:
            ingest_status_final = "ingest_failed_all"
            print(f"❌ [INGEST] Failed to complete Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")            
        elif ingest_rows_output == ingest_rows_input:
            ingest_status_final = "ingest_succeed_all"
            print(f"🏆 [INGEST] Successfully completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")    
        else:
            ingest_status_final = "ingest_succeed_partial"
            print(f"⚠️ [INGEST] Partially completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.") 
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 2. INGEST FACEBOOK ADS INSIGHTS

# 2.1. Ingest Facebook Ads campaign insights
def ingest_campaign_insights(
    ingest_date_start: str,
    ingest_date_end: str,
) -> pd.DataFrame:  
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign insights from {ingest_date_start} to {ingest_date_end}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign insights from {ingest_date_start} to {ingest_date_end}...")

    # 2.1.1. Start timing Facebook Ads campaign insights ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh") 
    ingest_dates_uploaded = []
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    ingest_loops_time = {
        "[INGEST] Trigger to fetch Facebook Ads campaign insights": 0.0,
        "[INGEST] Trigger to enforce schema for Facebook Ads campaign insights": 0.0,
        "[INGEST] Prepare Google BigQuery table_id for ingestion": 0.0,
        "[INGEST] Delete existing row(s) or create new table if not exist": 0.0,
        "[INGEST] Upload Facebook Ads campaign insights to Google BigQuery": 0.0,
        "[INGEST] Cooldown before next Facebook Ads campaign insights fetch": 0.0,
    }
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights from {ingest_date_start} to {ingest_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights from {ingest_date_start} to {ingest_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 2.1.2. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 2.1.3. Trigger to fetch Facebook Ads campaign insights
        ingest_date_list = pd.date_range(start=ingest_date_start, end=ingest_date_end).strftime("%Y-%m-%d").tolist()
        for ingest_date_indexed, ingest_date_separated in enumerate(ingest_date_list):    
            ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads campaign insights"
            ingest_section_start = time.time()
            try:
                print(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaigns insights for {ingest_date_separated}...")
                logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaigns insights for {ingest_date_separated}...")
                ingest_results_fetched = fetch_campaign_insights(ingest_date_separated, ingest_date_separated)
                ingest_df_fetched = ingest_results_fetched["fetch_df_final"]                
                ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
                ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
                if ingest_status_fetched == "fetch_succeed_all":
                    ingest_sections_status[ingest_section_name] = "succeed"
                    print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                    
                elif ingest_status_fetched == "fetch_succeed_partial":
                    ingest_sections_status[ingest_section_name] = "partial"
                    print(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                    
                else:
                    ingest_sections_status[ingest_section_name] = "failed"
                    print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")     
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)
                
    # 2.1.4. Trigger to enforce schema for Facebook Ads campaign insights
            ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads campaign insights"
            ingest_section_start = time.time()
            try:
                print(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
                logging.info(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
                ingest_results_enforced = enforce_table_schema(schema_df_input=ingest_df_fetched,schema_type_mapping="ingest_campaign_insights")
                ingest_df_enforced = ingest_results_enforced["schema_df_final"]                
                ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
                ingest_status_enforced = ingest_results_enforced["schema_status_final"]
                if ingest_status_enforced == "schema_succeed_all":
                    ingest_sections_status[ingest_section_name] = "succeed"
                    print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")                    
                elif ingest_status_enforced == "schema_succeed_partial":
                    ingest_sections_status[ingest_section_name] = "partial"
                    print(f"⚠️ [FETCH] Partially triggered Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.warning(f"⚠️ [FETCH] Partially triggered Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                else:
                    ingest_sections_status[ingest_section_name] = "failed"
                    print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)         
        
    # 2.1.5. Prepare Google BigQuery table_id for ingestion
            ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
            ingest_section_start = time.time()
            try:
                first_date = pd.to_datetime(ingest_df_fetched["date_start"].dropna().iloc[0])
                y, m = first_date.year, first_date.month
                raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
                raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_campaign}...")
                logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_campaign}...")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)          

    # 2.1.6. Delete existing row(s) or create new table if not exist
            ingest_section_name = "[INGEST] Delete existing row(s) or create new table if not exist"
            ingest_section_start = time.time()
            try:
                ingest_df_deduplicated = ingest_df_enforced.drop_duplicates().reset_index(drop=True)
                table_clusters_defined = []
                table_schemas_defined = []
                try:
                    print(f"🔍 [INGEST] Checking Facebook Ads campaign insights table {raw_table_campaign} existence...")
                    logging.info(f"🔍 [INGEST] Checking Facebook Ads campaign insights table {raw_table_campaign} existence...")
                    google_bigquery_client.get_table(raw_table_campaign)
                    ingest_table_existed = True
                except NotFound:
                    ingest_table_existed = False
                except Exception as e:
                    print(f"❌ [INGEST] Failed to check Facebook Ads campaign insights table {raw_table_campaign} existence due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to check Facebook Ads campaign insights table {raw_table_campaign} existence due to {e}.")
                if not ingest_table_existed:
                    print(f"⚠️ [INGEST] Facebook Ads campaign insights table {raw_table_campaign} not found then table creation will be proceeding...")
                    logging.info(f"⚠️ [INGEST] Facebook Ads campaign insights table {raw_table_campaign} not found then table creation will be proceeding...")
                    for col, dtype in ingest_df_deduplicated.dtypes.items():
                        if dtype.name.startswith("int"):
                            bq_type = "INT64"
                        elif dtype.name.startswith("float"):
                            bq_type = "FLOAT64"
                        elif dtype.name == "bool":
                            bq_type = "BOOL"
                        elif "datetime" in dtype.name:
                            bq_type = "TIMESTAMP"
                        else:
                            bq_type = "STRING"
                        table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                    table_configuration_defined = bigquery.Table(raw_table_campaign, schema=table_schemas_defined)
                    table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                    if table_partition_effective:
                        table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_effective
                        )                    
                    table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns] if table_clusters_defined else []
                    if table_clusters_filtered:
                        table_configuration_defined.clustering_fields = table_clusters_filtered
                    try:
                        print(f"🔍 [INGEST] Creating Facebook Ads campaign insights table {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        logging.info(f"🔍 [INGEST] Creating Facebook Ads campaign insights table {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        ingest_table_create = google_bigquery_client.create_table(table_configuration_defined)
                        ingest_table_id = ingest_table_create.full_table_id
                        print(f"✅ [INGEST] Successfully created Facebook Ads campaign insights table {ingest_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                        logging.info(f"✅ [INGEST] Successfully created Facebook Ads campaign insights table {ingest_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create Facebook Ads campaign insights table {raw_table_campaign} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create Facebook Ads campaign insights table {raw_table_campaign} due to {e}.")
                else:
                    ingest_dates_new = ingest_df_deduplicated["date_start"].dropna().unique().tolist()
                    query_select_config = f"SELECT DISTINCT date_start FROM `{raw_table_campaign}`"
                    query_select_load = google_bigquery_client.query(query_select_config)
                    query_select_result = query_select_load.result()
                    ingest_dates_existed = [row.date_start for row in query_select_result]
                    ingest_dates_overlapped = set(ingest_dates_new) & set(ingest_dates_existed)
                    if ingest_dates_overlapped:
                        print(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads campaign insights {raw_table_campaign} table then deletion will be proceeding...")
                        logging.warning(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads campaign insights {raw_table_campaign} table then deletion will be proceeding...")
                        for ingest_date_overlapped in ingest_dates_overlapped:
                            query_delete_config = f"""
                                DELETE FROM `{raw_table_campaign}`
                                WHERE date_start = @date_value
                            """
                            job_query_config = bigquery.QueryJobConfig(
                                query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", ingest_date_overlapped)]
                            )
                            try:
                                query_delete_load = google_bigquery_client.query(query_delete_config, job_config=job_query_config)
                                query_delete_result = query_delete_load.result()
                                ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                                print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}.")
                                logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}.")
                            except Exception as e:
                                print(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign} due to {e}.")
                                logging.error(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign} due to {e}.")
                    else:
                        print(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads campaign insights found in Google BigQuery {raw_table_campaign} table then deletion is skipped.")
                        logging.info(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads campaign insights found in Google BigQuery {raw_table_campaign} table then deletion is skipped.")
                ingest_sections_status[ingest_section_name] = "succeed"
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign insights due to {e}.")
                logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign insights due to {e}.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)

    # 2.1.7. Upload Facebook Ads campaign insights to Google BigQuery
            ingest_section_name = "[INGEST] Upload Facebook Ads campaign insights to Google BigQuery"
            ingest_section_start = time.time()
            try:
                print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated deduplicated row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}...")
                logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated deduplicated row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}...")
                job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job_load_load = google_bigquery_client.load_table_from_dataframe(
                    ingest_df_deduplicated,
                    raw_table_campaign,
                    job_config=job_load_config
                )
                job_load_result = job_load_load.result()
                ingest_rows_uploaded = job_load_load.output_rows
                ingest_dates_uploaded.append(ingest_df_deduplicated.copy())
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}.")
                logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}.")
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign} due to {e}.")
                logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign} due to {e}.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)              

    # 2.1.8. Cooldown before next Facebook Ads campaign insights fetch
            ingest_section_name = "[INGEST] Cooldown before next Facebook Ads campaign insights fetch"
            ingest_section_start = time.time()
            try:
                if ingest_date_indexed < len(ingest_date_list) - 1:
                    ingest_cooldown_queued = ingest_results_fetched["fetch_summary_final"].get("fetch_cooldown_queued", 60)
                    print(f"🔁 [INGEST] Waiting {ingest_cooldown_queued}s cooldown before triggering to fetch next day of Facebook Ads campaign insights...")
                    logging.info(f"🔁 [INGEST] Waiting {ingest_cooldown_queued}s cooldown before triggering to fetch next day of Facebook Ads campaign insights...")
                    time.sleep(ingest_cooldown_queued)
                ingest_sections_status[ingest_section_name] = "succeed"
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to set cooldown for {ingest_cooldown_queued}s before triggering to fetch next day of Facebook Ads campaign insights due to {e}")
                logging.error(f"❌ [INGEST] Failed to set cooldown for {ingest_cooldown_queued}s before triggering to fetch next day of Facebook Ads campaign insights due to {e}")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)

    # 2.1.9. Summarize ingestion results for Facebook Ads campaign insights
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = pd.concat(ingest_dates_uploaded or [], ignore_index=True)
        ingest_sections_total = len(ingest_sections_status)
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"]
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_dates_input = len(ingest_date_list)
        ingest_dates_output = len(ingest_dates_uploaded)
        ingest_dates_failed = ingest_dates_input - ingest_dates_output
        ingest_rows_output = ingest_rows_uploaded
        ingest_section_all = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys()) +
            list(ingest_loops_time.keys())
            ))
        ingest_sections_detail = {}
        for ingest_section_separated in ingest_section_all:
            ingest_section_time = (
                ingest_loops_time.get(ingest_section_separated)
                if ingest_section_separated in ingest_loops_time
                else ingest_sections_time.get(ingest_section_separated)
                )
            ingest_sections_detail[ingest_section_separated] = {
                "status": ingest_sections_status.get(ingest_section_separated, "unknown"),
                "time": round(ingest_section_time or 0.0, 2),
                "type": "loop" if ingest_section_separated in ingest_loops_time else "single"
                }
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_dates_output == ingest_dates_input:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign insights ingestion from from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign insights ingestion from from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"            
        else:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded,
                "ingest_sections_failed": ingest_sections_failed,
                "ingest_sections_detail": ingest_sections_detail,
                "ingest_dates_input": ingest_dates_input,
                "ingest_dates_output": ingest_dates_output,
                "ingest_dates_failed": ingest_dates_failed,
                "ingest_rows_output": ingest_rows_output,
                }
            }
    return ingest_results_final

# 2.2. Ingest Facebook Ad ad insight to Google BigQuery raw tables
def ingest_ad_insights(
    ingest_date_start: str,
    ingest_date_end: str,
) -> pd.DataFrame:  
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads ad insights from {ingest_date_start} to {ingest_date_end}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads ad insights from {ingest_date_start} to {ingest_date_end}...")

    # 2.2.1. Start timing Facebook Ads ad insights ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")     
    ingest_dates_uploaded = []
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    ingest_loops_time = {
        "[INGEST] Trigger to fetch Facebook Ads ad insights": 0.0,
        "[INGEST] Trigger to enforce schema for Facebook Ads ad insights": 0.0,
        "[INGEST] Prepare Google BigQuery table_id for ingestion": 0.0,
        "[INGEST] Delete existing row(s) or create new table if not exist": 0.0,
        "[INGEST] Upload Facebook Ads ad insights to Google BigQuery": 0.0,
        "[INGEST] Cooldown before next Facebook Ads ad insights fetch": 0.0,
    }
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad insights from {ingest_date_start} to {ingest_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad insights from {ingest_date_start} to {ingest_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 2.2.2. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 2.2.3. Trigger to fetch Facebook Ads ad insights
        ingest_date_list = pd.date_range(start=ingest_date_start, end=ingest_date_end).strftime("%Y-%m-%d").tolist()
        for ingest_date_indexed, ingest_date_separated in enumerate(ingest_date_list):
            ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads ad insights"
            ingest_section_start = time.time()
            try:
                print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad insights for {ingest_date_separated}...")
                logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad insights for {ingest_date_separated}...")
                ingest_results_fetched = fetch_ad_insights(ingest_date_separated, ingest_date_separated)
                ingest_df_fetched = ingest_results_fetched["fetch_df_final"]                
                ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
                ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
                if ingest_status_fetched == "fetch_succeed_all":
                    ingest_sections_status[ingest_section_name] = "succeed"
                    print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                    
                elif ingest_status_fetched == "fetch_succeed_partial":
                    ingest_sections_status[ingest_section_name] = "partial"
                    print(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                    
                else:
                    ingest_sections_status[ingest_section_name] = "failed"
                    print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")     
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)

    # 2.2.4. Trigger to enforce schema for Facebook Ads ad insights
            ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads ad insights"
            ingest_section_start = time.time()
            try:
                print(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
                logging.info(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
                ingest_results_enforced = enforce_table_schema(schema_df_input=ingest_df_fetched,schema_type_mapping="ingest_ad_insights")
                ingest_df_enforced = ingest_results_enforced["schema_df_final"]                
                ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
                ingest_status_enforced = ingest_results_enforced["schema_status_final"]
                if ingest_status_enforced == "schema_succeed_all":
                    ingest_sections_status[ingest_section_name] = "succeed"
                    print(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")                    
                elif ingest_status_enforced == "schema_succeed_partial":
                    ingest_sections_status[ingest_section_name] = "partial"
                    print(f"⚠️ [FETCH] Partially triggered Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.warning(f"⚠️ [FETCH] Partially triggered Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                else:
                    ingest_sections_status[ingest_section_name] = "failed"
                    print(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)     

    # 2.2.5. Prepare Google BigQuery table_id for ingestion
            ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
            ingest_section_start = time.time()
            try:
                first_date = pd.to_datetime(ingest_df_fetched["date_start"].dropna().iloc[0])
                y, m = first_date.year, first_date.month
                raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
                raw_table_ad = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_ad}...")
                logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_ad}...")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)
    
    # 2.2.6. Delete existing row(s) or create new table if not exist
            ingest_section_name = "[INGEST] Delete existing row(s) or create new table if not exist"
            ingest_section_start = time.time()
            try:
                ingest_df_deduplicated = ingest_df_enforced.drop_duplicates().reset_index(drop=True)
                table_clusters_defined = []
                table_schemas_defined = []
                try:
                    print(f"🔍 [INGEST] Checking Facebook Ads ad insights table {raw_table_ad} existence...")
                    logging.info(f"🔍 [INGEST] Checking Facebook Ads ad insights table {raw_table_ad} existence...")
                    google_bigquery_client.get_table(raw_table_ad)
                    ingest_table_existed = True
                except NotFound:
                    ingest_table_existed = False
                except Exception as e:
                    print(f"❌ [INGEST] Failed to check Facebook Ads ad insights table {raw_table_ad} existence due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to check Facebook Ads ad insights table {raw_table_ad} existence due to {e}.")
                if not ingest_table_existed:
                    print(f"⚠️ [INGEST] Facebook Ads ad insights table {raw_table_ad} not found then table creation will be proceeding...")
                    logging.info(f"⚠️ [INGEST] Facebook Ads ad insights table {raw_table_ad} not found then table creation will be proceeding...")
                    for col, dtype in ingest_df_deduplicated.dtypes.items():
                        if dtype.name.startswith("int"):
                            bq_type = "INT64"
                        elif dtype.name.startswith("float"):
                            bq_type = "FLOAT64"
                        elif dtype.name == "bool":
                            bq_type = "BOOL"
                        elif "datetime" in dtype.name:
                            bq_type = "TIMESTAMP"
                        else:
                            bq_type = "STRING"
                        table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                    table_configuration_defined = bigquery.Table(raw_table_ad, schema=table_schemas_defined)
                    table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                    if table_partition_effective:
                        table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_effective
                        )                    
                    table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns] if table_clusters_defined else []
                    if table_clusters_filtered:
                        table_configuration_defined.clustering_fields = table_clusters_filtered
                    try:
                        print(f"🔍 [INGEST] Creating Facebook Ads ad insights table {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        logging.info(f"🔍 [INGEST] Creating Facebook Ads ad insights table {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        ingest_table_create = google_bigquery_client.create_table(table_configuration_defined)
                        ingest_table_id = ingest_table_create.full_table_id
                        print(f"✅ [INGEST] Successfully created Facebook Ads ad insights table {ingest_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                        logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad insights table {ingest_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create Facebook Ads ad insights table {raw_table_ad} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create Facebook Ads ad insights table {raw_table_ad} due to {e}.")
                else:
                    ingest_dates_new = ingest_df_deduplicated["date_start"].dropna().unique().tolist()
                    query_select_config = f"SELECT DISTINCT date_start FROM `{raw_table_ad}`"
                    query_select_load = google_bigquery_client.query(query_select_config)
                    query_select_result = query_select_load.result()
                    ingest_dates_existed = [row.date_start for row in query_select_result]
                    ingest_dates_overlapped = set(ingest_dates_new) & set(ingest_dates_existed)
                    if ingest_dates_overlapped:
                        print(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads ad insights {raw_table_ad} table then deletion will be proceeding...")
                        logging.warning(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads ad insights {raw_table_ad} table then deletion will be proceeding...")
                        for ingest_date_overlapped in ingest_dates_overlapped:
                            query_delete_config = f"""
                                DELETE FROM `{raw_table_ad}`
                                WHERE date_start = @date_value
                            """
                            job_query_config = bigquery.QueryJobConfig(
                                query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", ingest_date_overlapped)]
                            )
                            try:
                                query_delete_load = google_bigquery_client.query(query_delete_config, job_config=job_query_config)
                                query_delete_result = query_delete_load.result()
                                ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                                print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad}.")
                                logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad}.")
                            except Exception as e:
                                print(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad} due to {e}.")
                                logging.error(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad} due to {e}.")
                    else:
                        print(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads ad insights found in Google BigQuery {raw_table_ad} table then deletion is skipped.")
                        logging.info(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads ad insights found in Google BigQuery {raw_table_ad} table then deletion is skipped.")
                ingest_sections_status[ingest_section_name] = "succeed"
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad insights due to {e}.")
                logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad insights due to {e}.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)

    # 2.2.7. Upload Facebook Ads ad insights to Google BigQuery
            ingest_section_name = "[INGEST] Upload Facebook Ads ad insights to Google BigQuery"
            ingest_section_start = time.time()
            try:
                print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated deduplicated row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}...")
                logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated deduplicated row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}...")
                job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job_load_load = google_bigquery_client.load_table_from_dataframe(
                    ingest_df_deduplicated,
                    raw_table_ad,
                    job_config=job_load_config
                )
                job_load_result = job_load_load.result()
                ingest_rows_uploaded = job_load_load.output_rows
                ingest_dates_uploaded.append(ingest_df_deduplicated.copy())
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}.")
                logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}.")
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad} due to {e}.")
                logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} deduplicated row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad} due to {e}.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2) 

    # 2.2.8. Cooldown before next Facebook Ads ad insights fetch
            ingest_section_name = "[INGEST] Cooldown before next Facebook Ads ad insights fetch"
            ingest_section_start = time.time()
            try:
                if ingest_date_indexed < len(ingest_date_list) - 1:
                    ingest_cooldown_queued = ingest_results_fetched["fetch_summary_final"].get("fetch_cooldown_queued", 60)
                    print(f"🔁 [INGEST] Waiting {ingest_cooldown_queued}s cooldown before triggering to fetch next day of Facebook Ads ad insights...")
                    logging.info(f"🔁 [INGEST] Waiting {ingest_cooldown_queued}s cooldown before triggering to fetch next day of Facebook Ads ad insights...")
                    time.sleep(ingest_cooldown_queued)
                ingest_sections_status[ingest_section_name] = "succeed"
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to set cooldown for {ingest_cooldown_queued}s before triggering to fetch next day of Facebook Ads ad insights due to {e}")
                logging.error(f"❌ [INGEST] Failed to set cooldown for {ingest_cooldown_queued}s before triggering to fetch next day of Facebook Ads ad insights due to {e}")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)

    # 2.2.9. Summarize ingestion results for Facebook Ads ad insights
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = pd.concat(ingest_dates_uploaded or [], ignore_index=True)
        ingest_sections_total = len(ingest_sections_status)
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"]
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_dates_input = len(ingest_date_list)
        ingest_dates_output = len(ingest_dates_uploaded)
        ingest_dates_failed = ingest_dates_input - ingest_dates_output
        ingest_rows_output = ingest_rows_uploaded
        ingest_section_all = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys()) +
            list(ingest_loops_time.keys())
            ))
        ingest_sections_detail = {}
        for ingest_section_separated in ingest_section_all:
            ingest_section_time = (
                ingest_loops_time.get(ingest_section_separated)
                if ingest_section_separated in ingest_loops_time
                else ingest_sections_time.get(ingest_section_separated)
                )
            ingest_sections_detail[ingest_section_separated] = {
                "status": ingest_sections_status.get(ingest_section_separated, "unknown"),
                "time": round(ingest_section_time or 0.0, 2),
                "type": "loop" if ingest_section_separated in ingest_loops_time else "single"
                }
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads ad insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads ad insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_dates_output == ingest_dates_input:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads ad insights ingestion from from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad insights ingestion from from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"            
        else:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads ad insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads ad insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded,
                "ingest_sections_failed": ingest_sections_failed,
                "ingest_sections_detail": ingest_sections_detail,
                "ingest_dates_input": ingest_dates_input,
                "ingest_dates_output": ingest_dates_output,
                "ingest_dates_failed": ingest_dates_failed,
                "ingest_rows_output": ingest_rows_output,
                }
            }
    return ingest_results_final