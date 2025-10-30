"""
==================================================================
FACEBOOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Facebook Ads fetching module 
into Google BigQuery, establishing the foundational raw layer used 
for centralized storage and historical retention.

It manages the complete ingestion flow — from authentication and 
data fetching, to enrichment, schema validation, and loading into 
BigQuery tables segmented by campaign, ad, creative and metadata.

✔️ Supports both append and truncate modes via `write_disposition`  
✔️ Validates data structure using centralized schema utilities  
✔️ Integrates enrichment routines before loading into BigQuery  
✔️ Implements granular logging and CSV-based error traceability  
✔️ Ensures pipeline reliability through retry and checkpoint logic  

⚠️ This module is dedicated solely to *raw-layer ingestion*.  
It does **not** handle advanced transformations, metric modeling, 
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

# Add Python Pandas libraries for integration
import pandas as pd


# Add Python time ultilities for integration
import time

# Add Python UUID ultilities for integration
import uuid

# Add Google API core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Facebook modules for handling
from src.enrich import (
    enrich_campaign_insights, 
    enrich_ad_insights   
)
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

# 1.1. Ingest Facebook Ads campaign metadata to Google BigQuery
def ingest_campaign_metadata(campaign_id_list: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 1.1.1. Start timing the Facebook Ads campaign metadata ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    ingest_section_name = "[INGEST] Start timing the Facebook Ads campaign metadata ingestion"  
    ingest_sections_time[ingest_section_name] = round(time.time() - ingest_time_start, 2)
    ingest_sections_status[ingest_section_name] = "succeed"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for the Facebook Ads campaign metadata ingestion
    ingest_section_name = "[INGEST] Validate input for the Facebook Ads campaign metadata ingestion"
    ingest_section_start = time.time()
    try:
        if not campaign_id_list:
            ingest_sections_status["[INGEST] Validate input for the Facebook Ads campaign metadata ingestion"] = "failed"
            print("⚠️ [INGEST] Empty Facebook Ads campaign_id_list provided then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty Facebook Ads campaign_id_list provided then ingestion is suspended.")
            raise ValueError("⚠️ [INGEST] Empty Facebook Ads campaign_id_list provided then ingestion is suspended.")
        else:
            ingest_sections_status["[INGEST] Validate input for the Facebook Ads campaign metadata ingestion"] = "succeed"
            print(f"✅ [INGEST] Successfully validated input for {len(campaign_id_list)} campaign_id(s) of Facebook Ads campaign metadata ingestion.")
            logging.info(f"✅ [INGEST] Successfully validated input for {len(campaign_id_list)} campaign_id(s) of Facebook Ads campaign metadata ingestion.")
    finally:
        ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    try:

    # 1.1.3. Trigger to fetch Facebook Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to fetch Facebook Ads campaign metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
            ingest_results_fetched = fetch_campaign_metadata(campaign_id_list=campaign_id_list)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads campaign metadata"] = "succeed"
            elif ingest_status_fetched == "fetch_succeed_partial":
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign metadata fetching {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign metadata fetching {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads campaign metadata"] = "partial"
            else:
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads campaign metadata"] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.4. Prepare table_id for Facebook Ads campaign metadata ingestion
        ingest_section_name = "[INGEST] Prepare table_id for Facebook Ads campaign metadata ingestion"
        ingest_section_start = time.time()
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
            ingest_sections_status["[INGEST] Prepare table_id for Facebook Ads campaign metadata ingestion"] = "succeed"   
            print(f"🔍 [INGEST] Preparing to ingest Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) with Google BigQuery table_id {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) with Google BigQuery table_id {raw_table_campaign}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.5. Trigger to enforce schema for Facebook Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to enforce schema for Facebook Ads campaign metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads campaign metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads campaign metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_campaign_metadata")
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]    
            if ingest_status_enforced == "schema_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads campaign metadata"] = "succeed"
            else:
                ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads campaign metadata"] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.6. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.7. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["account_id", "campaign_id"]
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads campaign metadata table {raw_table_campaign} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads campaign metadata table {raw_table_campaign} existence...")
                google_bigquery_client.get_table(raw_table_campaign)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check Facebook Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                raise RuntimeError(f"❌ [INGEST] Failed to check Facebook Ads campaign metadata table {raw_table_campaign} existence due to {e}.") from e
            if not ingest_table_existed:
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
                try:    
                    print(f"🔍 [INGEST] Creating Facebook Ads campaign metadata table defined name {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads campaign metadata table defined name {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                    print(f"✅ [INGEST] Successfully created Facebook Ads campaign metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads campaign metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads campaign metadata table {raw_table_campaign} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads campaign metadata table {raw_table_campaign} due to {e}.")
                    raise RuntimeError(f"❌ [INGEST] Failed to create Facebook Ads campaign metadata table {raw_table_campaign} due to {e}.") from e
            else:
                print(f"🔄 [INGEST] Found Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion will be proceeding...")
                unique_keys = ingest_df_deduplicated[["campaign_id", "account_id"]].dropna().drop_duplicates()
                if not unique_keys.empty:
                    temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                    join_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["campaign_id", "account_id"]
                    ])
                    delete_query = f"""
                        DELETE FROM `{raw_table_campaign}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads campaign metadata table {raw_table_campaign}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads campaign metadata table {raw_table_campaign}.")
                else:
                    print(f"⚠️ [INGEST] No unique campaign_id and account_id keys found in Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique campaign_id and account_id keys found in Facebook Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion is skipped.")
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign metadata due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.8. Upload Facebook Ads campaign metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload Facebook Ads campaign metadata to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_campaign, job_config=job_config).result()
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            ingest_sections_status["[INGEST] Upload Facebook Ads campaign metadata to Google BigQuery"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGESTS] Upload Facebook Ads campaign metadata to Google BigQuery"] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebok Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads campaign metadata due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.9. Summarize ingestion result(s) for Facebook Ads campaign metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(campaign_id_list)
        ingest_rows_output = len(ingest_df_final)
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_rows_output < ingest_rows_input:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_sections_detail = {
            section: {
                "status": ingest_sections_status.get(section, "unknown"),
                "time": ingest_sections_time.get(section, None),
            }
            for section in set(ingest_sections_status) | set(ingest_sections_time)
        }
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
def ingest_adset_metadata(adset_id_list: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")

    # 1.2.1. Start timing the Facebook Ads adset metadata ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_status["[INGEST] Start timing the Facebook Ads adset metadata ingestion"] = "succeed"    
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads adset metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads adset metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Validate input for Facebook Ads adset metadata ingestion
    if not adset_id_list:
        ingest_sections_status["[INGEST] Validate input for Facebook Ads adset metadata ingestion"] = "failed"
        print("⚠️ [INGEST] Empty Facebook Ads adset_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads adset_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads adset_id_list provided then ingestion is suspended.")
    else:
        ingest_sections_status["[INGEST] Validate input for Facebook Ads adset metadata ingestion"] = "succeed"
        print(f"✅ [INGEST] Successfully validated input for {len(adset_id_list)} adset_id(s) of Facebook Ads adset metadata ingestion.")
        logging.info(f"✅ [INGEST] Successfully validated input for {len(adset_id_list)} adset_id(s) of Facebook Ads adset metadata ingestion.")
    
    try:

    # 1.2.3. Trigger to fetch Facebook Ads adset metadata
        print(f"🔁 [INGEST] Triggering to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
        ingest_results_fetched = fetch_adset_metadata(adset_id_list=adset_id_list)
        ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
        ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
        ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
        if ingest_status_fetched == "fetch_succeed_all":
            print(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads adset metadata"] = "succeed"
        elif ingest_status_fetched == "fetch_succeed_partial":
            print(f"⚠️ [INGEST] Partially triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads adset metadata"] = "partial"
        else:
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads adset metadata"] = "failed"
            print(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            raise RuntimeError(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")

    # 1.2.4. Prepare table_id for Facebook Ads adset metadata ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_table_adset = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
        ingest_sections_status["[INGEST] Prepare table_id for Facebook Ads adset metadata ingestion"] = "succeed"
        print(f"🔍 [INGEST] Preparing to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) with Google BigQuery table_id {raw_table_adset}...")
        logging.info(f"🔍 [INGEST] Preparing to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) with Google BigQuery table_id {raw_table_adset}...")

    # 1.2.5. Trigger to enforce schema for Facebook Ads adset metadata
        print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads adset metadata with {len(ingest_df_fetched)} row(s)...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads adset metadata with {len(ingest_df_fetched)} row(s)...")
        ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_adset_metadata")
        ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
        ingest_status_enforced = ingest_results_enforced["schema_status_final"]
        ingest_df_enforced = ingest_results_enforced["schema_df_final"]   
        if ingest_status_enforced == "schemas_succeed_all":
            print(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads adset metadata"] = "succeed"
        else:
            ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads adset metadata"] = "failed"
            print(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['schema_time_elapsed']}s.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['schema_time_elapsed']}s.")
            raise RuntimeError(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['schema_time_elapsed']}s.")

    # 1.2.6. Initialize Google BigQuery client
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGESTS] Initialize Google BigQuery client"] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.2.7. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()    
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads adset metadata table {raw_table_adset} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads adset metadata table {raw_table_adset} existence...")
                google_bigquery_client.get_table(raw_table_adset)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check Facebook Ads adset metadata table {raw_table_adset} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads adset metadata table {raw_table_adset} existence due to {e}.")
                raise RuntimeError(f"❌ [INGEST] Failed to check Facebook Ads adset metadata table {raw_table_adset} existence due to {e}.") from e
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] Facebook Ads adset metadata table {raw_table_adset} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads adset metadata table {raw_table_adset} not found then table creation will be proceeding..")
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
                table_partition_effective  = "date" if "date" in ingest_df_deduplicated.columns else None
                if table_partition_effective :
                    table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=table_partition_effective 
                    )
                table_clusters_defined = ["adset_id", "account_id"]
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:
                    table_configuration_defined.clustering_fields = table_clusters_filtered
                try:    
                    print(f"🔍 [INGEST] Creating Facebook Ads adset metadata table defined name {raw_table_adset} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads adset metadata table defined name {raw_table_adset} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                    print(f"✅ [INGEST] Successfully created Facebook Ads adset metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads adset metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads adset metadata table {raw_table_adset} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads adset metadata table {raw_table_adset} due to {e}.")
                    raise RuntimeError(f"❌ [INGEST] Failed to create Facebook Ads adset metadata table {raw_table_adset} due to {e}..") from e
            else:
                print(f"🔄 [INGEST] Found Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion will be proceeding...")
                unique_keys = ingest_df_deduplicated[["adset_id", "account_id"]].dropna().drop_duplicates()
                if not unique_keys.empty:
                    temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_adset_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                    join_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["adset_id", "account_id"]
                    ])
                    delete_query = f"""
                        DELETE FROM `{raw_table_adset}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads adset metadata {raw_table_adset}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads adset metadata {raw_table_adset}.")
                else:
                    print(f"⚠️ [INGEST] No unique adset_id and account_id keys found in Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique adset_id and account_id keys found in Facebook Ads adset metadata table {raw_table_adset} then existing row(s) deletion is skipped.")
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_adset} if it not exist for Facebook Ads adset metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_adset} if it not exist for Facebook Ads adset metadata due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_adset} if it not exist for Facebook Ads adset metadata due to {e}.") from e

    # 1.2.8. Upload Facebook Ads adset metadata to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_adset, job_config=job_config).result()
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads adset metadata to Google BigQuery table {raw_table_adset}.")
            ingest_sections_status["[INGEST] Upload Facebook Ads adset metadata to Google BigQuery"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Upload Facebook Ads adset metadata to Google BigQuery"] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebok Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads campaign metadata due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to upload Facebook Ads campaign metadata due to {e}.")

    # 1.2.9. Summarize ingestion result(s) for Facebook Ads adset metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(adset_id_list)
        ingest_rows_output = len(ingest_df_final)
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_rows_output < ingest_rows_input:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads adset metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_status, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 1.3. Ingest Facebook Ads ad metadata to Google BigQuery
def ingest_ad_metadata(ad_id_list: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")

    # 1.3.1. Start timing the Facebook Ads ad metadata ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_status["[INGEST] Start timing the Facebook Ads ad metadata ingestion"] = "succeed"    
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.3.2. Validate input for Facebook Ads ad metadata ingestion
    if not ad_id_list:
        ingest_sections_status["[INGEST] Validate input for Facebook Ads ad metadata ingestion"] = "failed"
        print("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
    else:
        ingest_sections_status["[INGEST] Validate input for Facebook Ads ad metadata ingestion"] = "succeed"
        print(f"✅ [INGEST] Successfully validated input for {len(ad_id_list)} ad_id(s) of Facebook Ads ad metadata ingestion.")
        logging.info(f"✅ [INGEST] Successfully validated input for {len(ad_id_list)} ad_id(s) of Facebook Ads ad metadata ingestion.")
    
    try:

    # 1.3.3. Trigger to fetch Facebook Ads ad metadata
        print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        ingest_results_fetched = fetch_ad_metadata(ad_id_list=ad_id_list)
        ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
        ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
        ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
        if ingest_status_fetched == "fetch_succeed_all":
            print(f"✅ [INGEST] Successfully triggered Facebook Ads ad metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad metadata"] = "succeed"
        elif ingest_status_fetched == "fetch_succeed_partial":
            print(f"⚠️ [INGEST] Partially triggered Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad metadata"] = "partial"
        else:
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad metadata"] = "failed"
            print(f"❌ [INGEST] Failed to trigger Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            raise RuntimeError(f"❌ [INGEST] Failed to trigger Facebook Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")

    # 1.3.4. Prepare table_id for Facebook Ads ad metadata ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_table_ad = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        ingest_sections_status["[INGEST] Prepare table_id for Facebook Ads ad metadata ingestion"] = "partial"
        print(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {raw_table_ad}...")
        logging.info(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {raw_table_ad}...")

    # 1.3.5. Trigger to enforce schema for Facebook Ads ad metadata
        print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad metadata with {len(ingest_df_fetched)} row(s)...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad metadata with {len(ingest_df_fetched)} row(s)...")
        ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_ad_metadata")
        ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
        ingest_status_enforced = ingest_results_enforced["schema_status_final"]
        ingest_df_enforced = ingest_results_enforced["schema_df_final"]   
        if ingest_status_enforced == "schema_succeed_all":
            print(f"✅ [INGEST] Successfully triggered schema enforcement for Facebook Ads ad metadata with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            logging.info(f"✅ [INGEST] Successfully triggered schema enforcement for Facebook Ads ad metadata with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads ad metadata"] = "succeed"
        else:
            ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads ad metadata"] = "failed"
            print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad metadata with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])}.")
            logging.error(f"❌ [INGEST] Failed to retrieve schema enforcement for Facebook Ads ad metadata with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])}.")
            raise RuntimeError(f"❌ [INGEST] Failed to retrieve schema enforcement for Facebook Ads ad metadata with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])}.")

    # 1.3.6. Initialize Google BigQuery client
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.3.7. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
            table_clusters_filtered = []
            table_schemas_defined = []    
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads ad metadata table {raw_table_ad} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads ad metadata table {raw_table_ad} existence...")
                google_bigquery_client.get_table(raw_table_ad)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check Facebook Ads ad metadata table {raw_table_ad} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads ad metadata table {raw_table_ad} existence due to {e}.")
                raise RuntimeError(f"❌ [INGEST] Failed to check Facebook Ads ad metadata table {raw_table_ad} existence due to {e}.") from e
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
                table_partition_effective   = "date" if "date" in ingest_df_deduplicated.columns else None
                if table_partition_effective:
                    table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=table_partition_effective
                    )
                table_clusters_defined = ["ad_id", "account_id"]
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:
                    table_configuration_defined.clustering_fields = table_clusters_filtered
                try:    
                    print(f"🔍 [INGEST] Creating Facebook Ads ad metadata table defined name {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads ad metadata table defined name {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                    print(f"✅ [INGEST] Successfully created Facebook Ads ad metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads ad metadata table {raw_table_ad} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads ad metadata table {raw_table_ad} due to {e}.")
                    raise RuntimeError(f"❌ [INGEST] Failed to create Facebook Ads ad metadata table {raw_table_ad} due to {e}..") from e
            else:
                print(f"🔄 [INGEST] Found Facebook Ads ad metadata table {raw_table_ad} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads ad metadata table {raw_table_ad} then existing row(s) deletion will be proceeding...")
                unique_keys = ingest_df_deduplicated[["ad_id", "account_id"]].dropna().drop_duplicates()
                if not unique_keys.empty:
                    temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                    join_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["ad_id", "account_id"]
                    ])
                    delete_query = f"""
                        DELETE FROM `{raw_table_ad}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad metadata table {raw_table_ad}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad metadata table {raw_table_ad}.")
                else:
                    print(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook ad metadata table {raw_table_ad} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook ad metadata table {raw_table_ad} then existing row(s) deletion is skipped.")
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad metadata due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_ad} if it not exist for Facebook Ads ad metadata due to {e}.") from e

    # 1.3.8. Upload Facebook Ads ad metadata to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_ad, job_config=job_config).result()
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads ad metadata to Google BigQuery table {raw_table_ad}.")
            ingest_sections_status["[INGEST] Upload Facebook Ads ad metadata to Google BigQuery"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Upload Facebook Ads ad metadata to Google BigQuery"] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebok Ads ad metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads ad metadata due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to upload Facebook Ads ad metadata due to {e}.")

    # 1.3.9. Summarize ingestion result(s) for Facebook Ads ad metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ad_id_list)
        ingest_rows_output = len(ingest_df_final)
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_rows_output < ingest_rows_input:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_status, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 1.4. Ingest Facebook Ads ad creative to Google BigQuery
def ingest_ad_creative(ad_id_list: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")

    # 1.4.1. Start timing the Facebook Ads ad creative ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_status["[INGEST] Start timing the Facebook Ads ad creative ingestion"] = "succeed"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad creative at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad creative at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.4.2. Validate input for Facebook Ads ad creative ingestion
    if not ad_id_list:
        ingest_sections_status["[INGEST] Validate input for Facebook Ads ad creative ingestion"] = "failed"
        print("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
    else:
        ingest_sections_status["1.4.2. Validate input for Facebook Ads ad creative ingestion"] = "succeed"
        print(f"✅ [INGEST] Successfully validated input for {len(ad_id_list)} ad_id(s) of Facebook Ads ad creative ingestion.")
        logging.info(f"✅ [INGEST] Successfully validated input for {len(ad_id_list)} ad_id(s) of Facebook Ads ad creative ingestion.")

    try:
  
    # 1.4.3. Trigger to fetch Facebook Ads ad creative
        print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
        ingest_results_fetched = fetch_ad_creative(ad_id_list=ad_id_list)
        ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
        ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
        ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
        if ingest_status_fetched == "fetch_succeed_all":
            print(f"✅ [INGEST] Successfully triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad creative"] = "succeed"
        elif ingest_status_fetched == "fetch_succeed_partial":
            print(f"⚠️ [INGEST] Partially triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad creative"] = "partial"
        else:
            ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad creative"] = "failed"
            print(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
            raise RuntimeError(f"❌ [INGEST] Failed to fetch Facebook Ads ad creative due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")

    # 1.4.4. Prepare table_id for Facebook Ads ad creative ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_table_creative = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_creative"
        ingest_sections_status["[INGEST] Prepare table_id for Facebook Ads ad creative ingestion"] = "succeed"
        print(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {raw_table_creative}...")
        logging.info(f"🔍 [INGEST] Preparing to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {raw_table_creative}...")

    # 1.4.5. Trigger to enforce schema for Facebook Ads ad creative
        print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad creative with {len(ingest_df_fetched)} row(s)...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad creative with {len(ingest_df_fetched)} row(s)...")
        ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_ad_creative")
        ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
        ingest_status_enforced = ingest_results_enforced["schema_status_final"]
        ingest_df_enforced = ingest_results_enforced["schema_df_final"]   
        if ingest_status_enforced == "schema_succeed_all":
            print(f"✅ [INGEST] Successfully triggered schema enforcement for Facebook Ads ad creative with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            logging.info(f"✅ [INGEST] Successfully triggered schema enforcement for Facebook Ads ad creative with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads ad creative"] = "succeed"
        else:
            ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads ad creative"] = "failed"
            print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad creative with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])}.")
            logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad creative with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])}.")
            raise RuntimeError(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad creative with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed sections "f"{', '.join(ingest_summary_enforced['schema_sections_failed'])}.")

    # 1.4.6. Initialize Google BigQuery client
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.4.7. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
            table_clusters_filtered = []
            table_schemas_defined = []    
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads ad creative table {raw_table_creative} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads ad creative table {raw_table_creative} existence...")
                google_bigquery_client.get_table(raw_table_creative)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check Facebook Ads ad creative table {raw_table_creative} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Facebook Ads ad creative table {raw_table_creative} existence due to {e}.")
                raise RuntimeError(f"❌ [INGEST] Failed to check Facebook Ads ad creative table {raw_table_creative} existence due to {e}.") from e
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
                table_clusters_defined = ["ad_id", "account_id"]
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:
                    table_configuration_defined.clustering_fields = table_clusters_filtered
                try:    
                    print(f"🔍 [INGEST] Creating Facebook Ads ad creative table defined name {raw_table_creative} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads ad creative table defined name {raw_table_creative} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                    print(f"✅ [INGEST] Successfully created Facebook Ads ad creative table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad creative table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Facebook Ads ad creative table {raw_table_creative} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Facebook Ads ad creative table {raw_table_creative} due to {e}.")
                    raise RuntimeError(f"❌ [INGEST] Failed to create Facebook Ads ad creative table {raw_table_creative} due to {e}.") from e
            else:
                print(f"🔄 [INGEST] Found Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion will be proceeding...")
                unique_keys = ingest_df_deduplicated[["ad_id", "account_id"]].dropna().drop_duplicates()
                if not unique_keys.empty:
                    temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_creative_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                    join_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["ad_id", "account_id"]
                    ])
                    delete_query = f"""
                        DELETE FROM `{raw_table_creative}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad creative table {raw_table_creative}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad creative table {raw_table_creative}.")
                else:
                    print(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook Ads ad creative table {raw_table_creative} then existing row(s) deletion is skipped.")
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Delete existing row(s) or create new table if it not exist"] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_creative} if it not exist for Facebook Ads ad creative due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_creative} if it not exist for Facebook Ads ad creative due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_creative} if it not exist for Facebook Ads ad creative due to {e}.") from e

    # 1.4.8. Upload Facebook Ads ad creative to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_creative, job_config=job_config).result()
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {raw_table_creative}.")
            ingest_sections_status["[INGEST] Upload Facebook Ads ad creative to Google BigQuery"] = "succeed"
        except Exception as e:
            ingest_sections_status["[INGEST] Upload Facebook Ads ad creative to Google BigQuery"] = "failed"
            print(f"❌ [INGEST] Failed to upload Facebok Ads ad metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads ad creative due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to upload Facebook Ads ad creative due to {e}.")

    # 1.4.9. Summarize ingestion result(s) for Facebook Ads ad creative
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ad_id_list)
        ingest_rows_output = len(ingest_df_final)
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_rows_output < ingest_rows_input:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeeds_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad creative ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_status, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 2. INGEST FACEBOOK ADS INSIGHTS

# 2.1. Ingest Facebook Ads campaign insights to Google BigQuery
def ingest_campaign_insights(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:  
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Start timing the Facebook Ads campaign insights ingestion
    ingest_time_start = time.time()
    ingest_dates_uploaded = []
    ingest_sections_status = {}
    ingest_sections_status["[INGEST] Start timing the Facebook Ads campaign insights ingestion"] = "succeed"    
    print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 2.1.2. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "succeed"
    except Exception as e:
        ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "failed"
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 2.1.3. Split date range into individual days for ingestion
    try:
        ingest_date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
        ingest_sections_status["[INGEST] Split date range into individual days for ingestion"] = "succeed"
        for ingest_date_separated in ingest_date_list:

    # 2.1.4. Trigger to fetch Facebook Ads campaign insights 
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaigns insights for {ingest_date_separated}...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaigns insights for {ingest_date_separated}...")
            ingest_results_fetched = fetch_campaign_insights(ingest_date_separated, ingest_date_separated)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads campaign insights"] = "succeed"
            elif ingest_status_fetched == "fetch_succeed_partial":
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads campaign insights"] = "partial"
            else:
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads campaign insights"] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")

    # 2.1.5. Trigger to enrich Facebook Ads campaign insights
            print(f"🔁 [INGEST] Trigger to enrich Facebook Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔁 [INGEST] Trigger to enrich Facebook Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enriched = enrich_campaign_insights(enrich_df_input=ingest_df_fetched)
            ingest_df_enriched = ingest_results_enriched["enrich_df_final"]
            ingest_status_enriched = ingest_results_enriched["enrich_status_final"]
            ingest_summary_enriched = ingest_results_enriched["enrich_summary_final"]
            if ingest_status_enriched == "enrich_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights enrichment for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights enrichment for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Enrich Facebook Ads campaign insights"] = "succeed"
            elif ingest_status_enriched == "enrich_failed_all":
                ingest_sections_status["[INGEST] Enrich Facebook Ads campaign insights"] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights enrichment for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) due to section(s) {', '.join(ingest_summary_enriched["enrich_sections_failed"]) if ingest_summary_enriched["enrich_sections_failed"] else 'unknown error'} in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights enrichment for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) due to section(s) {', '.join(ingest_summary_enriched["enrich_sections_failed"]) if ingest_summary_enriched["enrich_sections_failed"] else 'unknown error'} in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                
    # 2.1.6. Trigger to enforce schema for Facebook Ads campaign insights
            print(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads campaign insights for {ingest_date_separated} with {len(ingest_df_enriched)} row(s)...")
            logging.info(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads campaign insights for {ingest_date_separated} with {len(ingest_df_enriched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(schema_df_input=ingest_df_enriched,schema_type_mapping="ingest_campaign_insights")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            if ingest_status_enforced == "schema_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads campaign insights"] = "succeed"
            else:
                ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads campaign insights"] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed']) if ingest_summary_enforced['schema_sections_failed'] else 'unknown error'} in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed']) if ingest_summary_enforced['schema_sections_failed'] else 'unknown error'} in {ingest_summary_enforced['schema_time_elapsed']}s.")
        
    # 2.1.7. Prepare table_id for Facebook Ads campaign insights ingestion
            first_date = pd.to_datetime(ingest_df_fetched["date_start"].dropna().iloc[0])
            y, m = first_date.year, first_date.month
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
            ingest_sections_status["[INGEST] Prepare table_id for Facebook Ads campaign insights ingestion"] = "succeed"
            print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads campaign insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_campaign}...")

    # 2.1.8. Delete existing row(s) or create new table if not exist
            try:
                ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
                table_clusters_defined = None
                table_clusters_filtered = []
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
                    table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                    if table_clusters_filtered:
                        table_configuration_defined.clustering_fields = table_clusters_filtered
                    try:
                        print(f"🔍 [INGEST] Creating Facebook Ads campaign insights table {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        logging.info(f"🔍 [INGEST] Creating Facebook Ads campaign insights table {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                        print(f"✅ [INGEST] Successfully created Facebook Ads campaign insights table {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                        logging.info(f"✅ [INGEST] Successfully created Facebook Ads campaign insights table {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create Facebook Ads campaign insights table {raw_table_campaign} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create Facebook Ads campaign insights table {raw_table_campaign} due to {e}.")
                else:
                    ingest_dates_new = ingest_df_deduplicated["date_start"].dropna().unique().tolist()
                    ingest_query_existed = f"SELECT DISTINCT date_start FROM `{raw_table_campaign}`"
                    ingest_dates_existed = [row.date_start for row in google_bigquery_client.query(ingest_query_existed).result()]
                    ingest_dates_overlapped = set(ingest_dates_new) & set(ingest_dates_existed)
                    if ingest_dates_overlapped:
                        print(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads campaign insights {raw_table_campaign} table then deletion will be proceeding...")
                        logging.warning(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads campaign insights {raw_table_campaign} table then deletion will be proceeding...")
                        for ingest_date_overlapped in ingest_dates_overlapped:
                            query = f"""
                                DELETE FROM `{raw_table_campaign}`
                                WHERE date_start = @date_value
                            """
                            job_config = bigquery.QueryJobConfig(
                                query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", ingest_date_overlapped)]
                            )
                            try:
                                ingest_result_deleted = google_bigquery_client.query(query, job_config=job_config).result()
                                ingest_rows_deleted = ingest_result_deleted.num_dml_affected_rows
                                print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}.")
                                logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}.")
                            except Exception as e:
                                print(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign} due to {e}.")
                                logging.error(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign} due to {e}.")
                    else:
                        print(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads campaign insights found in Google BigQuery {raw_table_campaign} table then deletion is skipped.")
                        logging.info(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads campaign insights found in Google BigQuery {raw_table_campaign} table then deletion is skipped.")
                ingest_sections_status["[INGEST] Delete existing row(s) or create new table if not exist"] = "succeed"
            except Exception as e:
                ingest_sections_status["[INGEST] Delete existing row(s) or create new table if not exist"] = "failed"
                print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign insights due to {e}.")
                logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for Facebook Ads campaign insights due to {e}.")

    # 2.1.9. Upload Facebook Ads campaign insights to Google BigQuery
            try:
                print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}...")
                logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}...")
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                load_job = google_bigquery_client.load_table_from_dataframe(
                    ingest_df_deduplicated,
                    raw_table_campaign,
                    job_config=job_config
                )
                load_job.result()
                ingest_dates_uploaded.append(ingest_df_deduplicated.copy())
                ingest_sections_status["[INGEST] Upload Facebook Ads campaign insights to Google BigQuery"] = "succeed"
                print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}.")
                logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign}.")
            except Exception as e:
                ingest_sections_status["[INGEST] Upload Facebook Ads campaign insights to Google BigQuery"] = "failed"
                print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign} due to {e}.")
                logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign insights to Google BigQuery table {raw_table_campaign} due to {e}.")

    # 2.1.10. Summarize ingestion result(s) for Facebook Ads campaign insights
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = pd.concat(ingest_dates_uploaded or [], ignore_index=True)
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_dates_input = len(ingest_date_list)
        ingest_dates_output = len(ingest_dates_uploaded)
        ingest_dates_failed = ingest_dates_input - ingest_dates_output
        ingest_rows_output = len(ingest_df_final)
        if len(ingest_dates_uploaded) == 0:
            print(f"❌ [INGEST] Failed to complete Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_dates_failed > 0:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_status, 
                "ingest_dates_input": ingest_dates_input,
                "ingest_dates_output": ingest_dates_output,
                "ingest_dates_failed": ingest_dates_failed,                
                "ingest_rows_uploaded": ingest_rows_output,
            }
        }
    return ingest_results_final

# 2.2. Ingest Facebook Ad ad insight to Google BigQuery raw tables
def ingest_ad_insights(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:   
    print(f"🚀 [INGEST] Starting to ingest Facebook Ads ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook Ads ad insights from {start_date} to {end_date}...")

    # 2.2.1. Start timing the Facebook Ads ad insights ingestion
    ingest_time_start = time.time()
    ingest_dates_uploaded = []
    ingest_sections_status = {}
    ingest_sections_status["[INGEST] Start timing the Facebook Ads ad insights ingestion"] = "succeed"    
    print(f"🔍 [FETCH] Proceeding to ingest Facebook Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to ingest Facebook Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 2.2.2. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "succeed"
    except Exception as e:
        ingest_sections_status["[INGEST] Initialize Google BigQuery client"] = "failed"
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e
    
    # 2.2.3. Split date range into individual days for Facebook Ads ad insights ingestion
    try:
        ingest_date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
        ingest_sections_status["[INGEST] Split date range into individual days for Facebook Ads ad insights ingestion"] = "succeed"
        for ingest_date_separated in ingest_date_list:

    # 2.2.4. Trigger to fetch Facebook Ads ad insights 
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad insights for {ingest_date_separated}...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad insights for {ingest_date_separated}...")
            ingest_results_fetched = fetch_ad_insights(ingest_date_separated, ingest_date_separated)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad insights "] = "succeed"
            elif ingest_status_fetched == "fetch_succeed_partial":
                print(f"⚠️ [INGEST] Partially triggered Facebook Ads ad insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Facebook Ads ad insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad insights "] = "partial"
            else:
                ingest_sections_status["[INGEST] Trigger to fetch Facebook Ads ad insights "] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                raise RuntimeError(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights fetching for for {ingest_date_separated} due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")

    # 2.2.5. Trigger to enrich Facebook Ads ad insights
            print(f"🔁 [INGEST] Trigger to enrich Facebook Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔁 [INGEST] Trigger to enrich Facebook Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enriched = enrich_ad_insights(enrich_df_input=ingest_df_fetched)
            ingest_df_enriched = ingest_results_enriched["enrich_df_final"]
            ingest_status_enriched = ingest_results_enriched["enrich_status_final"]
            ingest_summary_enriched = ingest_results_enriched["enrich_summary_final"]
            if ingest_status_enriched == "enrich_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights enrichment for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights enrichment for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to enrich Facebook Ads ad insights"] = "succeed"
            elif ingest_status_enriched == "enrich_failed_all":
                ingest_sections_status["[INGEST] Trigger to enrich Facebook Ads ad insights"] = "failed"
                print(f"❌ [INGEST] Failed to enrich Facebook Ads ad insights for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) due to section(s) {', '.join(ingest_summary_enriched["enrich_sections_failed"]) if ingest_summary_enriched["enrich_sections_failed"] else 'unknown error'} in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to enrich Facebook Ads ad insights for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) due to section(s) {', '.join(ingest_summary_enriched["enrich_sections_failed"]) if ingest_summary_enriched["enrich_sections_failed"] else 'unknown error'} in {ingest_summary_enriched['enrich_time_elapsed']}s.")
                raise RuntimeError(f"❌ [INGEST] Failed to enrich Facebook Ads ad insights for {ingest_date_separated} with {ingest_summary_enriched['enrich_rows_output']}/{ingest_summary_enriched['enrich_rows_input']} enriched row(s) due to section(s) {', '.join(ingest_summary_enriched["enrich_sections_failed"]) if ingest_summary_enriched["enrich_sections_failed"] else 'unknown error'} in {ingest_summary_enriched['enrich_time_elapsed']}s.")

    # 2.2.6. Trigger to enforce schema for Facebook Ads ad insights
            print(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads ad insights for {ingest_date_separated} with {len(ingest_df_enriched)} row(s)...")
            logging.info(f"🔁 [INGEST] Triggering to enforce schema for Facebook Ads ad insights for {ingest_date_separated} with {len(ingest_df_enriched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(schema_df_input=ingest_df_enriched,schema_type_mapping="ingest_ad_insights")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            if ingest_status_enforced == "schema_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads ad insights"] = "succeed"
            else:
                ingest_sections_status["[INGEST] Trigger to enforce schema for Facebook Ads ad insights"] = "failed"
                print(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed']) if ingest_summary_enforced['schema_sections_failed'] else 'unknown error'} in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed']) if ingest_summary_enforced['schema_sections_failed'] else 'unknown error'} in {ingest_summary_enforced['schema_time_elapsed']}s.")
                raise RuntimeError(f"❌ [INGEST] Failed to trigger Facebook Ads ad insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed']) if ingest_summary_enforced['schema_sections_failed'] else 'unknown error'} in {ingest_summary_enforced['schema_time_elapsed']}s.")

    # 2.2.7. Prepare table_id for Facebook Ads ad insights ingestion
            first_date = pd.to_datetime(ingest_df_fetched["date_start"].dropna().iloc[0])
            y, m = first_date.year, first_date.month
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_ad = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
            ingest_sections_status["[INGEST] Prepare table_id for Facebook Ads ad insights ingestion"] = "succeed"
            print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_ad}...")
            logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads ad insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_ad}...")

    # 2.2.8. Delete existing row(s) or create new table if not exist
            try:
                ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
                table_clusters_defined = None
                table_clusters_filtered = []
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
                    raise RuntimeError(f"❌ [INGEST] Failed to check Facebook Ads campaign insights table {raw_table_ad} existence due to {e}.") from e
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
                    table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                    if table_clusters_filtered:
                        table_configuration_defined.clustering_fields = table_clusters_filtered
                        table_configuration_defined.clustering_fields = table_clusters_filtered
                    try:
                        print(f"🔍 [INGEST] Creating Facebook Ads ad insights table {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        logging.info(f"🔍 [INGEST] Creating Facebook Ads ad insights table {raw_table_ad} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                        table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                        print(f"✅ [INGEST] Successfully created Facebook Ads ad insights table {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                        logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad insights table {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create Facebook Ads ad insights table {raw_table_ad} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create Facebook Ads ad insights table {raw_table_ad} due to {e}.")
                else:
                    ingest_dates_new = ingest_df_deduplicated["date_start"].dropna().unique().tolist()
                    ingest_query_existed = f"SELECT DISTINCT date_start FROM `{raw_table_ad}`"
                    ingest_dates_existed = [row.date_start for row in google_bigquery_client.query(ingest_query_existed).result()]
                    ingest_dates_overlapped = set(ingest_dates_new) & set(ingest_dates_existed)
                    if ingest_dates_overlapped:
                        print(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads ad insights {raw_table_ad} table then deletion will be proceeding...")
                        logging.warning(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Facebook Ads ad insights {raw_table_ad} table then deletion will be proceeding...")
                        for ingest_date_overlapped in ingest_dates_overlapped:
                            query = f"""
                                DELETE FROM `{raw_table_ad}`
                                WHERE date_start = @date_value
                            """
                            job_config = bigquery.QueryJobConfig(
                                query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", ingest_date_overlapped)]
                            )
                            try:
                                ingest_result_deleted = google_bigquery_client.query(query, job_config=job_config).result()
                                ingest_rows_deleted = ingest_result_deleted.num_dml_affected_rows
                                print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad}.")
                                logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad}.")
                            except Exception as e:
                                print(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad} due to {e}.")
                                logging.error(f"❌ [INGEST] Failed to delete existing row(s) of Facebook Ads ad insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_ad} due to {e}.")
                    else:
                        print(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads ad insights found in Google BigQuery {raw_table_ad} table then deletion is skipped.")
                        logging.info(f"⚠️ [INGEST] No overlapping date(s) of Facebook Ads ad insights found in Google BigQuery {raw_table_ad} table then deletion is skipped.")
                ingest_sections_status["[INGEST] Delete existing row(s) or create new table if not exist"] = "succeed"
            except Exception as e:
                ingest_sections_status["[INGEST] Delete existing row(s) or create new table if not exist"] = "failed"
                print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads ad insights ingestion due to {e}.")
                logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads ad insights ingestion due to {e}.")

    # 2.2.9. Upload Facebook Ads ad insights to Google BigQuery
            try:
                print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}...")
                logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}...")
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                    source_format=bigquery.SourceFormat.PARQUET,
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date",
                    ),
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    ingest_df_deduplicated,
                    raw_table_ad,
                    job_config=job_config
                )
                load_job.result()
                ingest_df_uploaded = ingest_df_deduplicated.copy()
                ingest_dates_uploaded.append(ingest_df_deduplicated.copy())
                ingest_sections_status["[INGEST] Upload Facebook Ads ad insights to Google BigQuery"] = "succeed"
                print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}.")
                logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad}.")
            except Exception as e:
                ingest_sections_status["[INGEST] Upload Facebook Ads ad insights to Google BigQuery"] = "failed"
                print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad} due to {e}.")
                logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad insights to Google BigQuery table {raw_table_ad} due to {e}.")

    # 2.1.9. Summarize ingestion result(s) for Facebook Ads ad insights
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = pd.concat(ingest_dates_uploaded or [], ignore_index=True)
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_dates_input = len(ingest_date_list)
        ingest_dates_output = len(ingest_dates_uploaded)
        ingest_dates_failed = ingest_dates_input - ingest_dates_output
        ingest_rows_output = len(ingest_df_final)
        if len(ingest_dates_uploaded) == 0:
            print(f"❌ [INGEST] Failed to complete Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_dates_failed > 0:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "success"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_status, 
                "ingest_dates_input": ingest_dates_input,
                "ingest_dates_output": ingest_dates_output,
                "ingest_dates_failed": ingest_dates_failed,                
                "ingest_rows_uploaded": ingest_rows_output,
            }
        }
    return ingest_results_final