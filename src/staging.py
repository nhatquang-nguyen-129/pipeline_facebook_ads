"""
==================================================================
FACEBOOK STAGING MODULE
------------------------------------------------------------------
This module transforms raw Facebook Ads data into enriched,  
normalized staging tables in BigQuery, acting as the bridge  
between raw API ingestion and final MART-level analytics.

It combines raw ad/campaign/creative data, applies business logic  
(e.g., parsing naming conventions, standardizing fields), and  
prepares clean, query-ready datasets for downstream consumption.

✔️ Joins raw ad insights with creative and campaign metadata  
✔️ Enriches fields such as owner, placement, format  
✔️ Normalizes and writes standardized tables into dataset  
✔️ Validates data integrity and ensures field completeness  
✔️ Supports modular extension for new Facebook Ads entities  

⚠️ This module is strictly responsible for *data transformation*  
into staging format. It does not handle API ingestion or final  
materialized aggregations.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Authentication modules for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google API Core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Facebook module for handling
from src.schema import enforce_table_schema
from src.enrich import (
    enrich_campaign_fields,
    enrich_ad_fields
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

# 1. TRANSFORM FACEBOOK ADS RAW DATA INTO CLEANED STAGING TABLES

# 1.1. Transform Facebook Ads campaign insights from raw tables into cleaned staging tables
def staging_campaign_insights() -> dict:
    print("🚀 [STAGING] Starting to build staging Facebook Ads campaign insights table...")
    logging.info("🚀 [STAGING] Starting to build staging Facebook Ads campaign insights table...")

    # 1.1.1. Start timing the Facebook Ads campaign insights staging
    staging_time_start = time.time()
    staging_table_queried= []
    staging_sections_status = {}
    staging_sections_status["[STAGING] Start timing the Facebook Ads campaign insights staging"] = "succeed"
    print(f"🔍 [STAGING] Proceeding to transform Facebook Ads campaign insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [STAGING] Proceeding to transform Facebook Ads campaign insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Prepare table_id for Facebook Ads campaign insights staging
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        print(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook Ads campaign insights...")
        logging.info(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook Ads campaign insights...")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_table_campaign} for Facebook Ads campaign insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_table_campaign} for Facebook Ads campaign insights...")
        staging_sections_status["[STAGING] Prepare table_id for Facebook Ads campaign insights staging"] = "succeed"

    # 1.1.3. Initialize Google BigQuery client
        try:
            print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            staging_sections_status["[STAGING] Initialize Google BigQuery client"] = "succeed"
        except Exception as e:
            staging_sections_status["[STAGING] Initialize Google BigQuery client"] = "failed"
            print(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.1.4. Scan all raw Facebook Ads campaign insights table(s)
        try:
            print(f"🔍 [STAGING] Scanning all raw Facebook Ads campaign insights table(s) from Google BigQuery dataset {raw_dataset}...")
            logging.info(f"🔍 [STAGING] Scanning all raw Facebook Ads campaign insights table(s) from Google BigQuery dataset {raw_dataset}...")
            query_campaign_raw = f"""
                SELECT table_name
                FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE REGEXP_CONTAINS(
                    table_name,
                    r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m[0-1][0-9][0-9]{{4}}$'
                )
            """
            raw_tables_campaign = [row.table_name for row in google_bigquery_client.query(query_campaign_raw).result()]
            raw_tables_campaign = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables_campaign]
            if not raw_tables_campaign:
                raise RuntimeError("❌ [STAGING] Failed to scan raw Facebook Ads campaign insights table(s) due to no tables found.")
            print(f"✅ [STAGING] Successfully found {len(raw_tables_campaign)} raw Facebook Ads campaign insights table(s).")
            logging.info(f"✅ [STAGING] Successfully found {len(raw_tables_campaign)} raw Facebook Ads campaign insights table(s).")
            staging_sections_status["[STAGING] Scan all raw Facebook Ads campaign insights table(s)"] = "succeed"
        except Exception as e:
            staging_sections_status["[STAGING] Scan all raw Facebook Ads campaign insights table(s)"] = "failed"
            print(f"❌ [STAGING] Failed to scan raw Facebook Ads campaign insights table(s) due to {e}.")
            logging.error(f"❌ [STAGING] Failed to scan raw Facebook Ads campaign insights table(s) due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to scan raw Facebook Ads campaign insights table(s) due to {e}.") from e
        
    # 1.1.5. Query all raw Facebook Ads campaign insights table(s)
        for raw_table_campaign in raw_tables_campaign:
            query_campaign_staging = f"""
                SELECT
                    raw.*,
                    metadata.campaign_name,
                    metadata.account_name,
                    metadata.effective_status AS delivery_status
                FROM `{raw_table_campaign}` AS raw
                LEFT JOIN `{raw_campaign_metadata}` AS metadata
                    ON CAST(raw.campaign_id AS STRING) = CAST(metadata.campaign_id AS STRING)
                    AND CAST(raw.account_id  AS STRING) = CAST(metadata.account_id  AS STRING)
            """
            try:
                print(f"🔄 [STAGING] Querying raw Facebook Ads campaign insights table {raw_table_campaign}...")
                logging.info(f"🔄 [STAGING] Querying raw Facebook Ads campaign insights table {raw_table_campaign}...")
                staging_df_queried = google_bigquery_client.query(query_campaign_staging).to_dataframe()
                staging_table_queried.append(staging_df_queried)
                print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw Facebook Ads campaign insights from {raw_table_campaign}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw Facebook Ads campaign insights from {raw_table_campaign}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query raw Facebook Ads campaign insights table {raw_table_campaign} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query Facebook Ads campaign insights table {raw_table_campaign} due to {e}.")
                continue
        if not staging_table_queried:
            staging_sections_status["[STAGING] Query all raw Facebook Ads campaign insights table(s)"] = "failed"
            raise RuntimeError("❌ [STAGING] Failed to query TiKTok Ads campaign insights due to no raw table found.")
        staging_df_concatenated = pd.concat(staging_table_queried, ignore_index=True)
        print(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of Facebook Ads campaign insights from {len(staging_table_queried)} table(s).")
        logging.info(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of Facebook Ads campaign insights from {len(staging_table_queried)} table(s).")
        staging_sections_status["[STAGING] Query all raw Facebook Ads campaign insights table(s)"] = "succeed"

    # 1.1.6. Trigger to enrich Facebook Ads campaign insights
        print(f"🔄 [STAGING] Triggering to enrich Facebook Ads campaign insights for {len(staging_df_concatenated)} row(s) from Google BigQuery table {raw_table_campaign}...")
        logging.info(f"🔄 [STAGING] Triggering to enrich Facebook Ads campaign insights for {len(staging_df_concatenated)} row(s) from Google BigQuery table {raw_table_campaign}...")
        staging_results_enriched = enrich_campaign_fields(staging_df_concatenated, enrich_table_id=raw_table_campaign)
        staging_df_enriched = staging_results_enriched["enrich_df_final"]
        staging_status_enriched = staging_results_enriched["enrich_status_final"]
        staging_summary_enriched = staging_results_enriched["enrich_summary_final"]
        if staging_status_enriched == "enrich_success_all":
            print(f"✅ [STAGING] Successfully triggered Facebook Ads campaign insights enrichment with {staging_summary_enriched["enrich_rows_output"]}/{staging_summary_enriched["enrich_rows_input"]} enriched row(s) in {staging_summary_enriched["enrich_time_elapsed"]}s.")
            logging.info(f"✅ [STAGING] Successfully triggered Facebook Ads campaign insights enrichment with {staging_summary_enriched["enrich_rows_output"]}/{staging_summary_enriched["enrich_rows_input"]} enriched row(s) in {staging_summary_enriched["enrich_time_elapsed"]}s.")
            staging_sections_status["[STAGING] Trigger to enrich Facebook Ads campaign insights"] = "succeed"
        else:
            print(f"❌ [STAGING] Failed to trigger Facebook Ads campaign insights enrichment with {staging_summary_enriched["enrich_rows_output"]}/{staging_summary_enriched["enrich_rows_input"]} enriched row(s) due to section(s): {', '.join(staging_summary_enriched.get('enrich_sections_failed', []))} in {staging_summary_enriched["enrich_time_elapsed"]}s.")
            logging.error(f"❌ [STAGING] Failed to trigger Facebook Ads campaign insights enrichment with {staging_summary_enriched["enrich_rows_output"]}/{staging_summary_enriched["enrich_rows_input"]} enriched row(s) due to section(s): {', '.join(staging_summary_enriched.get('enrich_sections_failed', []))} in {staging_summary_enriched["enrich_time_elapsed"]}s.")
            staging_sections_status["[STAGING] Trigger to enrich Facebook Ads campaign insights"] = "failed"


    # 1.1.7. Trigger to enforce schema for Facebook Ads campaign insights
        print(f"🔁 [STAGING] Triggering to enforce schema for Facebook Ads campaign insights for {len(staging_df_enriched)} row(s)...")
        logging.info(f"🔁 [STAGING] Triggering to enforce schema for Facebook Ads campaign insights for {len(staging_df_enriched)} row(s)...")
        staging_results_enforced = enforce_table_schema(schema_df_input=staging_df_enriched,schema_type_mapping="staging_campaign_insights")
        staging_df_enforced = staging_results_enforced["schema_df_final"]
        staging_status_enforced = staging_results_enforced["schema_status_final"]
        staging_summary_enforced = staging_results_enforced["schema_summary_final"]
        if staging_status_enforced == "schema_succeed_all":
            print(f"✅ [STAGING] Successfully triggered Facebook Ads campaign insights schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
            logging.info(f"✅ [STAGING] Successfully triggered Facebook Ads campaign insights schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
            staging_sections_status["[STAGING] Trigger to enforce schema for Facebook Ads campaign insights"] = "succeed"
        else:
            staging_sections_status["[STAGING] Trigger to enforce schema for Facebook Ads campaign insights"] = "failed"
            print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign insights schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")

    # 1.1.8. Create new table for staging Facebook Ads campaign insights if not exist
        staging_df_deduplicated = staging_df_enforced.drop_duplicates()
        table_clusters_filtered = []
        table_schemas_defined = []
        try:
            print(f"🔍 [STAGING] Checking staging Facebook Ads campaign insights table {staging_table_campaign} existence...")
            logging.info(f"🔍 [STAGING] Checking staging Facebook Ads campaign insights table {staging_table_campaign} existence...")
            google_bigquery_client.get_table(staging_table_campaign)
            staging_table_exists = True
        except Exception:
            staging_table_exists = False
        if not staging_table_exists:
            print(f"⚠️ [STAGING] Staging Facebook Ads campaign insights table {staging_table_campaign} not found then new table creation will be proceeding...")
            logging.warning(f"⚠️ [STAGING] Staging Facebook Ads campaign insights table {staging_table_campaign} not found then new table creation will be proceeding...")
            for col, dtype in staging_df_deduplicated.dtypes.items():
                if dtype.name.startswith("int"):
                    google_bigquery_type = "INT64"
                elif dtype.name.startswith("float"):
                    google_bigquery_type = "FLOAT64"
                elif dtype.name == "bool":
                    google_bigquery_type = "BOOL"
                elif "datetime" in dtype.name:
                    google_bigquery_type = "TIMESTAMP"
                else:
                    google_bigquery_type = "STRING"
                table_schemas_defined.append(bigquery.SchemaField(col, google_bigquery_type))
            table_configuration_defined = bigquery.Table(staging_table_campaign, schema=table_schemas_defined)
            table_partition_effective = "date" if "date" in staging_df_deduplicated.columns else None
            if table_partition_effective:
                table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_partition_effective
                )
            table_clusters_defined = ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"]
            table_clusters_filtered = [f for f in table_clusters_defined if f in staging_df_deduplicated.columns]
            if table_clusters_filtered:  
                table_configuration_defined.clustering_fields = table_clusters_filtered  
            try:
                print(f"🔍 [STAGING] Creating staging Facebook Ads campaign insights table with defined name {staging_table_campaign} and partition on {table_partition_effective}...")
                logging.info(f"🔍 [STAGING] Creating staging Facebook Ads campaign insights table with defined name {staging_table_campaign} and partition on {table_partition_effective}...")
                table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                print(f"✅ [STAGING] Successfully created staging Facebook Ads campaign insights table with actual name {table_metadata_defined.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully created staging Facebook Ads campaign insights table with actual name {table_metadata_defined.full_table_id}.")              
            except Exception as e:
                staging_sections_status["[STAGING] Create new table for staging Facebook Ads campaign insights if not exist"] = "failed"
                print(f"❌ [STAGING] Failed to create staging Facebook Ads campaign insights table {staging_table_campaign} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to create staging Facebook Ads campaign insights table {staging_table_campaign} due to {e}.")
                raise RuntimeError(f"❌ [STAGING] Failed to create staging Facebook Ads campaign insights table {staging_table_campaign} due to {e}.") from e
        staging_sections_status["[STAGING] Create new table for staging Facebook Ads campaign insights if not exist"] = "succeed"
    
    # 1.1.9. Upload staging Facebook Ads campaign insights to Google BigQuery
        if not staging_table_exists:
            try: 
                print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to new Google BigQuery table {table_metadata_defined.full_table_id}...")
                logging.warning(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id}...")     
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    ),
                    clustering_fields=table_clusters_filtered if table_clusters_filtered else None
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    staging_df_enforced,
                    staging_table_campaign, 
                    job_config=job_config
                )
                load_job.result()
                staging_df_uploaded = staging_df_enforced.copy()
                print(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) of staging Facebook Ads campaign insights to new Google BigQuery table {table_metadata_defined.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) of staging Facebook Ads campaign insights to new Google BigQuery table {table_metadata_defined.full_table_id}.")
                staging_sections_status["[STAGING] Upload staging Facebook Ads campaign insights to Google BigQuery"] = "succeed"
            except Exception as e:
                staging_sections_status["[STAGING] Upload staging Facebook Ads campaign insights to Google BigQuery"] = "failed"
                print(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.")      
                raise RuntimeError(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.") from e  
        else:
            try:
                print(f"🔍 [STAGING] Found existing Google BigQuery table {staging_table_campaign} and {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights will be overwritten...")
                logging.warning(f"🔍 [STAGING] Found existing Google BigQuery table {staging_table_campaign} and {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights will be overwritten...")                    
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    staging_df_enforced,
                    staging_table_campaign, 
                    job_config=job_config
                )
                load_job.result()
                staging_df_uploaded = staging_df_enforced.copy()
                print(f"✅ [STAGING] Successfully overwrote {len(staging_df_uploaded)} row(s) of staging Facebook Ads campaign insights to existing Google BigQuery table {staging_table_campaign}.")
                logging.info(f"✅ [STAGING] Successfully overwrote {len(staging_df_uploaded)} row(s) of staging Facebook Ads campaign insights to existing Google BigQuery table {staging_table_campaign}.")
                staging_sections_status["[STAGING] Upload staging Facebook Ads campaign insights to Google BigQuery"] = "succeed"
            except Exception as e:
                staging_sections_status["[STAGING] Upload staging Facebook Ads campaign insights to Google BigQuery"] = "failed"
                print(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to existing Google BigQuery table {staging_table_campaign} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to existing Google BigQuery table {staging_table_campaign} due to {e}.")      
                raise RuntimeError(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging Facebook Ads campaign insights to existing Google BigQuery table {staging_table_campaign} due to {e}.") from e     

    # 1.1.10. Summarize staging result(s) of Facebook Ads campaign insights
    finally:
        staging_time_elapsed = round(time.time() - staging_time_start, 2)
        staging_df_final = staging_df_uploaded.copy() if not staging_df_uploaded.empty else pd.DataFrame()
        staging_sections_total = len(staging_sections_status)
        staging_sections_succeed = [k for k, v in staging_sections_status.items() if v == "succeed"]
        staging_sections_failed = [k for k, v in staging_sections_status.items() if v == "failed"]     
        staging_tables_input = len(raw_tables_campaign)
        staging_tables_output = len(staging_table_queried)
        staging_tables_failed = staging_tables_input - staging_tables_output
        staging_rows_output = len(staging_df_final)
        if staging_sections_failed:
            print(f"❌ [STAGING] Failed to complete Facebook Ads campaign insights staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            logging.error(f"❌ [STAGING] Failed to complete Facebook Ads campaign insights staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            staging_status_final = "staging_failed_all"
        elif staging_tables_failed > 0:
            print(f"⚠️ [STAGING] Partially completed Facebook Ads campaign insights staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            logging.warning(f"⚠️ [STAGING] Partially completed Facebook Ads campaign insights staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            staging_status_final = "staging_failed_partial"
        else:
            print(f"🏆 [STAGING] Successfully completed Facebook Ads campaign insights staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            logging.info(f"🏆 [STAGING] Successfully completed Facebook Ads campaign insights staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            staging_status_final = "staging_succeed_all"
        staging_results_final = {
            "staging_df_final": staging_df_final,
            "staging_status_final": staging_status_final,
            "staging_summary_final": {
                "staging_time_elapsed": staging_time_elapsed,
                "schema_sections_total": staging_sections_total,
                "schema_sections_succeed": staging_sections_succeed,
                "schema_sections_failed": staging_sections_failed,
                "schema_sections_detail": staging_sections_status,
                "staging_tables_input": staging_tables_input,
                "staging_tables_output": staging_tables_output,
                "staging_tables_failed": staging_tables_failed,
                "staging_rows_output": staging_rows_output,
            }
        }
    return staging_results_final

# 1.2. Transform Facebook Ads ad insights from raw tables into cleaned staging tables
def staging_ad_insights() -> dict:
    print("🚀 [STAGING] Starting to build staging Facebook Ads ad insights table...")
    logging.info("🚀 [STAGING] Starting to build staging Facebook Ads ad insights table...")

    # 1.2.1. Start timing the Facebook Ads ad insights staging process
    staging_time_start = time.time()
    staging_section_succeeded = {}
    staging_section_failed = [] 
    staging_table_queried= []
    print(f"🔍 [STAGING] Proceeding to transform Facebook Ads ad insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [STAGING] Proceeding to transform Facebook Ads ad insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    try:

    # 1.2.2. Prepare table_id for Facebook Ads ad insights staging process
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        raw_adset_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
        raw_ad_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        raw_creative_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
        print(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook Ads ad insights...")
        logging.info(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook Ads ad insights...")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_ad = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_ad_insights} for Facebook Ads ad insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_ad_insights} for Facebook Ads ad insights...")

    # 1.2.3. Initialize Google BigQuery client
        try:
            print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            staging_section_succeeded["1.2.3. Initialize Google BigQuery client"] = True
        except Exception as e:
            staging_section_succeeded["1.2.3. Initialize Google BigQuery client"] = False
            staging_section_failed.append("1.2.3. Initialize Google BigQuery client")
            print(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.2.4. Scan all raw Facebook Ads ad insights table(s)
        try:
            print(f"🔍 [STAGING] Scanning all raw Facebook Ads ad insights table(s) from Google BigQuery dataset {raw_dataset}...")
            logging.info(f"🔍 [STAGING] Scanning all raw Facebook Ads ad insights table(s) from Google BigQuery dataset {raw_dataset}...")
            query_ad_raw = f"""
                SELECT table_name
                FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE REGEXP_CONTAINS(
                    table_name,
                    r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m[0-1][0-9][0-9]{{4}}$'
                )
            """
            raw_tables_ad = [row.table_name for row in google_bigquery_client.query(query_ad_raw).result()]
            raw_tables_ad = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables_ad]
            if not raw_tables_ad:
                raise RuntimeError("❌ [STAGING] Failed to scan raw Facebook Ads ad insights table(s) due to no tables found.")
            print(f"✅ [STAGING] Successfully found {len(raw_tables_ad)} raw Facebook Ads ad insights table(s).")
            logging.info(f"✅ [STAGING] Successfully found {len(raw_tables_ad)} raw Facebook Ads ad insights table(s).")
            staging_section_succeeded["1.2.4. Scan all raw Facebook Ads ad insights table(s)"] = True
        except Exception as e:
            staging_section_succeeded["1.2.4. Scan all raw Facebook Ads ad insights table(s)"] = False
            staging_section_failed.append("1.2.4. Scan all raw Facebook Ads ad insights table(s)")
            print(f"❌ [STAGING] Failed to scan raw Facebook Ads ad insights table(s) due to {e}.")
            logging.error(f"❌ [STAGING] Failed to scan raw Facebook Ads ad insights table(s) due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to scan raw Facebook Ads ad insights table(s) due to {e}.") from e

    # 1.2.5. Query all raw Facebook Ads ad insights table(s)
        for raw_table_ad in raw_tables_ad:
            query_ad_staging = f"""
                SELECT
                    raw.*,
                    ad.ad_name,
                    adset.adset_name,
                    campaign.campaign_name,
                    creative.thumbnail_url,
                    ad.effective_status AS delivery_status
                FROM `{raw_table_ad}` AS raw
                LEFT JOIN `{raw_ad_metadata}` AS ad
                    ON CAST(raw.ad_id AS STRING) = CAST(ad.ad_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(ad.account_id AS STRING)
                LEFT JOIN `{raw_adset_metadata}` AS adset
                    ON CAST(raw.adset_id AS STRING) = CAST(adset.adset_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(adset.account_id AS STRING)
                LEFT JOIN `{raw_campaign_metadata}` AS campaign
                    ON CAST(raw.campaign_id AS STRING) = CAST(campaign.campaign_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(campaign.account_id AS STRING)
                LEFT JOIN `{raw_creative_metadata}` AS creative
                    ON CAST(raw.ad_id AS STRING) = CAST(creative.ad_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(creative.account_id AS STRING)
            """
            try:
                print(f"🔄 [STAGING] Querying raw Facebook Ads ad insights table {raw_table_ad}...")
                logging.info(f"🔄 [STAGING] Querying raw Facebook Ads ad insights table {raw_table_ad}...")
                staging_df_queried = google_bigquery_client.query(query_ad_staging).to_dataframe()
                staging_table_queried.append(staging_df_queried)
                print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw Facebook Ads ad insights from {raw_table_ad}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw Facebook Ads ad insights from {raw_table_ad}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query raw Facebook Ads ad insights table {raw_table_ad} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query Facebook Ads ad insights table {raw_table_ad} due to {e}.")
                continue
        if not staging_table_queried:
            staging_section_succeeded["1.2.5. Query all raw Facebook Ads ad insights table(s)"] = False
            staging_section_failed.append("1.2.5. Query all raw Facebook Ads ad insights table(s)")
            raise RuntimeError("❌ [STAGING] Failed to query TiKTok Ads campaign insights due to no raw table found.")
        staging_df_concatenated = pd.concat(staging_table_queried, ignore_index=True)
        print(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of Facebook Ads ad insights from {len(staging_table_queried)} table(s).")
        logging.info(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of Facebook Ads ad insights from {len(staging_table_queried)} table(s).")
        staging_section_succeeded["1.2.5. Query all raw Facebook Ads ad insights table(s)"] = True

    # 1.2.6. Enrich Facebook Ads ad insights
        try:
            print(f"🔄 [STAGING] Triggering to enrich staging Facebook Ads ad insights field(s) for {len(staging_df_concatenated)} row(s)...")
            logging.info(f"🔄 [STAGING] Triggering to enrich staging Facebook Ads ad insights field(s) for {len(staging_df_concatenated)} row(s)...")
            staging_df_enriched = enrich_ad_fields(staging_df_concatenated, enrich_table_id=raw_table_ad)
            if "nhan_su" in staging_df_enriched.columns:
                vietnamese_map = {
                    'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                    'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
                    'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
                    'đ': 'd',
                    'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                    'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
                    'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                    'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                    'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
                    'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
                    'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                    'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
                    'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
                }
                vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map.items()}
                full_map = {**vietnamese_map, **vietnamese_map_upper}
                staging_df_enriched["nhan_su"] = staging_df_enriched["nhan_su"].apply(
                    lambda x: ''.join(full_map.get(c, c) for c in x) if isinstance(x, str) else x
                )
            print(f"✅ [STAGING] Successfully enriched {len(staging_df_enriched)} row(s) from all raw Facebook Ads ad insights table(s).")
            logging.info(f"✅ [STAGING] Successfully enriched {len(staging_df_enriched)} row(s) from all raw Facebook Ads ad insights table(s).")
            staging_section_succeeded["1.2.6. Enrich Facebook Ads ad insights"] = True
        except Exception as e:
            staging_section_succeeded["1.2.6. Enrich Facebook Ads ad insights"] = False
            staging_section_failed.append("1.2.6. Enrich Facebook Ads ad insights")
            print(f"❌ [STAGING] Failed to trigger enrichment for staging Facebook Ads campaign insights due to {e}.")
            logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging Facebook Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to trigger enrichment for staging Facebook Ads ad insights due to {e}.") from e

    # 1.2.7. Enforce schema for Facebook Ads campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_enriched)} row(s) of staging Facebook Ads ad insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_enriched)} row(s) of staging Facebook Ads ad insights...")
            staging_df_enforced = enforce_table_schema(staging_df_enriched, "staging_ad_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights due to {e}.")

    # 1.2.8. Create new table for staging Facebook Ads ad insights if not exist
        staging_df_deduplicated = staging_df_enforced.drop_duplicates()
        table_clusters_filtered = []
        table_schemas_defined = []
        try:
            print(f"🔍 [STAGING] Checking staging Facebook Ads ad insights table {staging_table_ad} existence...")
            logging.info(f"🔍 [STAGING] Checking staging Facebook Ads ad insights table {staging_table_ad} existence...")
            google_bigquery_client.get_table(staging_table_ad)
            staging_table_exists = True
        except Exception:
            staging_table_exists = False
        if not staging_table_exists:
            print(f"⚠️ [STAGING] Staging Facebook Ads ad insights table {staging_table_ad} not found then new table creation will be proceeding...")
            logging.warning(f"⚠️ [STAGING] Staging Facebook Ads ad insights table {staging_table_ad} not found then new table creation will be proceeding...")
            for col, dtype in staging_df_deduplicated.dtypes.items():
                if dtype.name.startswith("int"):
                    google_bigquery_type = "INT64"
                elif dtype.name.startswith("float"):
                    google_bigquery_type = "FLOAT64"
                elif dtype.name == "bool":
                    google_bigquery_type = "BOOL"
                elif "datetime" in dtype.name:
                    google_bigquery_type = "TIMESTAMP"
                else:
                    google_bigquery_type = "STRING"
                table_schemas_defined.append(bigquery.SchemaField(col, google_bigquery_type))
            table_configuration_defined = bigquery.Table(staging_table_ad, schema=table_schemas_defined)
            table_partition_effective = "date" if "date" in staging_df_deduplicated.columns else None
            if table_partition_effective:
                table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_partition_effective
                )
            table_clusters_defined = ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"]
            table_clusters_filtered = [f for f in table_clusters_defined if f in staging_df_deduplicated.columns]
            if table_clusters_filtered:  
                table_configuration_defined.clustering_fields = table_clusters_filtered  
            try:
                print(f"🔍 [STAGING] Creating staging Facebook Ads ad insights table with defined name {staging_table_ad} and partition on {table_partition_effective}...")
                logging.info(f"🔍 [STAGING] Creating staging Facebook Ads ad insights table with defined name {staging_table_ad} and partition on {table_partition_effective}...")
                table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                print(f"✅ [STAGING] Successfully created staging Facebook Ads ad insights table with actual name {table_metadata_defined.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully created staging Facebook Ads ad insights table with actual name {table_metadata_defined.full_table_id}.")
                staging_section_succeeded["1.2.8. Create new table for staging Facebook Ads ad insights if not exist"] = True
            except Exception as e:
                staging_section_succeeded["1.2.8. Create new table for staging Facebook Ads ad insights if not exist"] = False
                staging_section_failed.append("1.2.8. Create new table for staging Facebook Ads ad insights if not exist")
                print(f"❌ [STAGING] Failed to create staging Facebook Ads ad insights table {staging_table_ad} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to create staging Facebook Ads ad insights table {staging_table_ad} due to {e}.")
                raise RuntimeError(f"❌ [STAGING] Failed to create staging Facebook Ads ad insights table {staging_table_ad} due to {e}.") from e

    # 1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery   
        if not staging_table_exists:
            try: 
                print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to new Google BigQuery table {table_metadata_defined.full_table_id}...")
                logging.warning(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to Google BigQuery table {table_metadata_defined.full_table_id}...")     
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    ),
                    clustering_fields=table_clusters_filtered if table_clusters_filtered else None
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    staging_df_enforced,
                    staging_table_ad, 
                    job_config=job_config
                )
                load_job.result()
                staging_df_uploaded = staging_df_enforced.copy()
                print(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) of staging Facebook Ads ad insights to new Google BigQuery table {table_metadata_defined.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) of staging Facebook Ads ad insights to new Google BigQuery table {table_metadata_defined.full_table_id}.")
                staging_section_succeeded["1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery"] = True
            except Exception as e:
                staging_section_succeeded["1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery"] = False
                staging_section_failed.append("1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery")
                print(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.")      
                raise RuntimeError(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.") from e  
        else:
            try:
                print(f"🔍 [STAGING] Found existing Google BigQuery table {staging_table_ad} and {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights will be overwritten...")
                logging.warning(f"🔍 [STAGING] Found existing Google BigQuery table {staging_table_ad} and {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights will be overwritten...")                    
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    staging_df_enforced,
                    staging_table_ad, 
                    job_config=job_config
                )
                load_job.result()
                staging_df_uploaded = staging_df_enforced.copy()
                print(f"✅ [STAGING] Successfully overwrote {len(staging_df_uploaded)} row(s) of staging Facebook Ads ad insights to existing Google BigQuery table {staging_table_ad}.")
                logging.info(f"✅ [STAGING] Successfully overwrote {len(staging_df_uploaded)} row(s) of staging Facebook Ads ad insights to existing Google BigQuery table {staging_table_ad}.")
                staging_section_succeeded["1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery"] = True
            except Exception as e:
                staging_section_succeeded["1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery"] = False
                staging_section_failed.append("1.2.9. Upload staging Facebook Ads ad insights to Google BigQuery")
                print(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to existing Google BigQuery table {staging_table_ad} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to existing Google BigQuery table {staging_table_ad} due to {e}.")      
                raise RuntimeError(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging Facebook Ads ad insights to existing Google BigQuery table {staging_table_ad} due to {e}.") from e

    # 1.2.10. Summarize staging result(s) of Facebook Ads ad insights
    finally:
        staging_time_elapsed = round(time.time() - staging_time_start, 2)
        staging_df_final = staging_df_uploaded.copy() if not staging_df_uploaded.empty else pd.DataFrame()
        staging_tables_input = len(raw_tables_ad)
        staging_tables_succeeded = len(staging_table_queried)
        staging_tables_failed = staging_tables_input - staging_tables_succeeded
        staging_rows_uploaded = len(staging_df_final)
        if staging_section_failed:
            print(f"❌ [STAGING] Failed to complete Facebook Ads ad insights staging process with all {staging_tables_failed} table(s) failed and 0 row(s) queried in {staging_time_elapsed}s due to section {', '.join(staging_section_failed)}.")
            logging.error(f"❌ [STAGING] Failed to complete Facebook Ads ad insights staging process with all {staging_tables_failed} table(s) failed and 0 row(s) queried in {staging_time_elapsed}s due to section {', '.join(staging_section_failed)}.")
            staging_status_final = "staging_failed_all"
        elif staging_tables_failed > 0:
            print(f"⚠️ [STAGING] Completed Facebook Ads ad insights staging process with partial failure of {staging_tables_failed}/{staging_tables_input} table(s) and {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            logging.warning(f"⚠️ [STAGING] Completed Facebook Ads ad insights staging process with partial failure of {staging_tables_failed}/{staging_tables_input} table(s) and {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            staging_status_final = "staging_failed_partial"
        else:
            print(f"🏆 [STAGING] Successfully completed Facebook Ads ad insights staging process for {staging_tables_succeeded} table(s) with {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            logging.info(f"🏆 [STAGING] Successfully completed Facebook Ads ad insights staging process for {staging_tables_succeeded} table(s) with {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            staging_status_final = "staging_succeed_all"
        staging_results_final = {
            "staging_df_final": staging_df_final,
            "staging_status_final": staging_status_final,
            "staging_summary_final": {
                "staging_time_elapsed": staging_time_elapsed,
                "staging_rows_uploaded": staging_rows_uploaded,
                "staging_tables_input": staging_tables_input,
                "staging_tables_succeeded": staging_tables_succeeded,
                "staging_tables_failed": staging_tables_failed,
                "staging_section_failed": staging_section_failed,
            }
        }
    return staging_results_final     