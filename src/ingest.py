"""
==================================================================
FACEBOOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Facebook Marketing API into 
Google BigQuery, establishing the foundational raw layer of the Ads 
Data Pipeline used for centralized storage and historical retention.

It manages the complete ingestion flow — from authentication and 
data fetching, to enrichment, schema validation, and loading into 
BigQuery tables segmented by campaign, ad set, creative, and metadata.

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

# Add Python JSON ultilities for integration
import json 

# Add Python logging ultilities forintegration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python timezone ultilities for integration
import pytz

# Add Python time ultilities for integration
import time

# Add Python UUID ultilities for integration
import uuid

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    Forbidden,
    GoogleAPICallError
)
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Facebook modules for handling
from src.enrich import (
    enrich_campaign_insights, 
    enrich_ad_insights   
)
from src.fetch import (
    fetch_account_name,
    fetch_campaign_metadata, 
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights, 
    fetch_ad_insights,
)
from src.schema import ensure_table_schema

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

# 1.1. Ingest Facebook Ads campaign metadata from Facebook Ads API to Google BigQuery
def ingest_campaign_metadata(campaign_id_list: list) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to ingest Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to ingest Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 1.1.1. Start timing the Facebook Ads campaign metadata ingestion process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to ingest Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to ingest Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for Facebook Ads campaign metadata ingestion
    if not campaign_id_list:
        print("⚠️ [INGEST] Empty Facebook Ads campaign_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads campaign_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads campaign_id_list provided then ingestion is suspended.")

    try:

    # 1.1.3. Trigger to fetch Facebook Ads campaign metadata
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
            ingest_df_fetched = fetch_campaign_metadata(campaign_id_list=campaign_id_list)
            if ingest_df_fetched.empty:
                print("⚠️ [INGEST] Empty Facebook Ads campaign metadata returned then ingestion is suspended.")
                logging.warning("⚠️ [INGEST] Empty Facebook Ads campaign metadata returned then ingestion is suspended.")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching due to {e}.")
            return pd.DataFrame()

    # 1.1.4. Prepare table_id for Facebook Ads campaign metadata ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        print(f"🔍 [INGEST] Proceeding to ingest Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) with Google BigQuery table_id {table_id}...")
        logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) with Google BigQuery table_id {table_id}...")

    # 1.1.5. Enforce schema for Facebook Ads campaign metadata
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads campaign metadataw with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads campaign metadataw with {len(ingest_df_fetched)} row(s)...")
            ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_campaign_metadata")
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads campaign metadata due to {e}.")
            raise 
        
    # 1.1.6. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()    
            try:
                print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                google_bigquery_client = bigquery.Client(project=PROJECT)
                print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
                logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
            except Forbidden as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
            except GoogleAPICallError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
            except Exception as e:
                raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads campaign metadata table {table_id} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads campaign metadata table {table_id} existence...")
                google_bigquery_client.get_table(table_id)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                print(f"⚠️ [INGEST] Facebook Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
                schema = []
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
                    schema.append(bigquery.SchemaField(col, bq_type))
                table = bigquery.Table(table_id, schema=schema)
                effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
                if effective_partition:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=effective_partition
                    )
                clustering_fields = ["campaign_id", "account_id"]
                filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
                if filtered_clusters:
                    table.clustering_fields = filtered_clusters
                    print(f"🔍 [INGEST] Creating Facebook Ads campaign metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads campaign metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [INGEST] Successfully created Facebook Ads campaign metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
                logging.info(f"✅ [INGEST] Successfully created Facebook Ads campaign metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            else:
                print(f"🔄 [INGEST] Facebook Ads campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Facebook Ads campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
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
                        DELETE FROM `{table_id}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads campaign metadata table {table_id}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads campaign metadata table {table_id}.")
                else:
                    print(f"⚠️ [INGEST] No unique campaign_id and account_id keys found in Facebook Ads campaign metadata table {table_id} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique campaign_id and account_id keys found in Facebook Ads campaign metadata table {table_id} then existing row(s) deletion is skipped.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads campaign metadata ingestion due to {e}.")
            logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads campaign metadata ingestion due to {e}.")
            raise

    # 1.1.7. Upload Facebook Ads campaign metadata to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {table_id}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {table_id}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads campaign metadata to Google BigQuery table {table_id}.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload Facebok Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads campaign metadata due to {e}.")
            raise

    # 1.1.8. Summarize ingestion result(s)
        ingest_df_final = ingest_df_deduplicated
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        return ingest_df_final
    except Exception as e:
        print(f"❌ [INGEST] Failed to ingest Facebook Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to ingest Facebook Ads campaign metadata due to {e}.")
        return pd.DataFrame()

# 1.2. Ingest Facebook Ads adset metadata from Facebook Ads API to Google BigQuery
def ingest_adset_metadata(adset_id_list: list) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")

    # 1.2.1. Start timing the Facebook Ads adset metadata ingestion process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Validate input for Facebook Ads adset metadata ingestion
    if not adset_id_list:
        print("⚠️ [INGEST] Empty Facebook Ads adset_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads adset_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads adset_id_list provided then ingestion is suspended.")

    try:

    # 1.2.3. Trigger to fetch Facebook Ads adset metadata
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s)...")
            ingest_df_fetched = fetch_adset_metadata(adset_id_list=adset_id_list)
            if ingest_df_fetched.empty:
                print("⚠️ [INGEST] Empty Facebook Ads adset metadata returned then ingestion is suspended.")
                logging.warning("⚠️ [INGEST] Empty Facebook Ads adset metadata returned then ingestion is suspended.")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads adset metadata fetching due to {e}.")
            return pd.DataFrame()

    # 1.2.4. Prepare table_id for Facebook Ads adset metadata ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
        print(f"🔍 [INGEST] Proceeding to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) with Google BigQuery table_id {table_id}...")
        logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook Ads adset metadata for {len(adset_id_list)} adset_id(s) with Google BigQuery table_id {table_id}...")

    # 1.2.5. Enforce schema for Facebook Ads adset metadata
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads adset metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads adset metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_adset_metadata")
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads adset metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads adset metadata due to {e}.")
            raise 

    # 1.2.6. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()    
            try:
                print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                google_bigquery_client = bigquery.Client(project=PROJECT)
                print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
                logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
            except Forbidden as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
            except GoogleAPICallError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
            except Exception as e:
                raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads adset metadata table {table_id} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads adset metadata table {table_id} existence...")
                google_bigquery_client.get_table(table_id)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                print(f"⚠️ [INGEST] Facebook Ads adset metadata table {table_id} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads adset metadata table {table_id} not found then table creation will be proceeding..")
                schema = []
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
                    schema.append(bigquery.SchemaField(col, bq_type))
                table = bigquery.Table(table_id, schema=schema)
                effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
                if effective_partition:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=effective_partition
                    )
                clustering_fields = ["adset_id", "account_id"]
                filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
                if filtered_clusters:
                    table.clustering_fields = filtered_clusters
                    print(f"🔍 [INGEST] Creating Facebook Ads adset metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads adset metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [INGEST] Successfully created Facebook Ads adset metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
                logging.info(f"✅ [INGEST] Successfully created Facebook Ads adset metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            else:
                print(f"🔄 [INGEST] Facebook Ads adset metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Facebook Ads adset metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
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
                        DELETE FROM `{table_id}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads adset metadata {table_id}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads adset metadata {table_id}.")
                else:
                    print(f"⚠️ [INGEST] No unique adset_id and account_id keys found in Facebook Ads adset metadata table {table_id} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique adset_id and account_id keys found in Facebook Ads adset metadata table {table_id} then existing row(s) deletion is skipped.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads adset metadata ingestion due to {e}.")
            logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads adset metadata ingestion due to {e}.")
            raise

    # 1.2.7. Upload Facebook Ads adset metadata to Google BigQuerys
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads adset metadata to Google BigQuery table {table_id}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads adset metadata to Google BigQuery table {table_id}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads adset metadata to Google BigQuery table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads adset metadata to Google BigQuery table {table_id}.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload Facebok Ads adset metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads adset metadata due to {e}.")
            raise

    # 1.2.8. Summarize ingestion result(s)
        ingest_df_final = ingest_df_deduplicated
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [INGEST] Successfully completed Facebook Ads adset metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads adset metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        return ingest_df_final
    except Exception as e:
        print(f"❌ [INGEST] Failed to ingest Facebook Ads adset metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to ingest Facebook Ads adset metadata due to {e}.")
        return pd.DataFrame()

# 1.3. Ingest Facebook Ads ad metadata from Facebook Ads API to Google BigQuery
def ingest_ad_metadata(ad_id_list: list) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")

    # 1.3.1. Start timing the Facebook Ads ad metadata ingestion process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to ingest Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.3.2. Validate input for Facebook Ads ad metadata ingestion
    if not ad_id_list:
        print("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")

    try:

    # 1.3.3. Trigger to fetch Facebook Ads ad metadata
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
            ingest_df_fetched = fetch_ad_metadata(ad_id_list=ad_id_list)
            if ingest_df_fetched.empty:
                print("⚠️ [INGEST] Empty Facebook Ads ad metadata returned then ingestion is suspended.")
                logging.warning("⚠️ [INGEST] Empty Facebook Ads ad metadata returned then ingestion is suspended.")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads campaign metadata fetching due to {e}.")
            return pd.DataFrame()

    # 1.3.4. Prepare table_id for Facebook Ads ad metadata ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        print(f"🔍 [INGEST] Proceeding to ingest Facebook ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {table_id}...")
        logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {table_id}...")

    # 1.3.5. Enforce schema for Facebook Ads ad metadata
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_ad_metadata")
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad metadata due to {e}.")
            raise 

    # 1.3.6. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()    
            try:
                print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                google_bigquery_client = bigquery.Client(project=PROJECT)
                print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
                logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
            except Forbidden as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
            except GoogleAPICallError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
            except Exception as e:
                raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads ad metadata table {table_id} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads ad metadata table {table_id} existence...")
                google_bigquery_client.get_table(table_id)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                print(f"⚠️ [INGEST] Facebook Ads ad metadata table {table_id} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads ad metadata table {table_id} not found then table creation will be proceeding...")
                schema = []
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
                    schema.append(bigquery.SchemaField(col, bq_type))
                table = bigquery.Table(table_id, schema=schema)
                effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
                if effective_partition:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=effective_partition
                    )
                clustering_fields = ["ad_id", "account_id"]
                filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
                if filtered_clusters:
                    table.clustering_fields = filtered_clusters
                    print(f"🔍 [INGEST] Creating Facebook Ads ad metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads ad metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [INGEST] Successfully created Facebook Ads ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
                logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            else:
                print(f"🔄 [INGEST] Facebook Ads ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Facebook Ads ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
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
                        DELETE FROM `{table_id}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad metadata table {table_id}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad metadata table {table_id}.")
                else:
                    print(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook ad metadata table {table_id} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook ad metadata table {table_id} then existing row(s) deletion is skipped.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads ad metadata ingestion due to {e}.")
            logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads ad metadata ingestion due to {e}.")
            raise

    # 1.3.7. Upload Facebook Ads ad metadata to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad metadata to Google BigQuery table {table_id}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad metadata to Google BigQuery table {table_id}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad metadata to Google BigQuery table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad metadata to Google BigQuery table {table_id}.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload Facebok Ads ad metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads ad metadata due to {e}.")
            raise

    # 1.3.8. Summarize ingestion result(s)
        ingest_df_final = ingest_df_deduplicated
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [INGEST] Successfully completed Facebook Ads ad metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        return ingest_df_final
    except Exception as e:
        print(f"❌ [INGEST] Failed to ingest Facebook Ads ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to ingest Facebook Ads ad metadata due to {e}.")
        return pd.DataFrame()

# 1.4. Ingest Facebook Ads ad creative from Facebook Ads API into Google BigQuery
def ingest_ad_creative(ad_id_list: list) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")

    # 1.4.1. Start timing the Facebook Ads ad creative ingestion process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to ingest Facebook Ads ad creative for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.4.2. Validate input for Facebook Ads ad creative ingestion
    if not ad_id_list:
        print("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty Facebook Ads ad_id_list provided then ingestion is suspended.")

    try:
  
    # 1.4.3. Trigger to fetch Facebook Ads ad creative
        try:
            print(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Facebook Ads ad creative for {len(ad_id_list)} ad_id(s)...")
            ingest_df_fetched = fetch_ad_creative(ad_id_list=ad_id_list)
            if ingest_df_fetched.empty:
                print("⚠️ [INGEST] Empty Facebook Ads ad creative returned then ingestion is suspended.")
                logging.warning("⚠️ [INGEST] Empty Facebook Ads ad creative returned then ingestion is suspended.")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative fetching due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger Facebook Ads ad creative fetching due to {e}.")
            return pd.DataFrame()

    # 1.4.4. Prepare table_id for Facebook Ads ad creative ingestion
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_creative"
        print(f"🔍 [INGEST] Proceeding to ingest Facebook ad creative for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {table_id}...")
        logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook ad creative for {len(ad_id_list)} ad_id(s) with Google BigQuery table_id {table_id}...")

    # 1.4.5. Enforce schema for Facebook Ads ad creative
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad creative with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Facebook Ads ad creative with {len(ingest_df_fetched)} row(s)...")
            ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_ad_creative")
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad creative due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook Ads ad creative due to {e}.")
            raise 

    # 1.4.5. Delete existing row(s) or create new table if it not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
            try:
                print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
                google_bigquery_client = bigquery.Client(project=PROJECT)
                print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
                logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
            except Forbidden as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
            except GoogleAPICallError as e:
                raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
            except Exception as e:
                raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
            try:
                print(f"🔍 [INGEST] Checking Facebook Ads ad creative table {table_id} existence...")
                logging.info(f"🔍 [INGEST] Checking Facebook Ads ad creative table {table_id} existence...")
                google_bigquery_client.get_table(table_id)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                print(f"⚠️ [INGEST] Facebook Ads ad creative table {table_id} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Facebook Ads ad creative table {table_id} not found then table creation will be proceeding...")
                schema = []
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
                    schema.append(bigquery.SchemaField(col, bq_type))
                table = bigquery.Table(table_id, schema=schema)
                effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
                if effective_partition:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=effective_partition
                    )
                clustering_fields = ["ad_id", "account_id"]
                filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
                if filtered_clusters:
                    table.clustering_fields = filtered_clusters
                    print(f"🔍 [INGEST] Creating Facebook Ads ad creative table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                    logging.info(f"🔍 [INGEST] Creating Facebook Ads ad creative table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [INGEST] Successfully created Facebook Ads ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
                logging.info(f"✅ [INGEST] Successfully created Facebook Ads ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            else:
                print(f"🔄 [INGEST] Facebook Ads ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Facebook Ads ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
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
                        DELETE FROM `{table_id}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad creative table {table_id}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook Ads ad creative table {table_id}.")
                else:
                    print(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook Ads ad creative table {table_id} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique ad_id and account_id keys found in Facebook Ads ad creative table {table_id} then existing row(s) deletion is skipped.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads ad creative ingestion due to {e}.")
            logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of Facebook Ads ad creative ingestion due to {e}.")
            raise

    # 1.4.7. Upload Facebook Ads ad creative to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {table_id}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {table_id}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of Facebook Ads ad creative to Google BigQuery table {table_id}.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload Facebok Ads ad creative due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Facebook Ads ad creative due to {e}.")
            raise

    # 1.4.8. Summarize ingestion result(s)
        ingest_df_final = ingest_df_deduplicated
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [INGEST] Successfully completed Facebook Ads ad creative ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads ad creative ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
        return ingest_df_final
    except Exception as e:
        print(f"❌ [INGEST] Failed to ingest Facebook Ads ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to ingest Facebook Ads ad creative due to {e}.")
        return pd.DataFrame()

# 2. INGEST FACEBOOK ADS INSIGHTS FROM FACEBOOK MARKETING API TO GOOGLE BIGQUERY RAW TABLES

# 2.1. Ingest Facebook Ads campaign-level insights to Google BigQuery raw tables
def ingest_campaign_insights(
    start_date: str,
    end_date: str,
    write_disposition: str = "WRITE_APPEND"
) -> pd.DataFrame:
    
    print(f"🚀 [INGEST] Starting to ingest Facebook campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Call Facebook API to fetch campaign insights
    print("🔍 [INGEST] Triggering to fetch Facebook campaigns insights from API...")
    logging.info("🔍 [INGEST] Triggering to fetch Facebook campaigns insights from API...")
    df = fetch_campaign_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty Facebook campaign insights returned.")
        logging.warning("⚠️ [INGEST] Empty Facebook campaign insights returned.")    
        return df

    # 2.1.2. Prepare table_id
    first_date = pd.to_datetime(df["date_start"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook campaign insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook campaign insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich Facebook campaign insights
    try:
        print(f"🔁 [INGEST] Trigger to enrich Facebook campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Trigger to enrich Facebook campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_campaign_insights(df)
        df["account_name"] = fetch_account_name()
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment Facebook campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment Facebook campaign insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.1.4. Cast Facebook numeric fields to float
    try:
        numeric_fields = [
            "spend",
            "reach",
            "impressions",
            "clicks",
            "result",
            "purchase",
            "messaging_conversations_started"
        ]
        print(f"🔁 [INGEST] Casting Facebook campaign insights {numeric_fields} numeric field(s)...")
        logging.info(f"🔁 [INGEST] Casting Facebook campaign insights {numeric_fields} numeric field(s)...")
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"✅ [INGEST] Successfully casted Facebook campaign insights {numeric_fields} numeric field(s) to float.")
        logging.info(f"✅ [INGEST] Successfully casted Facebook campaign insights {numeric_fields} numeric field(s) to float.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to cast Facebook numeric field(s) to float due to {e}.")
        logging.error(f"❌ [INGEST] Failed to cast Facebook numeric field(s) to float due to {e}.")
        raise

    # 2.1.5. Enforce schema for Facebook campaign insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Facebook campaign insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Facebook campaign insights...")
        if "actions" in df.columns:
            df["actions"] = df["actions"].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None)
        df = ensure_table_schema(df, schema_type="ingest_campaign_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook campaign insights due to {e}.")
        raise

    # 2.1.6. Parse date column(s) for Facebook campaign insights
    try:
        print(f"🔁 [INGEST] Parsing Facebook campaign insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing Facebook campaign insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["date_start"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Facebook campaign insights.")
        logging.info(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Facebook campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for Facebook campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for Facebook campaign insights due to {e}.")
        raise

    # 2.1.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking Facebook campaign insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Facebook campaign insights table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Facebook campaign insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] Facebook campaign insights table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
                print(f"🔍 [INGEST] Creating Facebook campaign insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Facebook campaign insights {table_id} using partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Facebook campaign insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Facebook campaign insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
            existing_dates = [row.date_start for row in client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Facebook campaign insights {table_id} table then deletion will be proceeding...")
                logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Facebook campaign insights {table_id} table then deletion will be proceeding...")
                for date_val in overlap:
                    query = f"""
                        DELETE FROM `{table_id}`
                        WHERE date_start = @date_value
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                    )
                    try:
                        result = client.query(query, job_config=job_config).result()
                        deleted_rows = result.num_dml_affected_rows
                        print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Facebook campaign insights {table_id} table.")
                        logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Facebook campaign insights {table_id} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing rows in Facebook campaign insights {table_id} table for {date_val} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing rows in Facebook campaign insights {table_id} table for {date_val} due to {e}.")
            else:
                print(f"✅ [INGEST] No overlapping dates found in Facebook campaign insights {table_id} table then deletion is skipped.")
                logging.info(f"✅ [INGEST] No overlapping dates found in Facebook campaign insights {table_id} table then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Facebook campaign insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Facebook campaign insights ingestion due to {e}.")
        raise

    # 2.1.8. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook campaign insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook campaign insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook campaign insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Facebook campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Facebook campaign insights due to {e}.")
        raise
    return df

# 2.2. Ingest Facebook Ad ad insight to Google BigQuery raw tables
def ingest_ad_insights(
    start_date: str,
    end_date: str,
    write_disposition: str = "WRITE_APPEND"
) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Facebook ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Facebook ad insights from {start_date} to {end_date}...")

    # 2.2.1. Call Facebook API to fetch campaign insights
    print("🔍 [INGEST] Triggering to fetch Facebook ad insights from API...")
    logging.info("🔍 [INGEST] Triggering to fetch Facebook ad insights from API...")
    df = fetch_ad_insights(start_date, end_date)
    if df.empty:
        print("⚠️ [INGEST] Empty Facebook ad insights returned.")
        logging.warning("⚠️ [INGEST] Empty Facebook ad insights returned.")    
        return df

    # 2.2.2. Prepare full table_id for raw layer in BigQuery
    first_date = pd.to_datetime(df["date_start"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook ad insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook ad insights from {start_date} to {end_date} with table_id {table_id}...")
 
    # 2.2.3. Enrich Facebook ad insights
    try:
        print(f"🔁 [INGEST] Triggering to enrich Facebook ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Triggering to enrich Facebook ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_ad_insights(df)
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment Facebook ad insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment Facebook ad insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.2.4. Cast Facebook numeric fields to float
    try:
        numeric_fields = [
            "spend",
            "reach",
            "impressions",
            "clicks",
            "result",
            "purchase",
            "messaging_conversations_started"
        ]
        print(f"🔁 [INGEST] Casting Facebook ad insights {numeric_fields} numeric field(s)...")
        logging.info(f"🔁 [INGEST] Casting Facebook ad insights {numeric_fields} numeric field(s)....")
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"✅ [INGEST] Successfully casted Facebook ad insights {numeric_fields} numeric field(s) to float.")
        logging.info(f"✅ [INGEST] Successfully casted Facebook ad insights {numeric_fields} numeric field(s) to float.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to cast Facebook numeric field(s) to float due to {e}.")
        logging.error(f"❌ [INGEST] Failed to cast Facebook numeric field(s) to float due to {e}.")
        raise

    # 2.2.5. Enforce schema for Facebook ad insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Facebook ad insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Facebook ad insights...")
        if "actions" in df.columns:
            df["actions"] = df["actions"].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None)
        df = ensure_table_schema(df, "ingest_ad_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Facebook ad insights due to {e}.")
        raise

    # 2.2.6. Parse date column(s) for Facebook campaign insights
    try:
        print(f"🔁 [INGEST] Parsing Facebook ad insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing Facebook ad insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["date_start"])
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Facebook ad insights.")
        logging.info(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Facebook ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for Facebook ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for Facebook ad insights due to {e}.")
        raise

    # 2.2.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking Facebook ad insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Facebook ad insights table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Facebook ad insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] Facebook ad insights table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
                print(f"🔍 [INGEST] Creating Facebook ad insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Facebook ad insights {table_id} using partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Facebook ad insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Facebook ad insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
            existing_dates = [row.date_start for row in client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Facebook ad insights {table_id} table then deletion will be proceeding...")
                logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Facebook ad insights {table_id} table then deletion will be proceeding...")
                for date_val in overlap:
                    query = f"""
                        DELETE FROM `{table_id}`
                        WHERE date_start = @date_value
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                    )
                    try:
                        result = client.query(query, job_config=job_config).result()
                        deleted_rows = result.num_dml_affected_rows
                        print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Facebook ad insights {table_id} table.")
                        logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Facebook ad insights {table_id} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing rows in Facebook ad insights {table_id} table for {date_val} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing rows in Facebook ad insights {table_id} table for {date_val} due to {e}.")
            else:
                print(f"✅ [INGEST] No overlapping dates found in Facebook ad insights {table_id} table then deletion is skipped.")
                logging.info(f"✅ [INGEST] No overlapping dates found in Facebook ad insights {table_id} table then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Facebook ad insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Facebook ad insights ingestion due to {e}.")
        raise

    # 2.2.8. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook ad insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook ad insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook ad insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Facebook ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Facebook ad insights due to {e}.")
        raise
    return df