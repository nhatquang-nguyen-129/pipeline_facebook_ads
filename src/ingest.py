#services/facebook/ingest.py
"""
==================================================================
FACEBOOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Facebook Marketing API into 
Google BigQuery, forming the raw data layer of the Ads Data Pipeline.

It orchestrates the full ingestion flow: from authenticating the SDK, 
to fetching data, enriching it, validating schema, and loading into 
BigQuery tables organized by campaign, ad, creative and metadata.

✔️ Supports append or truncate via configurable `write_disposition`  
✔️ Applies schema validation through centralized schema utilities  
✔️ Includes logging and CSV-based error tracking for traceability

⚠️ This module is strictly limited to *raw-layer ingestion*.  
It does **not** handle data transformation, modeling, or aggregation.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add datetime utilities for managing timestamps and timezone conversions
from datetime import datetime
import pytz

# Add JSON support for debugging action list structure and testing data
import json 

# Add logging capability for tracking process execution and errors
import logging

# Add Python Pandas libraries for data processing
import pandas as pd

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google CLoud libraries for integration
from google.cloud import bigquery

# Add UUID module to generate unique identifiers for ingest operations or error tracking
import uuid

# Add internal Facebook module for data handling
from services.facebook.enrich import (
    enrich_campaign_insights, 
    enrich_ad_insights   
)
from services.facebook.fetch import (
    fetch_account_name,
    fetch_campaign_metadata, 
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights, 
    fetch_ad_insights,
)
from services.facebook.schema import ensure_table_schema

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

# 1. INGEST FACEBOOK ADS METADATA FROM FACEBOOK MARKETING API TO GOOGLE BIGQUERY RAW TABLES 

# 1.1. Ingest Facebook campaign metadata from Facebook API to Google BigQuery raw table
def ingest_campaign_metadata(campaign_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest Facebook campaign metadata...")
    logging.info("🚀 [INGEST] Starting Facebook campaign metadata...")

    # 1.1.1. Validate input list is not empty
    if not campaign_id_list:
        print("⚠️ [INGEST] Empty Facebook campaign_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty Facebook campaign_id_list provided then ingestion is suspended.")
        raise ValueError("❌ [INGEST] Facebook campaign_id_list must be provided and not empty.")

    # 1.1.2. Call Facebook API to fetch campaign metadata
    try:
        print(f"🔍 [INGEST] Fetching Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) from API...")
        logging.info(f"🔍 [INGEST] Fetching Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) from API...")
        df = fetch_campaign_metadata(campaign_id_list=campaign_id_list)
        print(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of Facebook campaign metadata.")
        logging.info(f"✅ [INGEST] Successfully fetching {len(df)} row(s) of Facebook campaign metadata.")
        if df.empty:
            print("⚠️ [INGEST] Empty Facebook campaign metadata returned.")
            logging.warning("⚠️ [INGEST] Empty Facebook campaign metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to fetch Facebook campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to fetch Facebook campaign metadata due to {e}.")
        return pd.DataFrame()

    # 1.1.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")

    # 1.1.4. Enforce schema for Facebook campaign metadata
    try:
        print(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook campaign metadata...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook campaign metadata...")
        df = ensure_table_schema(df, "ingest_campaign_metadata")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook campaign metadata.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook campaign metadata.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for Facebook campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for Facebook campaign metadata due to {e}.")
        return df
    
    # 1.1.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking Facebok campaign metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Facebok campaign metadata table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Facebook campaign metadata table {table_id} not found then table creation wil be proceeding...")
            logging.info(f"⚠️ [INGEST] Facebook campaign metadata table {table_id} not found then table creation wil be proceeding...")
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
            clustering_fields = ["campaign_id", "account_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating Facebook campaign metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Facebook campaign metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Facebook campaing metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Facebook campaing metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] Facebook campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Facebook campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["campaign_id", "account_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
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
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook campaign metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook campaign metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (campaign_id, account_id) keys found in Facebook campaign metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (campaign_id, account_id) keys found in Facebook campaign metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Facebook campaign metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Facebook campaign metadata ingestion due to {e}.")
        raise

    # 1.1.6. Upload Facebook campaign metadata to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook campaign metadata {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook campaign metadata {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook campaign metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook campaign metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Facebook campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Facebook campaign metadata due to {e}.")
        raise
    return df

# 1.2. Ingest Facebook adset metadata from Facebook API to Google BigQuery raw table
def ingest_adset_metadata(adset_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest Facebook adset metadata...")
    logging.info("🚀 [INGEST] Starting Facebook adset metadata...")

    # 1.2.1. Validate input for Facebook adset metadata selective fetching
    if not adset_id_list:
        print("⚠️ [FETCH] Empty Facebook adset_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook adset_id_list provided.")
        raise ValueError("⚠️ [FETCH] Empty Facebook adset_id_list provided.")

    # 1.2.2. Call Facebook API to fetch adset metadata
    try:
        print(f"🔍 [INGEST] Fetching Facebook adset metadata for {len(adset_id_list)} adset_id(s) from API...")
        logging.info(f"🔍 [INGEST] Fetching Facebook adset metadata for {len(adset_id_list)} adset_id(s) from API...")
        df = fetch_adset_metadata(adset_id_list=adset_id_list)
        print(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of Facebook adset metadata.")
        logging.info(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of Facebook adset metadata.")
        if df.empty:
            print("⚠️ [INGEST] Empty Facebook adset metadata returned.")
            logging.warning("⚠️ [INGEST] Empty Facebook adset metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to fetch Facebook adset metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to fetch Facebook adset metadata due to {e}.")
        return pd.DataFrame()

    # 1.2.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook adset metadata for {len(adset_id_list)} adset_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook adset metadata for {len(adset_id_list)} adset_id(s) with table_id {table_id}...")

    # 1.2.4. Enforce schema for Facebook adset metadata
    try:
        print(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook adset metadata...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook adset metadata...")
        df = ensure_table_schema(df, "ingest_adset_metadata")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook adset metadata.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook adset metadata.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for Facebook adset metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for Facebook adset metadata due to {e}.")
        return df

    # 1.2.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking Facebook adset metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Facebook adset metadata table {table_id} existence...")
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
            print(f"⚠️ [INGEST] Facebook adset metadata table {table_id} not found then table creation wil be proceeding...")
            logging.info(f"⚠️ [INGEST] Facebook adset metadata table {table_id} not found then table creation wil be proceeding...")
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
            clustering_fields = ["adset_id", "account_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating Facebook adset metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Facebook adset metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Facebook adset metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Facebook adset metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] Facebook adset metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Facebook adset metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["adset_id", "account_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
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
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook adset metadata {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook adset metadata {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (adset_id, account_id) keys found in Facebook adset metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (adset_id, account_id) keys found in Facebook adset metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Facebook adset metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Facebook adset metadata ingestion due to {e}.")
        raise

    # 1.2.6. Upload Facebook adset metadata to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook adset metadata to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook adset metadata to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook adset metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook adset metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Facebook adset metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Facebook adset metadata due to {e}.")
        raise
    return df

# 1.3. Ingest Facebook ad metadata from Facebook API to Google BigQuery raw table
def ingest_ad_metadata(ad_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest Facebook ad metadata...")
    logging.info("🚀 [INGEST] Starting Facebook ad metadata...")

    # 1.3.1. Validate input for Facebook ad metadata selective fetching
    if not ad_id_list:
        print("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        raise ValueError("⚠️ [FETCH] Empty Facebook ad_id_list provided.")

    # 1.3.2. Call Facebook API to fetch ad metadata
    try:
        print(f"🔍 [INGEST] Fetching Facebook ad metadata for {len(ad_id_list)} of ad_id(s) from API...")
        logging.info(f"🔍 [INGEST] Fetching Facebook ad metadata for {len(ad_id_list)} of ad_id(s) from API...")
        df = fetch_ad_metadata(ad_id_list=ad_id_list)
        if df.empty:
            print("⚠️ [INGEST] Empty Facebook ad metadata returned.")
            logging.warning("⚠️ [INGEST] Empty Facebook ad metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to fetch Facebook ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to fetch Facebook ad metadata due to {e}.")
        return pd.DataFrame()

    # 1.3.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook ad metadata for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook ad metadata for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")

    # 1.3.4. Enforce schema for Facebook ad metadata
    try:
        print(f"🔄 [INGEST] Enforcing schema for {len(ad_id_list)} row(s) of Facebook ad metadata...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(ad_id_list)} row(s) of Facebook ad metadata...")
        df = ensure_table_schema(df, "ingest_ad_metadata")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook ad metadata.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook ad metadata.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for Facebook ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for Facebook ad metadata due to {e}.")
        return df

    # 1.3.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking Facebook ad metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Facebook ad metadata table {table_id} existence...")
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
            print(f"⚠️ [INGEST] Facebook ad metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] Facebook ad metadata table {table_id} not found then table creation will be proceeding...")
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
            clustering_fields = ["ad_id", "account_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating Facebook ad metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Facebook ad metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Facebook ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Facebook ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] Facebook ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Facebook ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["ad_id", "account_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
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
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook ad metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook ad metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (ad_id, account_id) keys found in Facebook ad metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id, account_id) keys found in Facebook ad metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Facebook ad metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Facebook ad metadata ingestion due to {e}.")
        raise

    # 1.3.6. Upload Facebook ad metadata to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook ad metadata to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook ad metadata to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook ad metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook ad metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Facebook ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Facebook ad metadata due to {e}.")
        raise
    return df

# 1.4. Ingest Facebook ad creatives for given ad_ids into Google BigQuery raw table
def ingest_ad_creative(ad_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest Facebook ad creative...")
    logging.info("🚀 [INGEST] Starting to ingest Facebook ad creative...")

    # 1.4.1. Validate input for Facebook ad creative selective fetching
    if not ad_id_list:
        print("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
        raise ValueError("⚠️ [FETCH] Empty Facebook ad_id_list provided.")
  
    # 1.4.2. Call Facebook API to fetch adset metadata
    try:
        print(f"🔍 [INGEST] Fetching Facebook creative metadata for {len(ad_id_list)} ad_id(s) from API...")
        logging.info(f"🔍 [INGEST] Fetching Facebook creative metadata for {len(ad_id_list)} ad_id(s) from API...")
        df = fetch_ad_creative(ad_id_list)
        print(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of Facebook creative metadata.")
        logging.info(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of Facebook creative metadata.")
        if df.empty:
            print("⚠️ [INGEST] No Facebook ad creative returned.")
            logging.warning("⚠️ [INGEST] No Facebook ad creative returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to fetch Facebook ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to fetch Facebook ad creative due to {e}.")
        return pd.DataFrame()

    # 1.4.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook ad metadata for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook ad metadata for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")

    # 1.4.4 Enforce schema for Facebook ad creative
    try:
        print(f"🔍 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook ad creative...")
        logging.info(f"🔍 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook ad creative...")
        df = ensure_table_schema(df, schema_type="ingest_ad_creative")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook ad creative.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook ad creative.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for Facebook ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for Facebook ad creative due to {e}.")
        return df

    # 1.4.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking Facebook ad creative table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Facebook ad creative table {table_id} existence...")
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
            print(f"⚠️ [INGEST] Facebook ad creative table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] Facebook ad creative table {table_id} not found then table creation will be proceeding...")
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
            clustering_fields = ["ad_id", "account_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating Facebook ad creative table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition})...")
                logging.info(f" [INGEST] Creating Facebook ad creative table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Facebook ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Facebook ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] Facebook ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Facebook ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["ad_id", "account_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
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
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook ad creative table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of Facebook ad creative table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (ad_id, account_id) keys found in Facebook ad creative table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id, account_id) keys found in Facebook ad creative table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Facebook ad creative ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Facebook ad creative ingestion due to {e}.")
        raise

    # 1.4.6. Upload Facebook ad creative to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook ad creative to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Facebook ad creative to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook ad creative.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Facebook ad creative.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Facebook ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Facebook ad creative due to {e}.")
        raise
    return df

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
    print("🔍 [INGEST] Fetching Facebook campaigns insights from API...")
    logging.info("🔍 [INGEST] Fetching Facebook campaigns insights from API...")
    df = fetch_campaign_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty Facebook campaign insights returned.")
        logging.warning("⚠️ [INGEST] Empty Facebook campaign insights returned.")    
        return df

    # 2.1.2. Prepare full table_id for raw layer in BigQuery
    first_date = pd.to_datetime(df["date_start"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook campaign insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook campaign insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich Facebook campaign insights
    try:
        print(f"🔁 [INGEST] Enriching Facebook campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Enriching Facebook campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_campaign_insights(df)
        df["account_name"] = fetch_account_name()
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
        print(f"✅ [INGEST] Successfully enriched Facebook campaign insights from {start_date} to {end_date} with {len(df)} row(s).")
        logging.info(f"✅ [INGEST] Successfully enriched Facebook campaign insights from {start_date} to {end_date} with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enrich Facebook campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enrich Facebook campaign insights from {start_date} to {end_date} due to {e}.")
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
        print(f"🔁 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook campaign insights...")
        logging.info(f"🔁 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook campaign insights...")
        if "actions" in df.columns:
            df["actions"] = df["actions"].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None)
        df = ensure_table_schema(df, schema_type="ingest_campaign_insights")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook campaign insights.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for Facebook campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for Facebook campaign insights due to {e}.")
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
    print("🔍 [INGEST] Fetching Facebook ad insights from API...")
    logging.info("🔍 [INGEST] Fetching Facebook ad insights from API...")
    df = fetch_ad_insights(start_date, end_date)
    if df.empty:
        print("⚠️ [INGEST] Empty Facebook ad insights returned.")
        logging.warning("⚠️ [INGEST] Empty Facebook ad insights returned.")    
        return df

    # 2.2.2. Prepare full table_id for raw layer in BigQuery
    first_date = pd.to_datetime(df["date_start"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest Facebook ad insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Facebook ad insights from {start_date} to {end_date} with table_id {table_id}...")
 
    # 2.2.3. Enrich Facebook ad insights
    try:
        print(f"🔁 [INGEST] Enriching Facebook ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Enriching Facebook ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_ad_insights(df)
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
        print(f"✅ [INGEST] Successfully enriched Facebook ad insights from {start_date} to {end_date} with {len(df)} row(s).")
        logging.info(f"✅ [INGEST] Successfully enriched Facebook ad insights from {start_date} to {end_date} with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enrich Facebook ad insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enrich Facebook ad insights from {start_date} to {end_date} due to {e}.")
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
        print(f"🔁 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook ad insights...")
        logging.info(f"🔁 [INGEST] Enforcing schema for {len(df)} row(s) of Facebook ad insights...")
        if "actions" in df.columns:
            df["actions"] = df["actions"].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None)
        df = ensure_table_schema(df, "ingest_ad_insights")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook ad insights.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of Facebook ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for Facebook ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for Facebook ad insights due to {e}.")
        raise

    # 2.2.6. Parse date column(s) for Facebook campaign insights
    try:
        print(f"🔁 [INGEST] Parsing Facebook ad insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing Facebook ad insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["date_start"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
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