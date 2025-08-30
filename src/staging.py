"""
==================================================================
FACEBOOK STAGING MODULE
------------------------------------------------------------------
This module transforms raw Facebook Ads data into enriched,  
normalized **staging tables** in BigQuery, acting as the bridge  
between raw API ingestion and final MART-level analytics.

It combines raw ad/campaign/creative data, applies business logic  
(e.g., parsing naming conventions, standardizing fields), and  
prepares clean, query-ready datasets for downstream consumption.

✔️ Joins raw ad insights with creative & campaign metadata  
✔️ Enriches fields like owner, program code, placement, format...  
✔️ Normalizes and writes standardized tables into staging dataset  

⚠️ This module is strictly responsible for *data transformation*  
into staging format. It does **not** handle API ingestion or final  
MART aggregations.
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

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google CLoud libraries for integration
from google.cloud import bigquery

# Add internal Facebook module for configuration
from config.utils import remove_string_accents
from config.schema import ensure_table_schema
 
# Add internal Facebook module for data handling
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

# 1.1. Transform Facebook Ads campaign-level insights from raw tables into cleaned staging tables
def staging_campaign_insights(updated_date: pd.DataFrame):
    print("🚀 [STAGING] Starting unified staging process for Facebook campaign insights...")
    logging.info("🚀 [STAGING] Starting unified staging process for Facebook campaign insights...")

    # 1.1.1. Prepare table_id
    try: 
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        print(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook campaign insights...")
        logging.info(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook campaign insights...")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_campaign_insights = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_campaign_insights} for Facebook campaign insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_campaign_insights} for Facebook campaign insights...")

    # 1.1.2. Scan all Facebook raw campaign insights table(s)
        print("🔍 [STAGING] Scanning all raw Facebook campaign insights table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw Facebook campaign insights table(s)...")
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to credentials error.") from e
        updated_date["date"] = pd.to_datetime(updated_date["date"])
        updated_months = updated_date["date"].dt.to_period("M").unique()
        raw_tables = [
            f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{d.month:02d}{d.year}"
            for d in updated_months.to_timestamp()
        ]
        if not raw_tables:
            print("⚠️ [STAGING] No raw Facebook campaign insights table(s) found then staging is skipped.")
            logging.warning("⚠️ [STAGING] No raw Facebook campaign insights table(s) found then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_tables)} raw Facebook campaign insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_tables)} raw Facebook campaign insights table(s).")

    # 1.1.3. Query raw table(s)
        all_dfs = []
        updated_date["date"] = pd.to_datetime(updated_date["date"])
        updated_months = updated_date["date"].dt.to_period("M").unique()
        raw_tables = [
            f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{d.month:02d}{d.year}"
            for d in updated_months.to_timestamp()
        ]
        for raw_table, month_period in zip(raw_tables, updated_months):
            month_days = updated_date[updated_date["date"].dt.to_period("M") == month_period]["date"]
            target_days_str = ",".join([f"DATE('{d.strftime('%Y-%m-%d')}')" for d in month_days])
            print(f"🔄 [STAGING] Querying raw Facebook campaign insights table {raw_table} for {len(month_days)} day(s)...")
            logging.info(f"🔄 [STAGING] Querying raw Facebook campaign insights table {raw_table} for {len(month_days)} day(s)...")
            query = f"""
                SELECT
                    raw.*,
                    metadata.campaign_name,
                    metadata.account_name,
                    metadata.effective_status AS delivery_status
                FROM `{raw_table}` AS raw
                LEFT JOIN `{raw_campaign_metadata}` AS metadata
                    ON CAST(raw.campaign_id AS STRING) = CAST(metadata.campaign_id AS STRING)
                    AND CAST(raw.account_id  AS STRING) = CAST(metadata.account_id  AS STRING)
                WHERE DATE(raw.date) IN ({target_days_str})
            """
            try:
                df_month = client.query(query).to_dataframe()
                print(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of raw Facebook campaign insights from {raw_table}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of raw Facebook campaign insights from {raw_table}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query Facebook campaign insights raw table {raw_table} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query Facebook campaign insights raw table {raw_table} due to {e}.")
                continue

    # 1.1.4. Enrich Facebook campaign insights
            if not df_month.empty:
                try:
                    print(f"🔄 [STAGING] Triggering to enrich staging Facebook campaign insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                    logging.info(f"🔄 [STAGING] Triggering to enrich staging Facebook campaign insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                    df_month = enrich_campaign_fields(df_month, table_id=raw_table)
                    if "nhan_su" in df_month.columns:
                        df_month["nhan_su"] = df_month["nhan_su"].apply(remove_string_accents)
                    all_dfs.append(df_month)
                except Exception as e:
                    print(f"❌ [STAGING] Failed to trigger enrichment for staging Facebook campaign insights due to {e}.")
                    logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging Facebook campaign insights due to {e}.")
                    continue
        if not all_dfs:
            print("⚠️ [STAGING] No data found in any raw Facebook campaign insights table(s).")
            logging.warning("⚠️ [STAGING] No data found in any raw Facebook campaign insights table(s).")
            return
        df_all = pd.concat(all_dfs, ignore_index=True)
        print(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all Facebook raw campaign insights table(s).")
        logging.info(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all Facebook raw campaign insights table(s).")

    # 1.1.5. Enforce schema for Facebook staging campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging Facebook campaign insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging Facebook campaign insights...")
            df_all = ensure_table_schema(df_all, "staging_campaign_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging Facebook campaign insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging Facebook campaign insights due to {e}.")
            raise   

    # 1.1.6. Upload Facebook staging campaign insights to Google BigQuery
        try:
            print(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging Facebook campaign insights to Google BigQuery table {staging_campaign_insights}...")
            logging.info(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging Facebook campaign insights to Google BigQuery table {staging_campaign_insights}...")
            df_all = df_all.drop_duplicates()
            clustering_fields = [f for f in ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"] if f in df_all.columns]
            try:
                client.get_table(staging_campaign_insights)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                schema = []
                for col, dtype in df_all.dtypes.items():
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
                table = bigquery.Table(staging_campaign_insights, schema=schema)
                if "date" in df_all.columns:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    )
                    if clustering_fields:  
                        table.clustering_fields = clustering_fields  
                    print(f"🔍 [STAGING] Creating staging Facebook campaign insights table {staging_campaign_insights} with partition on 'date'...")
                    logging.info(f"🔍 [STAGING] Creating staging Facebook campaign insights table {staging_campaign_insights} with partition on 'date'...")
                table = client.create_table(table)
                print(f"✅ [STAGING] Successfully created staging Facebook campaign insights table {staging_campaign_insights}.")
                logging.info(f"✅ [STAGING] Successfully created staging Facebook campaign insights table {staging_campaign_insights}.")
            else:
                new_dates = df_all["date"].dropna().dt.date.unique().tolist()
                query_existing = f"SELECT DISTINCT DATE(date) AS d FROM `{staging_campaign_insights}`"
                existing_dates = [row.d for row in client.query(query_existing).result()]
                overlap = set(new_dates) & set(existing_dates)
                if overlap:
                    print(f"⚠️ [STAGING] Found {len(overlap)} overlapping date(s) {overlap} in staging Facebook campaign insights table {staging_campaign_insights} then deletion will be proceeding...")
                    logging.warning(f"⚠️ [STAGING] Found {len(overlap)} overlapping date(s) {overlap} in staging Facebook campaign insights table {staging_campaign_insights} then deletion will be proceeding...")
                    for date_val in overlap:
                        query = f"""
                            DELETE FROM `{staging_campaign_insights}`
                            WHERE DATE(date) = @date_value
                        """
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[bigquery.ScalarQueryParameter("date_value", "DATE", date_val)]
                        )
                        try:
                            result = client.query(query, job_config=job_config).result()
                            print(f"✅ [STAGING] Successfully deleted {result.num_dml_affected_rows} row(s) for {date_val} in staging Facebook campaign insights table {staging_campaign_insights}.")
                            logging.info(f"✅ [STAGING] Successfully deleted {result.num_dml_affected_rows} row(s) for {date_val} in staging Facebook campaign insights table {staging_campaign_insights}.")
                        except Exception as e:
                            print(f"❌ [STAGING] Failed to delete row(s) of staging Facebook campaign insights for {date_val} due to {e}.")
                            logging.error(f"❌ [STAGING] Failed to delete row(s) of staging Facebook campaign insights for {date_val} due to {e}.")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.PARQUET,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date"
                ),
                clustering_fields=clustering_fields if clustering_fields else None
            )
            load_job = client.load_table_from_dataframe(
                df_all,
                staging_campaign_insights,
                job_config=job_config
            )
            load_job.result()
            print(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to staging Facebook campaign insights table {staging_campaign_insights}.")
            logging.info(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to staging Facebook campaign insights table {staging_campaign_insights}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed during staging Facebook campaign insights upload due to {e}.")
            logging.error(f"❌ [STAGING] Failed during staging Facebook campaign insights upload due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to unify staging Facebook campaign insights table due to {e}.")
        logging.error(f"❌ [STAGING] Faild to unify staging Facebook campaign insights table due to {e}.")

# 1.2. Transform Facebook Ads ad-level insights from raw tables into cleaned staging tables
def staging_ad_insights(updated_date: pd.DataFrame):
    print("🚀 [STAGING] Starting unified staging process for Facebook ad insights...")
    logging.info("🚀 [STAGING] Starting unified staging process for Facebook ad insights...")

    # 1.2.1. Prepare table_id
    try:
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        raw_adset_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
        raw_ad_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        raw_creative_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
        print(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook ad insights...")
        logging.info(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for Facebook ad insights...")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_ad_insights = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_ad_insights} for Facebook ad insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_ad_insights} for Facebook ad insights...")

    # 1.2.2. Scan all Facebook raw ad insights tables which match naming convetion
        print("🔍 [STAGING] Scanning all raw Facebook ad insights table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw Facebook ad insights table(s)...")
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        updated_date["date"] = pd.to_datetime(updated_date["date"])
        updated_months = updated_date["date"].dt.to_period("M").unique()
        raw_tables = [
            f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{d.month:02d}{d.year}"
            for d in updated_months.to_timestamp()
        ]
        if not raw_tables:
            print("⚠️ [STAGING] No raw Facebook ad insights table(s) found then staging is skipped.")
            logging.warning("⚠️ [STAGING] No raw Facebook ad insights table(s) found then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_tables)} raw Facebook ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_tables)} raw Facebook ad insights table(s).")

    # 1.2.3. Query raw table(s)
        all_dfs = []
        updated_date["date"] = pd.to_datetime(updated_date["date"])
        updated_months = updated_date["date"].dt.to_period("M").unique()
        raw_tables = [
            f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{d.month:02d}{d.year}"
            for d in updated_months.to_timestamp()
        ]
        for raw_table, month_period in zip(raw_tables, updated_months):
            month_days = updated_date[updated_date["date"].dt.to_period("M") == month_period]["date"]
            target_days_str = ",".join([f"DATE('{d.strftime('%Y-%m-%d')}')" for d in month_days])
            print(f"🔄 [STAGING] Querying raw Facebook ad insights table {raw_table} for {len(month_days)} day(s)...")
            logging.info(f"🔄 [STAGING] Querying raw Facebook ad insights table {raw_table} for {len(month_days)} day(s)...")
            query = f"""
                SELECT
                    raw.*,
                    ad.ad_name,
                    adset.adset_name,
                    campaign.campaign_name,
                    creative.thumbnail_url,
                    ad.effective_status AS delivery_status
                FROM `{raw_table}` AS raw
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
                WHERE DATE(raw.date) IN ({target_days_str})
            """
            try:
                df_month = client.query(query).to_dataframe()
                print(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of Facebook ad insights from {raw_table}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of Facebook ad insights from {raw_table}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query Facebook ad insights raw table {raw_table} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query Facebook ad insights raw table {raw_table} due to {e}.")
                continue
    
    # 1.1.4. Enrich Facebook ad insights            
            if not df_month.empty:
                try:
                    print(f"🔄 [STAGING] Triggering to enrich staging Facebook ad insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                    logging.info(f"🔄 [STAGING] Triggering to enrich staging Facebook ad insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                    df_month = enrich_ad_fields(df_month, table_id=raw_table)
                    if "nhan_su" in df_month.columns:
                        df_month["nhan_su"] = df_month["nhan_su"].apply(remove_string_accents)
                    all_dfs.append(df_month)
                except Exception as e:
                    print(f"❌ [STAGING] Failed to trigger enrichment for Facebook campaign insights due to {e}.")
                    logging.warning(f"❌ [STAGING] Failed to trigger enrichment for Facebook campaign insights due to {e}.")
                    continue
        if not all_dfs:
            print("⚠️ [STAGING] No data found in any raw Facebook ad insights table(s).")
            logging.warning("⚠️ [STAGING] No data found in any raw Facebook ad insights table(s).")
            return
        df_all = pd.concat(all_dfs, ignore_index=True)
        print(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all raw Facebook ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all raw Facebook ad insights table(s).")

    # 1.2.5. Enforce schema for Facebook staging campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging Facebook ad insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging Facebook ad insights...")
            df_all = ensure_table_schema(df_all, "staging_ad_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging Facebook ad insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging Facebook ad insights due to {e}.")
            raise

    # 1.2.6. Upload Facebook staging ad insights to Google BigQuery raw table
        try:
            print(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging Facebook ad insights to Google BigQuery table {staging_ad_insights}...")
            logging.info(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging Facebook campaign insights to Google BigQuery table {staging_ad_insights}...")
            df_all = df_all.drop_duplicates()
            clustering_fields = [f for f in ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"] if f in df_all.columns]
            try:
                client.get_table(staging_ad_insights)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                schema = []
                for col, dtype in df_all.dtypes.items():
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
                table = bigquery.Table(staging_ad_insights, schema=schema)
                if "date" in df_all.columns:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    )
                    if clustering_fields:  
                        table.clustering_fields = clustering_fields  
                    print(f"🔍 [STAGING] Creating staging table {staging_ad_insights} with partition on 'date'...")
                    logging.info(f"🔍 [STAGING] Creating staging table {staging_ad_insights} with partition on 'date'...")
                table = client.create_table(table)
                print(f"✅ [STAGING] Successfully created table {staging_ad_insights}.")
                logging.info(f"✅ [STAGING] Successfully created table {staging_ad_insights}.")
            else:
                new_dates = df_all["date"].dropna().dt.date.unique().tolist()
                query_existing = f"SELECT DISTINCT DATE(date) AS d FROM `{staging_ad_insights}`"
                existing_dates = [row.d for row in client.query(query_existing).result()]
                overlap = set(new_dates) & set(existing_dates)
                if overlap:
                    print(f"⚠️ [STAGING] Found {len(overlap)} overlapping date(s) {overlap}, deleting them before upload...")
                    logging.warning(f"⚠️ [STAGING] Found {len(overlap)} overlapping date(s) {overlap}, deleting them before upload...")
                    for date_val in overlap:
                        query = f"""
                            DELETE FROM `{staging_ad_insights}`
                            WHERE DATE(date) = @date_value
                        """
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[bigquery.ScalarQueryParameter("date_value", "DATE", date_val)]
                        )
                        try:
                            result = client.query(query, job_config=job_config).result()
                            print(f"✅ [STAGING] Deleted {result.num_dml_affected_rows} row(s) for {date_val}.")
                            logging.info(f"✅ [STAGING] Deleted {result.num_dml_affected_rows} row(s) for {date_val}.")
                        except Exception as e:
                            print(f"❌ [STAGING] Failed to delete rows for {date_val} due to {e}.")
                            logging.error(f"❌ [STAGING] Failed to delete rows for {date_val} due to {e}.")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.PARQUET,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date"
                ),
                clustering_fields=clustering_fields if clustering_fields else None
            )
            load_job = client.load_table_from_dataframe(
                df_all,
                staging_campaign_insights,
                job_config=job_config
            )
            load_job.result()
            print(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to {staging_ad_insights}.")
            logging.info(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to {staging_ad_insights}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed during staging Facebook ad insights upload due to {e}.")
            logging.error(f"❌ [STAGING] Failed during staging Facebook ad insights upload due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to unify staging Facebook ad insights table due to {e}.")
        logging.error(f"❌ [STAGING] Faild to unify staging Facebook ad insights table due to {e}.")
