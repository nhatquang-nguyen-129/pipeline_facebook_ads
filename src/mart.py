"""
==================================================================
FACEBOOK MATERIALIZATION MODULE
------------------------------------------------------------------
This module builds the MART layer for Facebook Ads by aggregating 
and transforming data from staging tables generated during the 
raw layer ingestion process. 

It focuses on preparing final analytical tables for cost tracking 
and campaign performance reporting at a daily granularity.

✔️ Dynamically detects all campaign staging tables for the target year  
✔️ Applies transformation and standardization (type cast, parsing)  
✔️ Writes partitioned & clustered MART tables to BigQuery for analytics  

⚠️ This module is strictly responsible for *MART layer construction*.  
It does not handle raw data ingestion, API fetching, or enrichment logic.
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

# Add Google Authentication libraries for integration
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import AuthorizedSession

# Add Google Spreadsheets API libraries for integration
import gspread

# Add Google CLoud libraries for integration
from google.cloud import bigquery

# Add Google Secret Manager for integration
from google.cloud import secretmanager

# Add UUID libraries for integration
import uuid

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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR CAMPAIGN PERFORMANCE FROM STAGING TABLE(S)

# 1.1. Build materialzed table for Facebook campaign performance by union all staging tables
def mart_campaign_all() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook campaign performance...")

    # 1.1.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook campaign performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook campaign performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_performance = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Facebook campaign performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Facebook campaign performance...")

    # 1.1.2. Query all staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_performance}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            SELECT
                SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,  -- thêm nganh_hang
                SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                CAST(`date` AS DATE) AS ngay,
                SAFE_CAST(spend AS FLOAT64) AS spend,
                SAFE_CAST(result AS INT64) AS result,
                SAFE_CAST(result_type AS STRING) AS result_type,
                SAFE_CAST(purchase AS INT64) AS purchase,
                SAFE_CAST(messaging_conversations_started AS INT64) AS messaging_conversations_started,
                SAFE_CAST(reach AS INT64) AS reach,
                SAFE_CAST(impressions AS INT64) AS impressions,
                SAFE_CAST(clicks AS INT64) AS clicks,
                CASE
                    WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                    WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                    ELSE "❓"
                END AS trang_thai
            FROM `{staging_table_campaign}`
            WHERE date IS NOT NULL
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_performance}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_performance} with {row_count} row(s) for Facebook campaign performance.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_performance} with {row_count} row(s) for Facebook campaign performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook campaign performance due to {e}.")

# 1.2. Build materialzed table for Facebook supplier campaign performance by union all staging tables
def mart_campaign_supplier() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook campaign performance (Supplier)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook campaign performance (Supplier)...")

    # 1.2.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_campaign_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_campaign_performance"

        print(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier metadata...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier metadata...")

    # 1.2.2. Initialize Google BigQuery client
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud project {PROJECT}...")
            bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}...")
            logging.info(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}...")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to your credentials.") from e

    # 1.2.3. Initialize Google Secret Manager client
        try:
            print(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud project {PROJECT}...")
            secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}...")
            logging.info(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}...")
        except Exception as e:
            print(f"❌ [MART] Failed to initialize Google Secret Manager client due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google Secret Manager client due to {e}.")
            raise

    # 1.2.4. Initialize Google Sheets client
        try:
            print(f"🔍 [MART] Initializing Google Sheets client...")
            logging.info(f"🔍 [MART] Initializing Google Sheets client....")
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds, _ = default(scopes=scopes)
            gspread_client = gspread.Client(auth=creds)
            gspread_client.session = AuthorizedSession(creds)
            print(f"✅ [MART] Successfully initialized Google Sheets client with scope {scopes}.")
            logging.info(f"✅ [MART] Successfully initialized Google Sheets client with scope {scopes}.")
        except Exception as e:
            print(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.")
            raise

    # 1.2.5. Query supplier metadata from Google Sheets
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_budget_sheet_id_supplier"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(name=secret_name)
        sheet_id_supplier = response.payload.data.decode("UTF-8")
        worksheet = gspread_client.open_by_key(sheet_id_supplier).worksheet("supplier")
        records = worksheet.get_all_records()
        df_supplier = pd.DataFrame(records)        
        if "supplier_name" not in df_supplier.columns:
            raise RuntimeError("❌ [MART] Missing 'supplier_name' column in supplier sheet.")
        temp_table_id = f"{PROJECT}.{mart_dataset}.temp_supplier_{uuid.uuid4().hex[:8]}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        try:
            print(f"🔍 [MART] Creating supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            logging.info(f"🔍 [MART] Creating supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            bigquery_client.load_table_from_dataframe(df_supplier[["supplier_name"]], temp_table_id, job_config=job_config).result()
            print(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            logging.info(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
        except Exception as e:
            print(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.")
            logging.error(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.")
    
    # 1.2.6. Query staging table to build materialized table for supplier
        print(f"🔄 [MART] Querying staging Facebook campaign insights table {staging_table_campaign} to build materialized table for supplier campaign performance...")
        logging.info(f"🔄 [MART] Querying staging Facebook campaign insights table {staging_table_campaign} to build materialized table for supplier campaign performance...")
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_campaign_supplier}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            WITH base AS (
                SELECT
                    a.*,
                    s.supplier_name AS supplier_name
                FROM `{staging_table_campaign}` a
                LEFT JOIN `{temp_table_id}` s
                  ON REGEXP_CONTAINS(a.chuong_trinh, s.supplier_name)
                WHERE a.ma_ngan_sach_cap_1 = 'NC'
                  AND a.date IS NOT NULL
            )
            SELECT
                SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,
                SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                SAFE_CAST(supplier_name AS STRING) AS supplier_name,
                CAST(date AS DATE) AS ngay,
                SAFE_CAST(spend AS FLOAT64) AS spend,
                SAFE_CAST(result AS INT64) AS result,
                SAFE_CAST(result_type AS STRING) AS result_type,
                SAFE_CAST(purchase AS INT64) AS purchase,
                SAFE_CAST(messaging_conversations_started AS INT64) AS messaging_conversations_started,
                SAFE_CAST(reach AS INT64) AS reach,
                SAFE_CAST(impressions AS INT64) AS impressions,
                SAFE_CAST(clicks AS INT64) AS clicks,
                CASE
                    WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                    WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                    ELSE "❓"
                END AS trang_thai
            FROM base
        """
        bigquery_client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_campaign_supplier}`"
        row_count = list(bigquery_client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully built materialized table {mart_table_campaign_supplier} with {row_count} row(s) for Facebook supplier campaign performance.")
        logging.info(f"✅ [MART] Successfully built materialized table {mart_table_campaign_supplier} with {row_count} row(s) for Facebook supplier campaign performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook supplier campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook supplier campaign performance due to {e}.")
    finally:
        try:
            print(f"🔄 [MART] Facebook supplier campaign performance process is completed then supplier metadata temporary table deletion will be proceeding...")
            logging.info(f"🔄 [MART] Facebook supplier campaign performance process is completed then supplier metadata temporary table deletion will be proceeding...")
            bigquery_client.delete_table(temp_table_id, not_found_ok=True)
            print(f"✅ [INGEST] Successfully deleted supplier metadata temporary table {temp_table_id}.")
            logging.info(f"✅ [INGEST] Successfully deleted supplier metadata temporary table {temp_table_id}.")
        except Exception as cleanup_error:
            print(f"❌ [MART] Failed to delete supplier metadata temporary table {temp_table_id} due to {cleanup_error}.")
            logging.warning(f"❌ [MART] Failed to delete supplier metadata temporary table {temp_table_id} due to {cleanup_error}.")

# 1.3. Build materialzed table for Facebook festival campaign performance by union all staging tables
def mart_campaign_festival() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook campaign performance (Festival only)...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook campaign performance (Festival only)...")

    # 1.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Festival campaign performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Festival campaign performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_performance = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_festival_campaign_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Festival campaign performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Festival campaign performance...")

    # 1.3.2. Query all staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_performance}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            SELECT
                SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,  -- thêm nganh_hang
                SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                CAST(`date` AS DATE) AS ngay,
                SAFE_CAST(spend AS FLOAT64) AS spend,
                SAFE_CAST(result AS INT64) AS result,
                SAFE_CAST(result_type AS STRING) AS result_type,
                SAFE_CAST(purchase AS INT64) AS purchase,
                SAFE_CAST(messaging_conversations_started AS INT64) AS messaging_conversations_started,
                SAFE_CAST(reach AS INT64) AS reach,
                SAFE_CAST(impressions AS INT64) AS impressions,
                SAFE_CAST(clicks AS INT64) AS clicks,
                CASE
                    WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                    WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                    ELSE "❓"
                END AS trang_thai
            FROM `{staging_table_campaign}`
            WHERE date IS NOT NULL
              AND nganh_hang = 'FTV'
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_performance}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_performance} with {row_count} row(s) for Festival campaign performance.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_performance} with {row_count} row(s) for Festival campaign performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Festival campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Festival campaign performance due to {e}.")

# 2. MONTHLY MATERIALIZED TABLE FOR CREATIVE PERFORMANCE FROM STAGING TABLE(S)

# 2.1. Build materialized table for Facebook creative performance by union all staging tables
def mart_creative_all() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance (All)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance (All)...")

    # 2.1.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_creative_performance"
        print(f"🔍 [MART] Using staging table {staging_table} for creative performance (All)...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} for creative performance (All)...")

    # 2.1.2. Query staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_creative_all}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            SELECT
                SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,
                SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                SAFE_CAST(adset_name AS STRING) AS adset_name,
                SAFE_CAST(ad_name AS STRING) AS ad_name,
                SAFE_CAST(thumbnail_url AS STRING) AS thumbnail_url,
                SAFE_CAST(vi_tri AS STRING) AS vi_tri,
                SAFE_CAST(doi_tuong AS STRING) AS doi_tuong,
                SAFE_CAST(dinh_dang AS STRING) AS dinh_dang,
                CAST(date AS DATE) AS ngay,
                SAFE_CAST(spend AS FLOAT64) AS spend,
                SAFE_CAST(result AS INT64) AS result,
                SAFE_CAST(result_type AS STRING) AS result_type,
                SAFE_CAST(purchase AS INT64) AS purchase,
                SAFE_CAST(messaging_conversations_started AS INT64) AS messaging_conversations_started,
                SAFE_CAST(reach AS INT64) AS reach,
                SAFE_CAST(impressions AS INT64) AS impressions,
                SAFE_CAST(clicks AS INT64) AS clicks,
                CASE
                    WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                    WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                    ELSE "❓"
                END AS trang_thai
            FROM `{staging_table}`
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_creative_all}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_creative_all} with {row_count} row(s) for Facebook creative performance (All).")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_creative_all} with {row_count} row(s) for Facebook creative performance (All).")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook creative performance (All) due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook creative performance (All) due to {e}.")

# 2.2. Build materialized table for Facebook supplier creative performance by union all staging tables
def mart_creative_supplier() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance (Supplier)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance (Supplier)...")

    try:
        # 2.2.1. Prepare table_id
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_creative = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_creative_performance"

        print(f"🔍 [MART] Using staging table {staging_table_creative} with supplier metadata...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_creative} with supplier metadata...")

        # 2.2.2. Initialize BigQuery client
        try:
            bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized BigQuery client for {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized BigQuery client for {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [MART] Failed to initialize BigQuery client due to your credentials.") from e

        # 2.2.3. Initialize Secret Manager client
        try:
            secret_client = secretmanager.SecretManagerServiceClient()
            print("✅ [MART] Successfully initialized Secret Manager client.")
            logging.info("✅ [MART] Successfully initialized Secret Manager client.")
        except Exception as e:
            print(f"❌ [MART] Failed to initialize Secret Manager client due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Secret Manager client due to {e}.")
            raise

        # 2.2.4. Initialize Google Sheets client
        try:
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds, _ = default(scopes=scopes)
            gspread_client = gspread.Client(auth=creds)
            gspread_client.session = AuthorizedSession(creds)
            print(f"✅ [MART] Successfully initialized Google Sheets client with scope {scopes}.")
            logging.info(f"✅ [MART] Successfully initialized Google Sheets client with scope {scopes}.")
        except Exception as e:
            print(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.")
            raise

        # 2.2.5. Query supplier metadata from Google Sheets
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_budget_sheet_id_supplier"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(name=secret_name)
        sheet_id_supplier = response.payload.data.decode("UTF-8")
        worksheet = gspread_client.open_by_key(sheet_id_supplier).worksheet("supplier")
        records = worksheet.get_all_records()
        df_supplier = pd.DataFrame(records)

        if "supplier_name" not in df_supplier.columns:
            raise RuntimeError("❌ [MART] Missing 'supplier_name' column in supplier sheet.")

        temp_table_id = f"{PROJECT}.{mart_dataset}.temp_supplier_{uuid.uuid4().hex[:8]}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        bigquery_client.load_table_from_dataframe(df_supplier[["supplier_name"]], temp_table_id, job_config=job_config).result()
        print(f"✅ [MART] Temp supplier table {temp_table_id} created with {len(df_supplier)} row(s).")

        # 2.2.6. Query staging table to build materialized creative table
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_creative_supplier}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            WITH base AS (
                SELECT
                    a.*,
                    s.supplier_name AS supplier_name
                FROM `{staging_table_creative}` a
                LEFT JOIN `{temp_table_id}` s
                  ON REGEXP_CONTAINS(a.chuong_trinh, s.supplier_name)
                WHERE a.ma_ngan_sach_cap_1 = 'NC'
                  AND a.date IS NOT NULL
            )
            SELECT
                SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,
                SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                SAFE_CAST(adset_name AS STRING) AS adset_name,
                SAFE_CAST(ad_name AS STRING) AS ad_name,
                SAFE_CAST(thumbnail_url AS STRING) AS thumbnail_url,
                SAFE_CAST(vi_tri AS STRING) AS vi_tri,
                SAFE_CAST(doi_tuong AS STRING) AS doi_tuong,
                SAFE_CAST(dinh_dang AS STRING) AS dinh_dang,
                SAFE_CAST(supplier_name AS STRING) AS supplier_name,
                CAST(date AS DATE) AS ngay,
                SAFE_CAST(spend AS FLOAT64) AS spend,
                SAFE_CAST(result AS INT64) AS result,
                SAFE_CAST(result_type AS STRING) AS result_type,
                SAFE_CAST(purchase AS INT64) AS purchase,
                SAFE_CAST(messaging_conversations_started AS INT64) AS messaging_conversations_started,
                SAFE_CAST(reach AS INT64) AS reach,
                SAFE_CAST(impressions AS INT64) AS impressions,
                SAFE_CAST(clicks AS INT64) AS clicks,
                CASE
                    WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                    WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                    ELSE "❓"
                END AS trang_thai
            FROM base
        """
        bigquery_client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_creative_supplier}`"
        row_count = list(bigquery_client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created {mart_table_creative_supplier} with {row_count} row(s) for Facebook creative performance (Supplier).")
        logging.info(f"✅ [MART] Successfully created {mart_table_creative_supplier} with {row_count} row(s) for Facebook creative performance (Supplier).")

    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook creative performance (Supplier) due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook creative performance (Supplier) due to {e}.")
    finally:
        try:
            bigquery_client.delete_table(temp_table_id, not_found_ok=True)
            print(f"🧹 [MART] Temp supplier table {temp_table_id} deleted.")
            logging.info(f"🧹 [MART] Temp supplier table {temp_table_id} deleted.")
        except Exception as cleanup_error:
            print(f"⚠️ [MART] Failed to delete temp table {temp_table_id} due to {cleanup_error}.")
            logging.warning(f"⚠️ [MART] Failed to delete temp table {temp_table_id} due to {cleanup_error}.")

# 2.3. Build materialized table for Facebook festival creative performance by union all staging tables
def mart_creative_festival() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance (Festival only)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance (Festival only)...")

    # 2.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        print(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Festival creative performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Festival creative performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_festival_creative_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Festival creative performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Festival creative performance...")

    # 2.3.2. Query all staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_creative}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            SELECT
                SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,
                SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                SAFE_CAST(adset_name AS STRING) AS adset_name,
                SAFE_CAST(ad_name AS STRING) AS ad_name,
                SAFE_CAST(thumbnail_url AS STRING) AS thumbnail_url,
                SAFE_CAST(vi_tri AS STRING) AS vi_tri,
                SAFE_CAST(doi_tuong AS STRING) AS doi_tuong,
                SAFE_CAST(dinh_dang AS STRING) AS dinh_dang,
                CAST(date AS DATE) AS ngay,
                SAFE_CAST(spend AS FLOAT64) AS spend,
                SAFE_CAST(result AS INT64) AS result,
                SAFE_CAST(result_type AS STRING) AS result_type,
                SAFE_CAST(purchase AS INT64) AS purchase,
                SAFE_CAST(messaging_conversations_started AS INT64) AS messaging_conversations_started,
                SAFE_CAST(reach AS INT64) AS reach,
                SAFE_CAST(impressions AS INT64) AS impressions,
                SAFE_CAST(clicks AS INT64) AS clicks,
                CASE
                    WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                    WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                    ELSE "❓"
                END AS trang_thai
            FROM `{staging_table}`
            WHERE nganh_hang = 'FTV'
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_creative}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_creative} with {row_count} row(s) for Festival creative performance.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_creative} with {row_count} row(s) for Festival creative performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Festival creative performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Festival creative performance due to {e}.")