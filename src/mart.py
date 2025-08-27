#services/facebook/mart.py
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

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google CLoud libraries for integration
from google.cloud import bigquery

# Get Google Cloud Project ID environment variable
PROJECT = os.getenv("GCP_PROJECT_ID")

# Get Facebook service environment variable for Brand
COMPANY = os.getenv("COMPANY") 

# Get Facebook service environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get Facebook service environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get Facebook service environment variable for Account
LAYER = os.getenv("LAYER")

# 1. TRANSFORM FACEBOOK ADS STAGING DATA INTO MONTHLY MATERIALIZED TABLE FOR SPENDING

# 1.1. Build materialized table for Facebook campaign spending by union all staging tables
def mart_spend_all() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook campaign spending...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook campaign spending...")

    # 1.1.1. Prepare full table_id for raw layer in BigQuery  
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_staging"
        staging_campaign_insights = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_campaign_insights} to build materialized table for Facebook campaign spending...")
        logging.info(f"🔍 [MART] Using staging table {staging_campaign_insights} to build materialized table for Facebook campaign spending...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_mart"
        mart_table_spend = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_spend_all"    
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_spend} for Facebook campaign spending...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_spend} for Facebook campaign spending...")
    
    # 1.1.2. Query all staging tables to build materialized table for Facebook campaign spending
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_spend}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            WITH base AS (
                SELECT
                    SAFE_CAST(nhan_su AS STRING) AS nhan_su,
                    SAFE_CAST(nganh_hang AS STRING) AS nganh_hang,
                    SAFE_CAST(ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(khu_vuc AS STRING) AS khu_vuc,
                    SAFE_CAST(chuong_trinh AS STRING) AS chuong_trinh,
                    SAFE_CAST(noi_dung AS STRING) AS noi_dung,
                    SAFE_CAST(nen_tang AS STRING) AS nen_tang,
                    SAFE_CAST(hinh_thuc AS STRING) AS hinh_thuc,
                    SAFE_CAST(thang AS STRING) AS thang,
                    CAST(date AS DATE) AS ngay,
                    SAFE_CAST(spend AS FLOAT64) AS spend,
                    LOWER(SAFE_CAST(delivery_status AS STRING)) AS delivery_status
                FROM `{staging_campaign_insights}`
                WHERE date IS NOT NULL
            )
            SELECT
                nhan_su,
                ma_ngan_sach_cap_1,
                nganh_hang,
                khu_vuc,
                chuong_trinh,
                noi_dung,
                nen_tang,
                hinh_thuc,
                thang,
                ngay,
                SUM(spend) AS chi_tieu,
                delivery_status AS trang_thai
            FROM base
            GROUP BY
                nhan_su, nganh_hang, ma_ngan_sach_cap_1, khu_vuc, chuong_trinh, noi_dung,
                nen_tang, hinh_thuc, thang, ngay, delivery_status
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_spend}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_spend} with {row_count} row(s) for Facebook campaign spending.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_spend} with {row_count} row(s) for Facebook campaign spending.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook campaign spending due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook campaign spending due to {e}.")

# 2. TRANSFORM FACEBOOK ADS STAGING DATA INTO MONTHLY MATERIALIZED TABLE FOR CAMPAIGN PERFORMANCE

# 2.1. Build materialzed table for Facebook campaign performance by union all staging tables
def mart_campaign_all() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook campaign performance...")

    # 2.2.1. Prepare full table_id for raw layer in BigQuery
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_staging"
        staging_campaign_insights = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_campaign_insights} to build materialized table for Facebook campaign performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_campaign_insights} to build materialized table for Facebook campaign performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_ads_insights_api_mart"
        mart_table_performance = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_all"
        print(f"🔍 [INGEST] Preparing to build materialized table {mart_table_performance} for Facebook campaign performance...")
        logging.info(f"🔍 [INGEST] Preparing to build materialized table {mart_table_performance} for Facebook campaign performance...")

    # 2.2.2. Query all staging tables to build materialized table for Facebook campaign performance
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
            FROM `{staging_table}`
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

# 3. TRANSFORM FACEBOOK ADS STAGING DATA INTO MONTHLY MATERIALIZED TABLE FOR CREATIVE PERFORMANCE

# 3.1. Build materialized table for Facebook creative performance by union all staging tables
def mart_creative_all() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance...")

    # 3.1.1. Prepare full table_id for raw layer in BigQuery
    try:
        staging_dataset = get_facebook_dataset("staging")
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_ad_insights"
        print(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Facebook creative performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Facebook creative performance...")
        mart_dataset = get_facebook_dataset("mart")
        mart_table_creative = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_creative_all"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Facebook creative performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Facebook creative performance...")
    
    # 3.1.2. Query all staging tables to build materialized table for Facebook creative
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
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_creative}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_creative} with {row_count} row(s) for Facebook creative performance.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_creative} with {row_count} row(s) for Facebook creative performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook creative performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook creative performance due to {e}.")