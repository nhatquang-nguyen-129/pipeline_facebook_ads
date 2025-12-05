"""
==================================================================
FACEBOOK MATERIALIZATION MODULE
------------------------------------------------------------------
This module materializes the final layer for Facebook Ads by 
aggregating and transforming data sourced from staging tables 
produced during the raw data ingestion process.

It serves as the final transformation stage, consolidating daily 
performance and cost metrics into Google BigQuery tables optimized
for reporting, dashboarding, and business analysis.

✔️ Dynamically identifies all available Facebook Ads staging tables  
✔️ Applies data transformation, standardization and type enforcement  
✔️ Performs daily-level aggregation of campaign performance metrics  
✔️ Creates partitioned and clustered MART tables in Google BigQuery  
✔️ Ensures consistency and traceability across the data pipeline  

⚠️ This module is exclusively responsible for materialized layer  
construction. It does not perform data ingestion, API fetching, 
or enrichment tasks.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integraton
import logging

# Add Python time ultilities for integration
import time

# Add Google Cloud modules for integration
from google.cloud import bigquery

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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR CAMPAIGN PERFORMANCE

# 1.1. Build materialized table for Facebook campaign performance by union all staging table
def mart_campaign_all() -> dict:
    print(f"🚀 [MART] Starting to build materialized table for Facebook Ads campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook Ads campaign performance...")

    # 1.1.1. Start timing the Facebook Ads campaign materialization
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for Facebook Ads campaign performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for Facebook Ads campaign performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Prepare Google BigQuery table_id for materialization
        try: 
            mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
            mart_section_start = time.time()
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
            print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook Ads campaign performance...")
            logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook Ads campaign performance...")
            mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
            mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_performance"
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Facebook Ads campaign performance...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Facebook Ads campaign performance...")
            mart_sections_status[mart_section_name] = "succeed"    
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)            

    # 1.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            mart_sections_status[mart_section_name] = "succeed"
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
    
    # 1.1.4. Query staging table for materialization
        mart_section_name = "[MART] Query staging table for materialization"
        mart_section_start = time.time()    
        try:
            query = f"""
            CREATE OR REPLACE TABLE `{mart_table_all}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, hang_muc
            AS
            WITH base AS (
                SELECT
                    SAFE_CAST(enrich_account_name AS STRING) AS tai_khoan,
                    SAFE_CAST(enrich_account_department AS STRING) AS phong_ban,
                    SAFE_CAST(enrich_account_platform AS STRING) AS nen_tang,
                    SAFE_CAST(enrich_budget_group AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(enrich_budget_type AS STRING) AS ma_ngan_sach_cap_2,
                    SAFE_CAST(enrich_program_track AS STRING) AS hang_muc,
                    SAFE_CAST(enrich_program_group AS STRING) AS chuong_trinh,
                    SAFE_CAST(enrich_program_type AS STRING) AS noi_dung,
                    SAFE_CAST(enrich_campaign_objective AS STRING) AS hinh_thuc,
                    SAFE_CAST(enrich_campaign_region AS STRING) AS khu_vuc,
                    SAFE_CAST(enrich_campaign_personnel AS STRING) AS nhan_su,
                    SAFE_CAST(enrich_category_group AS STRING) AS nganh_hang,
                    SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                    CAST(date AS DATE) AS ngay,
                    SAFE_CAST(year AS STRING) AS nam,
                    SAFE_CAST(month AS STRING) AS thang,
                    SAFE_CAST(spend AS FLOAT64) AS spend,
                    SAFE_CAST(result AS FLOAT64) AS result,
                    SAFE_CAST(result_type AS STRING) AS result_type,
                    SAFE_CAST(purchase AS FLOAT64) AS purchase,
                    SAFE_CAST(messaging_conversations_started AS FLOAT64) AS message,
                    SAFE_CAST(impressions AS INT64) AS impression,
                    SAFE_CAST(clicks AS INT64) AS click,
                    CASE
                        WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                        WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                        ELSE "❓ Unrecognized"
                    END AS trang_thai,
                    SAFE_CAST(last_updated_at AS TIMESTAMP) AS last_updated_at
                FROM `{staging_table_campaign}`
                WHERE date IS NOT NULL
            )
            SELECT *
            FROM base;
            """
            print(f"🔄 [MART] Querying staging Facebook Ads campaign insights table {staging_table_campaign} to create or replace materialized table for campaign performance...")
            logging.info(f"🔄 [MART] Querying staging Facebook Ads campaign insights table {staging_table_campaign} to create or replace materialized table for campaign performance...")
            google_bigquery_client.query(query).result()
            count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_all}`"
            row_count = list(google_bigquery_client.query(count_query).result())[0]["row_count"]
            print(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for Facebook Ads campaign performance with {row_count} row(s).")
            logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for Facebook Ads campaign performance with {row_count} row(s).")
            mart_sections_status[mart_section_name] = "succeed"
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for Facebook Ads campaign performance due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for Facebook Ads campaign performance due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.5. Summarize materialization results for Facebook Ads campaign performance
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }       
        if len(mart_sections_failed) > 0:
            print(f"❌ [MART] Failed to complete Facebook Ads campaign performance materialization due to unsuccessful section(s) {', '.join(mart_sections_failed)}.")
            logging.error(f"❌ [MART] Failed to complete Facebook Ads campaign performance materialization due to unsuccessful section(s) {', '.join(mart_sections_failed)}.")
            mart_status_final = "mart_failed_all"
        else:
            print(f"🏆 [MART] Successfully completed Facebook Ads campaign performance materialization in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed Facebook Ads campaign performance materialization in {mart_time_elapsed}s.")
            mart_status_final = "mart_succeed_all"
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
            },
        }
    return mart_results_final

# 2. BUILD MONTHLY MATERIALIZED TABLE FOR CREATIVE PERFORMANCE FROM STAGING TABLES

# 2.1. Build materialized table for Facebook creative performance by union all staging tables
def mart_creative_all() -> dict:
    print(f"🚀 [MART] Starting to build materialized table for Facebook Ads creative performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table for Facebook Ads creative performance...")

    # 2.1.1. Start timing the Facebook Ads creative performance materialization
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for Facebook Ads creative performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for Facebook Ads creative performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 2.1.2. Prepare Google BigQuery table_id for materialization
        try:
            mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
            mart_section_start = time.time()    
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_ad = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
            print(f"🔍 [MART] Using staging table {staging_table_ad} to build materialized table for Facebook Ads creative performance...")
            logging.info(f"🔍 [MART] Using staging table {staging_table_ad} to build materialized table for Facebook Ads creative performance...")
            mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
            mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_creative_performance"
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Facebook Ads creative performance...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Facebook Ads creative performance...") 
            mart_sections_status[mart_section_name] = "succeed"    
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)              

    # 2.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()    
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            mart_sections_status[mart_section_name] = "succeed"
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.4. Query staging table for materialization
        mart_section_name = "[MART] Query staging table for materialization"
        mart_section_start = time.time()        
        try:
            query = f"""
                CREATE OR REPLACE TABLE `{mart_table_all}`
                PARTITION BY ngay
                CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, hang_muc
                AS
                SELECT
                    SAFE_CAST(enrich_account_name AS STRING) AS tai_khoan,
                    SAFE_CAST(enrich_account_department AS STRING) AS phong_ban,
                    SAFE_CAST(enrich_account_platform AS STRING) AS nen_tang,
                    SAFE_CAST(enrich_budget_group AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(enrich_budget_type AS STRING) AS ma_ngan_sach_cap_2,
                    SAFE_CAST(enrich_program_track AS STRING) AS hang_muc,
                    SAFE_CAST(enrich_program_group AS STRING) AS chuong_trinh,
                    SAFE_CAST(enrich_program_type AS STRING) AS noi_dung,
                    SAFE_CAST(enrich_campaign_objective AS STRING) AS hinh_thuc,
                    SAFE_CAST(enrich_campaign_region AS STRING) AS khu_vuc,
                    SAFE_CAST(enrich_campaign_personnel AS STRING) AS nhan_su,
                    SAFE_CAST(enrich_category_group AS STRING) AS nganh_hang,
                    SAFE_CAST(enrich_adset_location AS STRING) AS vi_tri,
                    SAFE_CAST(enrich_adset_audience AS STRING) AS doi_tuong,
                    SAFE_CAST(enrich_adset_format AS STRING) AS dinh_dang,
                    SAFE_CAST(enrich_adset_strategy AS STRING) AS hoat_dong,
                    SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                    SAFE_CAST(adset_name AS STRING) AS adset_name,
                    SAFE_CAST(ad_name AS STRING) AS ad_name,
                    SAFE_CAST(thumbnail_url AS STRING) AS thumbnail_url,
                    CAST(date AS DATE) AS ngay,
                    SAFE_CAST(spend AS FLOAT64) AS spend,
                    SAFE_CAST(result AS INT64) AS result,   
                    SAFE_CAST(result_type AS STRING) AS result_type,
                    SAFE_CAST(purchase AS INT64) AS purchase,
                    SAFE_CAST(messaging_conversations_started AS INT64) AS message,
                    SAFE_CAST(impressions AS INT64) AS impressions,
                    SAFE_CAST(clicks AS INT64) AS clicks,
                    CASE
                        WHEN REGEXP_CONTAINS(delivery_status, r"ACTIVE") THEN "🟢"
                        WHEN REGEXP_CONTAINS(delivery_status, r"PAUSED") THEN "⚪"
                        ELSE "❓"
                    END AS trang_thai
                FROM `{staging_table_ad}`
            """
            print(f"🔄 [MART] Querying staging Facebook Ads ad insights table {staging_table_ad} to create or replace materialized table for creative performance...")
            logging.info(f"🔄 [MART] Querying staging Facebook Ads ad insights table {staging_table_ad} to create or replace materialized table for creative performance...")       
            google_bigquery_client.query(query).result()
            count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_all}`"
            row_count = list(google_bigquery_client.query(count_query).result())[0]["row_count"]
            print(f"✅ [MART] Successfully created or replaced materialized table {mart_table_all} for Facebook Ads creative performance with {row_count} row(s).")
            logging.info(f"✅ [MART] Successfully created or replaced materialized table {mart_table_all} for Facebook Ads creative performance with {row_count} row(s).")
            mart_sections_status[mart_section_name] = "succeed"
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for Facebook Ads creative performance due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for Facebook Ads creative performance due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.5. Summarize materialization results for Facebook Ads creative performance
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }          
        if len(mart_sections_failed) > 0:
            print(f"❌ [MART] Failed to complete Facebook Ads creative performance materialization due to unsuccessful section(s) {', '.join(mart_sections_failed)}.")
            logging.error(f"❌ [MART] Failed to complete Facebook Ads creative performance materialization due to unsuccessful section(s) {', '.join(mart_sections_failed)}.")
            mart_status_final = "mart_failed_all"
        else:
            print(f"🏆 [MART] Successfully completed Facebook Ads creative performance materialization in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed Facebook Ads creative performance materialization in {mart_time_elapsed}s.")
            mart_status_final = "mart_succeed_all"
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
            },
        }
    return mart_results_final