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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR SPENDING FROM STAGING TABLE(S)

# 1.1. Build materialized table for Facebook campaign spending by union all staging tables
def mart_spend_all() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook campaign spending...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook campaign spending...")

    # 1.1.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook campaign spending...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook campaign spending...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_spend = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_spend"    
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_spend} for Facebook campaign spending...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_spend} for Facebook campaign spending...")
    
    # 1.1.2. Query all staging table(s)
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
                FROM `{staging_table_campaign}`
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

# 1.2. Build materialized table for Facebook supplier campaign spending by union all staging tables
def mart_spend_supplier() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook supplier campaign spending...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook supplier campaign spending...")

    # 1.2.1. Prepare table_id
    try:
        raw_supplier_table = f"{PROJECT}.{COMPANY}_dataset_budget_api_raw.{COMPANY}_table_budget_marketing_supplier_supplier_list"
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_spend_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_campaign_spend"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier list {raw_supplier_table}...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier list {raw_supplier_table}...")

        # 1.2.2. Query staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_spend_supplier}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            WITH base AS (
                SELECT
                    SAFE_CAST(a.nhan_su AS STRING) AS nhan_su,
                    SAFE_CAST(a.nganh_hang AS STRING) AS nganh_hang,
                    SAFE_CAST(a.ma_ngan_sach_cap_1 AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(a.khu_vuc AS STRING) AS khu_vuc,
                    SAFE_CAST(a.chuong_trinh AS STRING) AS chuong_trinh,
                    SAFE_CAST(a.noi_dung AS STRING) AS noi_dung,
                    SAFE_CAST(a.nen_tang AS STRING) AS nen_tang,
                    SAFE_CAST(a.hinh_thuc AS STRING) AS hinh_thuc,
                    SAFE_CAST(a.thang AS STRING) AS thang,
                    CAST(a.date AS DATE) AS ngay,
                    SAFE_CAST(a.spend AS FLOAT64) AS spend,
                    LOWER(SAFE_CAST(a.delivery_status AS STRING)) AS delivery_status,
                    s.supplier_name AS supplier_name
                FROM `{staging_table_campaign}` a
                LEFT JOIN `{raw_supplier_table}` s
                  ON REGEXP_CONTAINS(a.chuong_trinh, s.supplier_name)
                WHERE a.date IS NOT NULL
                  AND a.ma_ngan_sach_cap_1 = 'NC'
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
                supplier_name,
                SUM(spend) AS chi_tieu,
                delivery_status AS trang_thai
            FROM base
            GROUP BY
                nhan_su, nganh_hang, ma_ngan_sach_cap_1, khu_vuc, chuong_trinh, noi_dung,
                nen_tang, hinh_thuc, thang, ngay, delivery_status, supplier_name
        """
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_spend_supplier}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_spend_supplier} with {row_count} row(s) for Facebook supplier campaign spending.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_spend_supplier} with {row_count} row(s) for Facebook supplier campaign spending.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook supplier campaign spending due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook supplier campaign spending due to {e}.")

# 1.3. Build materialized table for Facebook festival campaign spending by union all staging tables
def mart_spend_festival() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook festival campaign spending...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook festival campaign spending...")

    # 1.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook festival campaign spending...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook festival campaign spending...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_spend = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_festival_campaign_spend"    
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_spend} for Facebook festival campaign spending...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_spend} for Facebook festival campaign spending...")
    
        # 1.3.2. Query all staging table(s)
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
                FROM `{staging_table_campaign}`
                WHERE date IS NOT NULL
                  AND nganh_hang = 'FTV'
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
        print(f"✅ [MART] Successfully created materialized table {mart_table_spend} with {row_count} row(s) for Facebook festival campaign spending.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_spend} with {row_count} row(s) for Facebook festival campaign spending.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook festival campaign spending due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook festival campaign spending due to {e}.")

# 2. BUILD MONTHLY MATERIALIZED TABLE FOR CAMPAIGN PERFORMANCE FROM STAGING TABLE(S)

# 2.1. Build materialzed table for Facebook campaign performance by union all staging tables
def mart_campaign_all() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook campaign performance...")

    # 2.1.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook campaign performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook campaign performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_performance = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Facebook campaign performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Facebook campaign performance...")

    # 2.1.2. Query all staging table(s)
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

# 2.2. Build materialzed table for Facebook supplier campaign performance by union all staging tables
def mart_campaign_supplier() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook campaign performance (Supplier)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook campaign performance (Supplier)...")

    # 3.1.1. Prepare table_id
    try:
        raw_supplier_table = f"{PROJECT}.{COMPANY}_dataset_budget_api_raw.{COMPANY}_table_budget_marketing_supplier_supplier_list"
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_campaign_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_campaign_performance"

        print(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier list {raw_supplier_table}...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier list {raw_supplier_table}...")

        # 3.1.2. Query staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
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
                LEFT JOIN `{raw_supplier_table}` s
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
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_campaign_supplier}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_campaign_supplier} with {row_count} row(s) for Facebook campaign performance (Supplier).")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_campaign_supplier} with {row_count} row(s) for Facebook campaign performance (Supplier).")

    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook campaign performance (Supplier) due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook campaign performance (Supplier) due to {e}.")

# 2.3. Build materialzed table for Facebook festival campaign performance by union all staging tables
def mart_campaign_festival() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook campaign performance (Festival only)...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook campaign performance (Festival only)...")

    # 2.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Festival campaign performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Festival campaign performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_performance = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_festival_campaign_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Festival campaign performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_performance} for Festival campaign performance...")

    # 2.3.2. Query all staging table(s)
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

# 3. MONTHLY MATERIALIZED TABLE FOR CREATIVE PERFORMANCE FROM STAGING TABLE(S)

# 3.1. Build materialized table for Facebook creative performance by union all staging tables
def mart_creative_all() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance (All)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance (All)...")

    # 3.1.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_creative_performance"
        print(f"🔍 [MART] Using staging table {staging_table} for creative performance (All)...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} for creative performance (All)...")

        # 3.1.2. Query staging table(s)
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

# 3.2. Build materialized table for Facebook supplier creative performance by union all staging tables
def mart_creative_supplier() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance (Supplier)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance (Supplier)...")

    # 3.2.1. Prepare table_id
    try:
        raw_supplier_table = f"{PROJECT}.{COMPANY}_dataset_budget_api_raw.{COMPANY}_table_budget_marketing_supplier_supplier_list"
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"     
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_creative_performance"
        print(f"🔍 [MART] Using staging table {staging_table} with supplier list {raw_supplier_table}...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} with supplier list {raw_supplier_table}...")

        # 3.2.2. Query staging table(s)
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("Cannot initialize BigQuery client. Check your credentials.") from e
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_creative_supplier}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            WITH base AS (
                SELECT
                    a.*,
                    s.supplier_name AS supplier_name
                FROM `{staging_table}` a
                LEFT JOIN `{raw_supplier_table}` s
                  ON REGEXP_CONTAINS(a.chuong_trinh, s.supplier_name)
                WHERE a.ma_ngan_sach_cap_1 = 'NC'
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
                SAFE_CAST(supplier_name AS STRING) AS supplier_name, -- thêm supplier_name
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
        client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_creative_supplier}`"
        row_count = list(client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_creative_supplier} with {row_count} row(s) for Facebook creative performance (Supplier).")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_creative_supplier} with {row_count} row(s) for Facebook creative performance (Supplier).")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook creative performance (Supplier) due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook creative performance (Supplier) due to {e}.")

# 3.3. Build materialized table for Facebook festival creative performance by union all staging tables
def mart_creative_festival() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook creative performance (Festival only)...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook creative performance (Festival only)...")

    # 3.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        print(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Festival creative performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Festival creative performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_festival_creative_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Festival creative performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Festival creative performance...")

    # 3.3.2. Query all staging table(s)
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