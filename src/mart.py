"""
==================================================================
FACEBOOK MATERIALIZATION MODULE
------------------------------------------------------------------
This module materializes the final layer for Facebook Ads by 
aggregating and transforming data sourced from staging tables 
produced during the raw data ingestion process.

It serves as the final transformation stage, consolidating daily 
performance and cost metrics into analytics-ready BigQuery tables 
optimized for reporting, dashboarding, and business analysis.

✔️ Dynamically identifies all available Facebook Ads staging tables  
✔️ Applies data transformation, standardization, and type enforcement  
✔️ Performs daily-level aggregation of campaign performance metrics  
✔️ Creates partitioned and clustered MART tables in Google BigQuery  
✔️ Ensures consistency and traceability across the data pipeline  

⚠️ This module is exclusively responsible for materialized layer  
construction*. It does not perform data ingestion, API fetching, 
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

# Add Python Pandas libraries for integraton
import pandas as pd

# Add Python UUID ultilities for integration
import uuid

# Add Google API Core modules for integration
from google.api_core.exceptions import (
    Forbidden,
    GoogleAPICallError,
)

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    GoogleAPICallError,
    NotFound,
    PermissionDenied, 
)

# Add Google Authentication modules for integration
from google.auth import default
from google.auth.exceptions import (
    DefaultCredentialsError,
    RefreshError
)
from google.auth.transport.requests import AuthorizedSession

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add Google Spreadsheets API modules for integration
import gspread

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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR CAMPAIGN PERFORMANCE

# 1.1. Build materialzed table for Facebook campaign performance by union all staging table(s)
def mart_campaign_all() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook Ads campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table Facebook Ads campaign performance...")

    # 1.1.1. Start timing the Facebook Ads campaign performance materialized table building process
    start_time = time.time()
    print(f"🔍 [MART] Proceeding to build materialzed table for Facebook Ads campaign performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [MART] Proceeding to build materialzed table for Facebook Ads campaign performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Initialize Google BigQuery client
    try:
        print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
    except Forbidden as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to permission denial.") from e
    except GoogleAPICallError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to API call error.") from e
    except Exception as e:
        raise RuntimeError(f"❌ [MART] Failed to initialize Google BigQuery client due to {e}.") from e
    
    # 1.1.3. Prepare table_id for Facebook Ads campaign performance
    staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
    staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
    print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook Ads campaign performance...")
    logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook Ads campaign performance...")
    mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
    mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_performance"
    print(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Facebook Ads campaign performance...")
    logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Facebook Ads campaign performance...")

    try:

    # 1.1.4. Query all staging table(s) for Facebook Ads campaign performane
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_all}`
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
        try:
            print(f"🔍 [MART] Creating materialized table {mart_table_all} for Facebook Ads campaign performance...")
            logging.info(f"🔍 [MART] Creating materialized table {mart_table_all} for Facebook Ads campaign performance...")
            google_bigquery_client.query(query).result()
            count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_all}`"
            row_count = list(google_bigquery_client.query(count_query).result())[0]["row_count"]
            print(f"✅ [MART] Successfully created materialized table {mart_table_all} with {row_count} row(s) for Facebook Ads campaign performance.")
            logging.info(f"✅ [MART] Successfully created materialized table {mart_table_all} with {row_count} row(s) for Facebook Ads campaign performance.")
        except Exception as e:
            print(f"❌ [MART] Failed to create materialized table for Facebook Ads campaign performance due to {e}.")
            logging.error(f"❌ [MART] Failed to create materialized table for Facebook Ads campaign performance due to {e}.")

    # 1.1.5. Summarize ingestion result(s)
        print(f"🏆 [MART] Successfully completed Facebook Ads campaign performance materialization with {row_count} row(s).")
        logging.info(f"🏆 [MART] Successfully completed Facebook Ads campaign performance materialization with {row_count} row(s).")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook Ads campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook Ads campaign performance due to {e}.")

# 1.2. Build materialzed table for Facebook supplier campaign performance by union all staging table(s)
def mart_campaign_supplier() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook Ads supplier campaign performance...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook Ads supplier campaign performance...")

    # 1.2.1. Start timing the Facebook Ads supplier campaign performance materialized table building process
    start_time = time.time()
    mart_section_succeeded = {}
    mart_section_failed = [] 
    print(f"🔍 [MART] Proceeding to build materialzed table for Facebook Ads supplier campaign performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [MART] Proceeding to build materialzed table for Facebook Ads supplier campaign performance at {time.strftime('%Y-%m-%d %H:%M:%S')}...")    

    # 1.2.2 Initialize Google Secret Manager client
    try:
        print(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        google_secret_client = secretmanager.SecretManagerServiceClient()
        print(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        logging.info(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        mart_section_succeeded["1.2.2 Initialize Google Secret Manager client"] = True
    except Exception as e:
        mart_section_succeeded["1.2.2 Initialize Google Secret Manager client"] = False
        mart_section_failed.append("1.2.2 Initialize Google Secret Manager client")
        raise RuntimeError(f"❌ [MART] Failed to initialize Google Secret Manager client due to {e}.") from e

    # 1.2.3. Get Google Sheets sheet_id containing suplier name list from Google Secret Manager
    try:
        print(f"🔍 [MART] Retrieving Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance from Google Secret Manager...")
        logging.info(f"🔍 [MART] Retrieving Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance from Google Secret Manager...")        
        supplier_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_budget_sheet_id_supplier"
        supplier_secret_name = f"projects/{PROJECT}/secrets/{supplier_secret_id}/versions/latest"
        supplier_secret_response = google_secret_client.access_secret_version(name=supplier_secret_name)
        supplier_sheet_id = supplier_secret_response.payload.data.decode("UTF-8")
        print(f"✅ [MART] Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance from Google Secret Manager.")
        logging.info(f"✅ [MART] Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance from Google Secret Manager.")
        mart_section_succeeded["1.2.3. Get Google Sheets sheet_id containing suplier name list from Google Secret Manager"] = True
    except Exception as e:
        mart_section_succeeded["1.2.3. Get Google Sheets sheet_id containing suplier name list from Google Secret Manager"] = False
        mart_section_failed.append("1.2.3. Get Google Sheets sheet_id containing suplier name list from Google Secret Manager")
        print(f"❌ [MART] Failed to retrieve Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to retrieve Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance due to {e}.")
        raise RuntimeError(f"❌ [MART] Failed to retrieve Google Sheets sheet_id containing suplier name list for Facebook Ads campaign performance due to {e}.")

    # 1.2.4 Initialize Google Sheets client
    try:
        print(f"🔍 [MART] Initializing Google Sheets client for read-only access...")
        logging.info(f"🔍 [MART] Initializing Google Sheets client for read-only access...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds, _ = default(scopes=scopes)
        google_gspread_client = gspread.Client(auth=creds)
        google_gspread_client.session = AuthorizedSession(creds)
        print(f"✅ [MART] Successfully initialized Google Sheets client with scopes {scopes}.")
        logging.info(f"✅ [MART] Successfully initialized Google Sheets client with scopes {scopes}.")
        mart_section_succeeded["1.2.4 Initialize Google Sheets client"] = True
    except Exception as e:
        mart_section_succeeded["1.2.4 Initialize Google Sheets client"] = False
        mart_section_failed.append("1.2.4 Initialize Google Sheets clientr")
        print(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.")
        logging.error(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.")        
        raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.") from e

    # 1.2.5. Get supplier name list from Google Sheets
    try:       
        print(f"🔍 [MART] Retrieving suplier name list for Facebook Ads campaign performance from Google Sheets...")
        logging.info(f"🔍 [MART] Retrieving suplier name list for Facebook Ads campaign performance from Google Sheets...")         
        supplier_worksheet_id = google_gspread_client.open_by_key(supplier_sheet_id).worksheet("supplier")
        suplier_records_fetched = supplier_worksheet_id.get_all_records()
        mart_df_supplier = pd.DataFrame(suplier_records_fetched)  
        _ = mart_df_supplier["supplier_name"]   
        print(f"✅ [MART] Successfully retrieved suplier name list for Facebook Ads campaign performance from Google Sheets.")
        logging.info(f"✅ [MART] Successfully retrieved suplier name list for Facebook Ads campaign performance from Google Sheets.")
        mart_section_succeeded["1.2.5. Get supplier name list from Google Sheets"] = True
    except Exception as e:
        mart_section_succeeded["1.2.4 Initialize Google Sheets client"] = False
        mart_section_failed.append("1.2.4 Initialize Google Sheets client")
        print(f"❌ [MART] Failed to retrieve suplier name list for Facebook Ads campaign performance from Google Sheets due to {e}.")
        logging.error(f"❌ [MART] Failed to retrieve suplier name list for Facebook Ads campaign performance from Google Sheets due to {e}.")
        raise RuntimeError(f"❌ [MART] Failed to retrieve suplier name list for Facebook Ads campaign performance from Google Sheets due to {e}.")
    
    # 1.2.6. Prepare table_id for Facebook Ads supplier campaign performance
    staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
    staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
    mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
    mart_table_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_campaign_performance"
    print(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier metadata...")
    logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} with supplier metadata...")

    # 1.2.7. Initialize Google BigQuery client
    try:
        print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        mart_section_succeeded["1.2.7. Initialize Google BigQuery client"] = True
    except Exception as e:
        mart_section_succeeded["1.2.7. Initialize Google BigQuery client"] = False
        mart_section_failed.append("1.2.7. Initialize Google BigQuery client")
        raise RuntimeError(f"❌ [MART] Failed to initialize Google BigQuery client due to {e}.") from e

    # 1.2.8. Query supplier metadata for Facebook Ads campaign performance from Google Sheets
    try: 
        temp_table_id = f"{PROJECT}.{mart_dataset}.temp_supplier_{uuid.uuid4().hex[:8]}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")            
        print(f"🔍 [MART] Creating supplier metadata temporary table {temp_table_id} with {len(mart_df_supplier)} row(s) for Facebook Ads campaign performance materialization...")
        logging.info(f"🔍 [MART] Creating supplier metadata temporary table {temp_table_id} with {len(mart_df_supplier)} row(s) for Facebook Ads campaign performance materialization...")
        google_bigquery_client.load_table_from_dataframe(mart_df_supplier[["supplier_name"]], temp_table_id, job_config=job_config).result()
        print(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} for Facebook Ads campaign performance materialization with {len(mart_df_supplier)} row(s).")
        logging.info(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} for Facebook Ads campaign performance materialization with {len(mart_df_supplier)} row(s).")
        mart_section_succeeded["1.2.8. Query supplier metadata for Facebook Ads campaign performance from Google Sheets"] = True
    except Exception as e:
        mart_section_succeeded["1.2.7. Initialize Google BigQuery client"] = False
        mart_section_failed.append("1.2.7. Initialize Google BigQuery client")        
        print(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.")
        logging.error(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.")
        raise RuntimeError(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.") from e
    
    # 1.2.9. Query staging table to build materialized table for Facebook Ads supplier campaign performance
    try: 
        print(f"🔄 [MART] Querying staging Facebook campaign insights table {staging_table_campaign} to build materialized table for supplier campaign performance...")
        logging.info(f"🔄 [MART] Querying staging Facebook campaign insights table {staging_table_campaign} to build materialized table for supplier campaign performance...")
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_supplier}`
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
        google_bigquery_client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_supplier}`"
        row_count = list(google_bigquery_client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully built materialized table {mart_table_supplier} with {row_count} row(s) for Facebook supplier campaign performance.")
        logging.info(f"✅ [MART] Successfully built materialized table {mart_table_supplier} with {row_count} row(s) for Facebook supplier campaign performance.")
        mart_section_succeeded["1.2.9. Query staging table to build materialized table for Facebook Ads supplier campaign performance"] = True
    except Exception as e:
        mart_section_succeeded["1.2.7. Initialize Google BigQuery client"] = False
        mart_section_failed.append("1.2.7. Initialize Google BigQuery client")   
        print(f"❌ [MART] Failed to build materialized table for Facebook supplier campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook supplier campaign performance due to {e}.")

    # 1.2.10. Delete temporary supplier metadata table after Facebook Ads campaign materialization
        try:
            print(f"🔄 [MART] Facebook supplier campaign performance process is completed then supplier metadata temporary table deletion will be proceeding...")
            logging.info(f"🔄 [MART] Facebook supplier campaign performance process is completed then supplier metadata temporary table deletion will be proceeding...")
            google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
            print(f"✅ [MART] Successfully deleted supplier metadata temporary table {temp_table_id}.")
            logging.info(f"✅ [MART] Successfully deleted supplier metadata temporary table {temp_table_id}.")
            mart_section_succeeded["1.2.10. Delete temporary supplier metadata table after Facebook Ads campaign materialization"] = True
        except Exception as cleanup_error:
            mart_section_succeeded["1.2.10. Delete temporary supplier metadata table after Facebook Ads campaign materialization"] = False
            mart_section_failed.append("1.2.10. Delete temporary supplier metadata table after Facebook Ads campaign materialization")
            print(f"❌ [MART] Failed to delete supplier metadata temporary table {temp_table_id} due to {cleanup_error}.")
            logging.warning(f"❌ [MART] Failed to delete supplier metadata temporary table {temp_table_id} due to {cleanup_error}.")

    # 1.2.11. Summarize ingestion result(s)
    elapsed = round(time.time() - start_time, 2)
    if mart_section_failed:
        print(f"❌ [MART] Failed to completed Facebook Ads supplier campaign performance materialization due to unsuccesfull section(s) {', '.join(mart_section_failed)}.")
        logging.error(f"❌ [MART] Failed to completed Facebook Ads supplier campaign performance materialization due to unsuccesfull section(s) {', '.join(mart_section_failed)}.")
        mart_status_def = "failed"
    else:
        print(f"🏆 [MART] Successfully completed Facebook Ads supplier campaign performance materialization in {elapsed}s.")
        logging.info(f"🏆 [MART] Successfully completed Facebook Ads supplier campaign performance materialization in {elapsed}s.")
        mart_status_def = "success"
    return {"status": mart_status_def, "elapsed_seconds": elapsed, "failed_sections": mart_section_failed}

# 1.3. Build materialzed table for Facebook festival campaign performance by union all staging tables
def mart_campaign_festival() -> None:
    print(f"🚀 [MART] Starting to build materialized table for Facebook festival campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table for Facebook festival campaign performance...")

    # 1.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook festival campaign performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for Facebook festival campaign performance...")
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
        print(f"✅ [MART] Successfully created materialized table {mart_table_performance} with {row_count} row(s) for Facebook festival campaign performance.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_performance} with {row_count} row(s) for Facebook festival campaign performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook festival campaign performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook festival campaign performance due to {e}.")

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

    # 2.2.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_ad = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_creative_performance"
        print(f"🔍 [MART] Using staging table {staging_table_ad} with supplier metadata...")
        logging.info(f"🔍 [MART] Using staging table {staging_table_ad} with supplier metadata...")

    # 2.2.2. Initialize Google BigQuery client
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud project {PROJECT}...")
            bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to your credentials.") from e

    # 2.2.3. Initialize Google Secret Manager client
        try:
            print(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud project {PROJECT}...")
            secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except Exception as e:
            print(f"❌ [MART] Failed to initialize Google Secret Manager client due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google Secret Manager client due to {e}.")
            raise

    # 2.2.4. Initialize Google Sheets client
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
        try:
            print(f"🔍 [MART] Creating supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            logging.info(f"🔍 [MART] Creating supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            bigquery_client.load_table_from_dataframe(df_supplier[["supplier_name"]], temp_table_id, job_config=job_config).result()
            print(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            logging.info(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
        except Exception as e:
            print(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.")
            logging.error(f"❌ [MART] Failed to create supplier metadata temporary table {temp_table_id} due to {e}.")

    # 2.2.6. Query staging table to build materialized table for supplier
        print(f"🔄 [MART] Querying staging Facebook ad insights table {staging_table_ad} to build materialized table for supplier creative performance...")
        logging.info(f"🔄 [MART] Querying staging Facebook ad insights table {staging_table_ad} to build materialized table for supplier creative performance...")
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_creative_supplier}`
            PARTITION BY ngay
            CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, chuong_trinh
            AS
            WITH base AS (
                SELECT
                    a.*,
                    s.supplier_name AS supplier_name
                FROM `{staging_table_ad}` a
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
        print(f"✅ [MART] Successfully built materialized table {mart_table_creative_supplier} with {row_count} row(s) for Facebook supplier creative performance.")
        logging.info(f"✅ [MART] Successfully built materialized table {mart_table_creative_supplier} with {row_count} row(s) for Facebook supplier creative performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook supplier creative performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook supplier creative performance due to {e}.")
    finally:
        try:
            print(f"🔄 [MART] Facebook supplier creative performance process is completed then supplier metadata temporary table deletion will be proceeding...")
            logging.info(f"🔄 [MART] Facebook supplier creative performance process is completed then supplier metadata temporary table deletion will be proceeding...")
            bigquery_client.delete_table(temp_table_id, not_found_ok=True)
            print(f"✅ [MART] Successfully deleted supplier metadata temporary table {temp_table_id}.")
            logging.info(f"✅ [MART] Successfully deleted supplier metadata temporary table {temp_table_id}.")
        except Exception as cleanup_error:
            print(f"❌ [MART] Failed to delete supplier metadata temporary table {temp_table_id} due to {cleanup_error}.")
            logging.warning(f"❌ [MART] Failed to delete supplier metadata temporary table {temp_table_id} due to {cleanup_error}.")

# 2.3. Build materialized table for Facebook festival creative performance by union all staging tables
def mart_creative_festival() -> None:
    print("🚀 [MART] Starting to build materialized table for Facebook festival creative performance...")
    logging.info("🚀 [MART] Starting to build materialized table for Facebook festival creative performance...")

    # 2.3.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        print(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Facebook festival creative performance...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} to build materialized table for Facebook festival creative performance...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
        mart_table_creative = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_festival_creative_performance"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Facebook festival creative performance...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_creative} for Facebook festival creative performance...")

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
        print(f"✅ [MART] Successfully created materialized table {mart_table_creative} with {row_count} row(s) for Facebook festival creative performance.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_creative} with {row_count} row(s) for Facebook festival creative performance.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook festival creative performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook festival creative performance due to {e}.")