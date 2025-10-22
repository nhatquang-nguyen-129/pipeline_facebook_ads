"""
===================================================================
FACEBOOK SCHEMA MODULE
-------------------------------------------------------------------
This module defines and manages **schema-related logic** for the  
Facebook Ads data pipeline, acting as a single source of truth  
for all required fields across different data layers.

It plays a key role in ensuring schema alignment and preventing  
data inconsistency between raw → staging → mart layers.

✔️ Declares expected column names for each entity type   
✔️ Supports schema enforcement in validation and ETL stages  
✔️ Prevents schema mismatch when handling dynamic Facebook API fields  

⚠️ This module does *not* fetch or transform data.  
It only provides schema utilities to support other pipeline components.
===================================================================
"""

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add external Python Pandas libraries for integration
import pandas as pd

# Add external Python NumPy libraries for integration
import numpy as np

# 1. ENSURE SCHEMA FOR GIVEN PYTHON DATAFRAME

# 1.1. Ensure that the given DataFrame contains all required columns with correct datatypes
def ensure_table_schema(schema_df_input: pd.DataFrame, schema_type_mapping: str) -> pd.DataFrame:

    # 1.1.1. Start timing the raw Facebook Ads campaign insights enrichment process
    schema_time_start = time.time()
    schema_sections_status = {}
    print(f"🔍 [SCHEMA] Proceeding to ensure schema for Facebook Ads with {len(schema_df_input)} given row(s) for mapping type {schema_type_mapping} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [SCHEMA] Proceeding to ensure schema for Facebook Ads with {len(schema_df_input)} given row(s) for mapping type {schema_type_mapping} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Define schema mapping for Facebook data type
    schema_types_mapping = {
        "fetch_campaign_metadata": {
            "campaign_id": str,
            "campaign_name": str,
            "status": str,
            "effective_status": str,
            "objective": str,
            "configured_status": str,
            "buying_type": str,
            "account_id": str,
            "account_name": str,
        },
        "fetch_adset_metadata": {
            "adset_id": str,
            "adset_name": str,
            "campaign_id": str,
            "status": str,
            "effective_status": str,
            "account_id": str,
            "account_name": str,
        },
        "fetch_ad_metadata": {
            "ad_id": str,
            "ad_name": str,
            "adset_id": str,
            "campaign_id": str,
            "status": str,
            "effective_status": str,
            "account_id": str,
            "account_name": str,
        },
        "fetch_ad_creative": {
            "ad_id": str,
            "thumbnail_url": str,
            "account_id": str,
        },
        "fetch_campaign_insights": {
            "account_id": str,
            "campaign_id": str,
            "optimization_goal": str,
            "spend": str,
            "reach": str,
            "impressions": str,
            "clicks": str,
            "date_start": str,
            "date_stop": str,
            "actions": object,
        },
        "fetch_ad_insights": {
            "account_id": str,
            "ad_id": str,
            "adset_id": str,
            "campaign_id": str,
            "spend": str,
            "reach": str,
            "impressions": str,
            "clicks": str,
            "optimization_goal": str,
            "date_start": str,
            "date_stop": str,
            "delivery_status": str,
            "actions": object
        },
        "ingest_campaign_metadata": {
            "campaign_id": str,
            "campaign_name": str,
            "effective_status": str,
            "account_id": str,
            "account_name": str,
        },
        "ingest_adset_metadata": {
            "adset_id": str,
            "adset_name": str,
            "campaign_id": str,
            "status": str,
            "effective_status": str,
            "account_id": str,
            "account_name": str,
        },
        "ingest_ad_metadata": {
            "ad_id": str,
            "ad_name": str,
            "adset_id": str,
            "campaign_id": str,
            "status": str,
            "effective_status": str,
            "account_id": str,
            "account_name": str,
        },
        "ingest_ad_creative": {
            "ad_id": str,
            "thumbnail_url": str,
            "account_id": str,
        },
        "ingest_campaign_insights": {
            "account_id": str,
            "campaign_id": str,
            "optimization_goal": str,
            "spend": float,
            "reach": int,
            "impressions": int,
            "clicks": int,
            "result": float,
            "result_type": str,
            "purchase": float,
            "messaging_conversations_started": float,
            "date_range": str,
            "date_start": str,
            "date_stop": str,
            "actions": str,
            "last_updated_at": "datetime64[ns, UTC]"
        },
        "ingest_ad_insights": {
            "account_id": str,
            "campaign_id": str,
            "adset_id": str,
            "ad_id": str,
            "spend": float,
            "reach": int,
            "impressions": int,
            "clicks": int,
            "optimization_goal": str,
            "result": float,
            "result_type": str,
            "purchase": float,
            "messaging_conversations_started": float,
            "date_range": str,
            "date_start": str,
            "date_stop": str,
            "actions": str,
            "last_updated_at": "datetime64[ns, UTC]"
        },
        "staging_campaign_insights": {
            "account_id": str,
            "campaign_id": str,
            "campaign_name": str,
            "account_name": str,
            "date_start": str,
            "date_stop": str,
            "delivery_status": str,
            "spend": float,
            "reach": int,
            "impressions": int,
            "clicks": int,
            "result": float,
            "result_type": str,
            "purchase": float,
            "messaging_conversations_started": float,
            "hinh_thuc": str,
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nhan_su": str,
            "nganh_hang": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "nen_tang": str,
            "phong_ban": str,
            "tai_khoan": str,
            "date": "datetime64[ns, UTC]"
        },
        "staging_ad_insights": {
            "ad_id": str,
            "ad_name": str,
            "campaign_id": str,
            "adset_id": str,
            "adset_name": str,
            "campaign_name": str,
            "date_start": str,
            "date_stop": str,
            "spend": float,
            "delivery_status": str,
            "hinh_thuc": str,
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nhan_su": str,
            "nganh_hang": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "campaign_name_invalid": bool,
            "vi_tri": str,
            "doi_tuong": str,
            "dinh_dang": str,
            "adset_name_invalid": bool,
            "nen_tang": str,
            "phong_ban": str,
            "tai_khoan": str,
            "thumbnail_url": str,
            "result": int,
            "result_type": str,
            "purchase": int,
            "reach": int,
            "impressions": int,
            "clicks": int,
            "messaging_conversations_started": int,
            "date": "datetime64[ns, UTC]",
        },
    }
    
    try:

    # 1.1.3. Validate that the given schema_type_mapping exists
        if schema_type_mapping not in schema_types_mapping:
            schema_sections_status["1.1.3. Validate that the given schema_type_mapping exists"] = "failed"
            print(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for Facebook Ads then enforcement is suspended.")
            logging.error(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for Facebook Ads then enforcement is suspended.")
            raise ValueError(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for Facebook Ads then enforcement is suspended.")
        else:
            schema_columns_expected = schema_types_mapping[schema_type_mapping]
            schema_sections_status["1.1.3. Validate that the given schema_type_mapping exists"] = "succeed"
            print(f"✅ [SCHEMA] Successfully validated schema type {schema_type_mapping} for Facebook Ads.")
            logging.info(f"✅ [SCHEMA] Successfully validated schema type {schema_type_mapping} for Facebook Ads.")       
        
    # 1.1.4. Enforce schema columns for Facebook Ads
        try:
            print(f"🔄 [SCHEMA] Enforcing schema for Facebook Ads with schema type {schema_type_mapping}...")
            logging.info(f"🔄 [SCHEMA] Enforcing schema for Facebook Ads with schema type {schema_type_mapping}...")
            schema_df_enforced = schema_df_input.copy()
            for schema_column_expected, schema_data_type in schema_columns_expected.items():
                if schema_column_expected not in schema_df_enforced.columns: 
                    schema_df_enforced[schema_column_expected] = pd.NA
                try:
                    if schema_data_type in [int, float]:
                        schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected].apply(
                            lambda x: x if isinstance(x, (int, float, np.number, type(None))) else np.nan
                        )
                        schema_df_enforced[schema_column_expected] = pd.to_numeric(schema_df_enforced[schema_column_expected], errors="coerce").fillna(0).astype(schema_data_type)
                    elif schema_data_type == "datetime64[ns, UTC]":
                        schema_df_enforced[schema_column_expected] = pd.to_datetime(schema_df_enforced[schema_column_expected], errors="coerce")
                        if schema_df_enforced[schema_column_expected].dt.tz is None:
                            schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected].dt.tz_localize("UTC")
                        else:
                            schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected].dt.tz_convert("UTC")
                    else:
                        schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected].astype(schema_data_type, errors="ignore")
                except Exception as e:
                    print(f"⚠️ [SCHEMA] Failed to coerce column {schema_column_expected} to {schema_data_type} due to {e}.")
                    logging.warning(f"⚠️ [SCHEMA] Failed to coerce column {schema_column_expected} to {schema_data_type} due to {e}.")
            schema_df_enforced = schema_df_enforced[[col for col in schema_column_expected]]        
            print(f"✅ [SCHEMA] Successfully enforced schema for Facebook Ads with {len(schema_df_enforced)} row(s) and schema type {schema_type_mapping}.")
            logging.info(f"✅ [SCHEMA] Successfully enforced schema for Facebook Ads with {len(schema_df_enforced)} row(s) and schema type {schema_type_mapping}.")
            schema_sections_status["1.1.4. Enforce schema columns for Facebook Ads"] = "succeed"
        except Exception as e:
            schema_sections_status["1.1.4. Enforce schema columns for Facebook Ads"] = "failed"
            print(f"❌ [SCHEMA] Failed to enforce schema for Facebook Ads with schema type {schema_type_mapping} due to {e}.")
            logging.error(f"❌ [SCHEMA] Failed to enforce schema for Facebook Ads with schema type {schema_type_mapping} due to {e}.")
            raise RuntimeError(f"❌ [SCHEMA] Failed to enforce schema for Facebook Ads with schema type {schema_type_mapping} due to {e}.")

    # 1.1.5. Summarize schema enforcement result(s)
    finally:
        schema_time_elapsed = round(time.time() - schema_time_start, 2)
        schema_df_final = schema_df_enforced.copy() if "schema_df_enforced" in locals() and not schema_df_enforced.empty else pd.DataFrame()
        schema_sections_total = len(schema_sections_status)
        schema_sections_failed = [k for k, v in schema_sections_status.items() if v == "failed"]
        schema_rows_output = len(schema_df_final)      
        if any(v == "failed" for v in schema_sections_status.values()):
            print(f"❌ [SCHEMA] Failed to complete schema enforcement for Facebook Ads due to section(s): {', '.join(schema_sections_failed)} in {schema_time_elapsed}s.")
            logging.error(f"❌ [SCHEMA] Failed to complete schema enforcement for Facebook Ads due to section(s): {', '.join(schema_sections_failed)} in {schema_time_elapsed}s.")
            schema_status_final = "schema_failed_all"
        else:
            print(f"🏆 [SCHEMA] Successfully completed schema enforcement for all {len(schema_sections_status)} section(s) with {schema_rows_output} row(s) output in {schema_time_elapsed}s.")
            logging.info(f"🏆 [SCHEMA] Successfully completed schema enforcement for all {len(schema_sections_status)} section(s) with {schema_rows_output} row(s) output in {schema_time_elapsed}s.")
            schema_status_final = "schema_success_all"
        return {
            "schema_df_final": schema_df_final,
            "schema_status_final": schema_status_final,
            "schema_summary_final": {
                "schema_time_elapsed": schema_time_elapsed,
                "schema_rows_output": schema_rows_output,
                "schema_sections_total": schema_sections_total,
                "schema_sections_failed": schema_sections_failed,
            },
        }