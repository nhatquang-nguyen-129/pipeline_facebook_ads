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
# Add logging capability for tracking process execution and errors
import logging

# Add Python Pandas library for data processing
import pandas as pd

# Add Python NumPy library for numerical computing and array operations
import numpy as np

# 1. PROCESS SCHEMA FOR GIVEN PYTHON DATAFRAME

# 1.1. Ensure that the given DataFrame contains all required columns with correct datatypes for the specified schema type
def ensure_table_schema(df: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    
    # 1.1.1. Define schema mapping for Facebook data type
    mapping_facebook_schema = {
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
            "platform": str,
            "department": str,
            "account": str,
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
    
    # 1.1.2. Validate that the given schema_type exists
    if schema_type not in mapping_facebook_schema:
        raise ValueError(f"❌ Unknown schema_type: {schema_type}")

    # 1.1.3. Retrieve the expected schema definition for the given type
    expected_columns = mapping_facebook_schema[schema_type]
    
    # 1.1.4. Iterate through each expected column and enforce schema rules
    for col, dtype in expected_columns.items():
        if col not in df.columns:
            df[col] = pd.NA

    # 1.1.5. Handle numeric type include integer and float
        try:
            if dtype in [int, float]:
                df[col] = df[col].apply(
                    lambda x: x if isinstance(x, (int, float, np.number, type(None))) else np.nan
                )
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(dtype)

    # 1.1.6. Handle datetime type by forcing UTC
            elif dtype == "datetime64[ns, UTC]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize("UTC")
                else:
                    df[col] = df[col].dt.tz_convert("UTC")

    # 1.1.7. Handle string or other object types
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
    
    # 1.1.8. Reorder columns to match schema definition
        except Exception as e:
            print(f"⚠️ [SCHEMA] Column '{col}' cannot be coerced to {dtype} due to {e}.")
            logging.warning(f"⚠️ [SCHEMA] Column '{col}' cannot be coerced to {dtype} due to {e}.")
    df = df[[col for col in expected_columns]]
    return df