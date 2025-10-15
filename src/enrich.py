"""
==================================================================
FACEBOOK ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming raw Facebook Ads insights 
into a clean, BigQuery-ready dataset optimized for advanced analytics, 
cross-platform reporting, and machine learning applications.

By centralizing enrichment rules, this module ensures transparency, 
consistency, and maintainability across the marketing data pipeline to  
build insight-ready tables.

✔️ Maps `optimization_goal` to its corresponding business action type  
✔️ Standardizes campaign, ad set, and ad-level naming conventions  
✔️ Extracts and normalizes key performance metrics across campaigns  
✔️ Cleans and validates data to ensure schema and field consistency  
✔️ Reduces payload size by removing redundant or raw field(s)

⚠️ This module focuses *only* on enrichment and transformation logic.  
It does **not** handle data fetching, ingestion, loading, or metric modeling.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integraton
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" libraries for integraton
import re

# Add Python time ultilities for integration
import time

# Get optimization_goal mapping to action_type used for calculating main result type
GOAL_TO_ACTION = {
    "REACH": "reach",
    "IMPRESSIONS": "impressions",
    "AD_RECALL_LIFT": "estimated_ad_recall_lift",
    "LANDING_PAGE_VIEWS": "landing_page_view",
    "LINK_CLICKS": "link_click",
    "PAGE_LIKES": "like",
    "VIDEO_VIEWS": "video_view",
    "POST_ENGAGEMENT": "post_engagement",
    "ENGAGED_USERS": "post_engagement",
    "MESSAGES": "onsite_conversion.messaging_conversation_started_7d",
    "REPLIES": "onsite_conversion.messaging_conversation_started_7d",
    "MESSAGING_CONVERSATIONS_REPLIES": "onsite_conversion.messaging_conversation_started_7d",
    "LEAD_GENERATION": "lead",
    "THRUPLAY": "video_view",
    "APP_INSTALLS": "mobile_app_install",
    "OFFSITE_CONVERSIONS": "purchase",
    "CONVERSIONS": "purchase",
    "VALUE": "value",
    "QUALITY_LEAD": "lead",
}

# 1. ENRICH FACEBOOK ADS INSIGHTS FROM INGESTION PHASE

# 1.1. Enrich Facebook Ads campaign insights from ingestion phase
def enrich_campaign_insights(enrich_df_input: pd.DataFrame) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich raw Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich raw Facebook Ads campaign insights for {len(enrich_df_input)} row(s)....")

    # 1.1.1. Start timing the Facebook Ads campaign metadata fetching process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for the Facebook Ads campaign insights enrichment
    if enrich_df_input.empty:
        print("⚠️ [ENRICH] Empty Facebook Ads campaign insights provided then enrichment is suspended.")
        logging.warning("⚠️ [ENRICH] Empty Facebook Ads campaign insights provided then enrichment is skipped.")
        raise ValueError("⚠️ [ENRICH] Empty Facebook Ads campaign insights provided then enrichment is skipped.")

    # 1.1.3. Enrich spend metric for Facebook Ads campaign insights
    try:
        enrich_df_processing = enrich_df_input.copy()
        enrich_df_processing["spend"] = pd.to_numeric(enrich_df_processing["spend"], errors="coerce").fillna(0)

    # 1.1.4. Enrich goal to action for Facebook Ads campaign insights
        results = []
        result_types = []
        purchases = []
        messaging_started = []
        for idx, row in enrich_df_processing.iterrows():
            goal = row.get("optimization_goal")
            actions = row.get("actions", [])
            if not isinstance(actions, list):
                actions = []
            value = None
            result_type = None
            if goal in GOAL_TO_ACTION:
                action_type = GOAL_TO_ACTION[goal]
                if action_type in ["reach", "impressions"]:
                    value = pd.to_numeric(row.get("impressions", 0), errors="coerce")
                    result_type = "impressions"
                else:
                    for act in actions:
                        if act.get("action_type") == action_type:
                            try:
                                value = float(act.get("value", 0))
                            except Exception:
                                print(f"⚠️ [ENRICH] Failed to convert Facebook Ads campaign insights value to float for action_type {action_type}.")
                                logging.warning(f"⚠️ [ENRICH] Failed to convert Facebook Ads campaign insights value to float for action_type {action_type}.")
                                value = None
                            break
                    result_type = "follows_or_likes" if action_type == "like" else action_type
            if value is None:
                for act in actions:
                    if act.get("action_type") == "onsite_conversion.lead_grouped":
                        try:
                            value = float(act.get("value", 0))
                        except Exception:
                            value = None
                        result_type = "onsite_conversion.lead_grouped"
                        break
            if value is None:
                for act in actions:
                    if act.get("action_type") == "lead":
                        try:
                            value = float(act.get("value", 0))
                        except Exception:
                            value = None
                        result_type = "lead"
                        break
            results.append(value)
            result_types.append(result_type)
    
    # 1.1.5. Enrich purchase result(s) for Facebook Ads campaign insights
            purchase_value = None
            for act in actions:
                if act.get("action_type") == "purchase":
                    try:
                        purchase_value = float(act.get("value", 0))
                    except Exception:
                        purchase_value = None
                    break
            purchases.append(purchase_value)

    # 1.1.6. Enrich messaging result(s) for Facebook Ads campaign insights
            messaging_value = None
            for act in actions:
                if act.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
                    try:
                        messaging_value = float(act.get("value", 0))
                    except Exception:
                        messaging_value = None
                    break
            messaging_started.append(messaging_value)

    # 1.1.7. Safe cast enriched result(s)
        enrich_df_processing["result"] = pd.to_numeric(results, errors="coerce")
        enrich_df_processing["result_type"] = pd.Series(result_types, dtype="string")
        enrich_df_processing["purchase"] = pd.to_numeric(purchases, errors="coerce")
        enrich_df_processing["messaging_conversations_started"] = pd.to_numeric(messaging_started, errors="coerce")

    # 1.1.8. Summarize enrichment result(s)
        enrich_df_final = enrich_df_processing
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads campaign insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads insights metadata enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        return enrich_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) due to {e}.")
        return pd.DataFrame()

# 1.2. Enrich Facebook ad insights from ingestion phase
def enrich_ad_insights(df: pd.DataFrame) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich raw Facebook ad insights...")
    logging.info("🚀 [ENRICH] Starting to enrich raw Facebook ad insights...")

    # 1.2.1. Checking if DataFrame is empty or not
    if df.empty:
        print("⚠️ [ENRICH] Facebook input dataframe is empty then enrichment is skipped.")
        logging.warning("⚠️ [ENRICH] Facebook input dataframe is empty then enrichment is skipped.")
        return df

    # 1.2.2. Enrich spend metric
    try:
        df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)

    # 1.2.3. Enrich goal to action
        results = []
        result_types = []
        purchases = []
        messaging_started = []
        for idx, row in df.iterrows():
            goal = row.get("optimization_goal", None) if isinstance(row, dict) else row["optimization_goal"]
            actions = row.get("actions", []) if isinstance(row, dict) else row["actions"]
            if not isinstance(actions, list):
                actions = []
            value = None
            result_type = None
            if goal in GOAL_TO_ACTION:
                action_type = GOAL_TO_ACTION[goal]
                if action_type in ["reach", "impressions"]:
                    value = pd.to_numeric(row["impressions"], errors="coerce")
                    result_type = "impressions"
                else:
                    for act in actions:
                        if act.get("action_type") == action_type:
                            try:
                                value = float(act.get("value", 0))
                            except Exception:
                                print(f"⚠️ [ENRICH] Cannot convert value to float for Facebook action_type {action_type}.")
                                logging.warning(f"⚠️ [ENRICH] Cannot convert value to float for Facebook action_type {action_type}.")
                                value = None
                            break
                    result_type = "follows_or_likes" if action_type == "like" else action_type
            if value is None:
                for act in actions:
                    if act.get("action_type") == "onsite_conversion.lead_grouped":
                        try:
                            value = float(act.get("value", 0))
                        except Exception:
                            value = None
                        result_type = "onsite_conversion.lead_grouped"
                        break
            if value is None:
                for act in actions:
                    if act.get("action_type") == "lead":
                        try:
                            value = float(act.get("value", 0))
                        except Exception:
                            value = None
                        result_type = "lead"
                        break
            results.append(value)
            result_types.append(result_type)

    # 1.2.4. Enrich purchase result(s)
            purchase_value = None
            for act in actions:
                if act.get("action_type") == "purchase":
                    try:
                        purchase_value = float(act.get("value", 0))
                    except Exception:
                        purchase_value = None
                    break
            purchases.append(purchase_value)

    # 1.2.5. Enrich messaging result(s)
            messaging_value = None
            for act in actions:
                if act.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
                    try:
                        messaging_value = float(act.get("value", 0))
                    except Exception:
                        messaging_value = None
                    break
            messaging_started.append(messaging_value)

    # 1.2.6. Summarize enrichment result(s)
        df["result"] = pd.to_numeric(results, errors="coerce")
        df["result_type"] = pd.Series(result_types, dtype="string")
        df["purchase"] = pd.to_numeric(purchases, errors="coerce")
        df["messaging_conversations_started"] = pd.to_numeric(messaging_started, errors="coerce")
        print(f"✅ [ENRICH] Successfully enriched raw Facebook ad insights with {len(df)} row(s).")
        logging.info(f"✅ [ENRICH] Successfully enriched raw Facebook ad insights with {len(df)} row(s).")
        return df
    except Exception as e:
        print(f"❌ [ENRICH] Failed to enrich raw Facebook ad insights due to {e}.")
        logging.error(f"❌ [ENRICH] Failed to enrich raw Facebook ad insights due to {e}.")
        return df

# 2. ENRICH FACEBOOK INSIGHTS FROM STAGING PHASE

# 2.1. Enrich structured campaign-level field(s) from campaign name
def enrich_campaign_fields(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich staging Facebook campaign field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich staging Facebook campaign fields(s)...")  
  
    # 2.1.1. Enrich table-level field(s)
    try:
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",
            table_name
        )
        if match:
            df["nen_tang"] = match.group("platform")
            df["phong_ban"] = match.group("department")
            df["tai_khoan"] = match.group("account")
        
    # 2.1.2. Enrich campaign-level field(s)
        df["hinh_thuc"] = df["campaign_name"].str.split("_").str    [0]
        df["khu_vuc"] = df["campaign_name"].str.split("_").str[1]
        df["ma_ngan_sach_cap_1"] = df["campaign_name"].str.split("_").str[2]
        df["ma_ngan_sach_cap_2"] = df["campaign_name"].str.split("_").str[3]
        df["nganh_hang"] = df["campaign_name"].str.split("_").str[4]
        df["nhan_su"] = df["campaign_name"].str.split("_").str[5]
        df["chuong_trinh"] = df["campaign_name"].str.split("_").str[7]
        df["noi_dung"] = df["campaign_name"].str.split("_").str[8]
        df["thang"] = pd.to_datetime(df["date_start"]).dt.strftime("%Y-%m")
    
    # 2.1.3. Summarize enrichment result(s)
        print(f"✅ [ENRICH] Successfully enriched field(s) for staging Facebook campaign insights with {len(df)} row(s).")
        logging.info(f"✅ [ENRICH] Successfully enriched field(s) for staging Facebook campaign insights with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [ENRICH] Failed to enrich staging Facebook campaign field(s) due to {e}.")
        logging.error(f"❌ [ENRICH] Failed to enrich staging Facebook campaign field(s) due to {e}.")
    return df

# 2.2. Enrich structured ad-level fields from adset_name and campaign_name
def enrich_ad_fields(df: pd.DataFrame, table_id: str) -> pd.DataFrame:   
    print("🚀 [ENRICH] Starting to enrich Facebook staging ad fields...")
    logging.info("🚀 [ENRICH] Starting to enrich Facebook staging ad fields...")    

    # 2.2.1. Enrich table-level field(s)
    try:
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$",
            table_name
        )
        if match:
            df["nen_tang"] = match.group("platform")
            df["phong_ban"] = match.group("department")
            df["tai_khoan"] = match.group("account")   

    # 2.2.2. Enrich adset-level field(s)
        if "adset_name" in df.columns:
            adset_parts = df["adset_name"].fillna("").str.split("_")
            df["vi_tri"] = adset_parts.str[0].fillna("unknown")
            df["doi_tuong"] = adset_parts.str[1].fillna("unknown")
            df["dinh_dang"] = adset_parts.str[2].fillna("unknown")
            df["adset_name_invalid"] = adset_parts.str.len() < 3

    # 2.2.3. Enrich campaign-level field(s)
        if "campaign_name" in df.columns:
            camp_parts = df["campaign_name"].fillna("").str.split("_")
            df["hinh_thuc"] = camp_parts.str[0].fillna("unknown")
            df["khu_vuc"] = camp_parts.str[1].fillna("unknown")
            df["ma_ngan_sach_cap_1"] = camp_parts.str[2].fillna("unknown")
            df["ma_ngan_sach_cap_2"] = camp_parts.str[3].fillna("unknown")
            df["nganh_hang"] = camp_parts.str[4].fillna("unknown")
            df["nhan_su"]   = camp_parts.str[5].fillna("unknown")
            df["chuong_trinh"] = camp_parts.str[7].fillna("unknown")
            df["noi_dung"] = camp_parts.str[8].fillna("unknown")
            df["campaign_name_invalid"] = camp_parts.str.len() < 9

    # 2.2.4. Enrich other ad-level field(s)
        if "date_start" in df.columns:
           df["thang"] = pd.to_datetime(df["date_start"]).dt.strftime("%Y-%m")

    # 2.2.5. Summarize enrich result(s)
        print(f"✅ [ENRICH] Successfully enriched fields for staging Facebook ad insights with {len(df)} row(s).")
        logging.info(f"✅ [ENRICH] Successfully enriched fields for staging Facebook ad insights with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [ENRICH] Failed to enrich fields for staging Facebook ad insights due to {e}.")
        logging.warning(f"❌ [ENRICH] Failed to enrich fields for staging Facebook ad insights due to {e}.")    
    return df