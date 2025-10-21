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

    # 1.1.1. Start timing the Facebook Ads campaign insights enrichment process
    enrich_time_start = time.time()
    enrich_sections_status = {}
    print(f"🔍 [ENRICH] Proceeding to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for the Facebook Ads campaign insights enrichment
    if enrich_df_input.empty:
        enrich_sections_status["1.1.1. Start timing the Facebook Ads campaign insights fetching process"] = "failed"
        print("⚠️ [ENRICH] Empty Facebook Ads campaign insights provided then enrichment is suspended.")
        logging.warning("⚠️ [ENRICH] Empty Facebook Ads campaign insights provided then enrichment is skipped.")
        raise ValueError("⚠️ [ENRICH] Empty Facebook Ads campaign insights provided then enrichment is skipped.")

    try:

    # 1.1.3. Enrich spend metric for Facebook Ads campaign insights
        try: 
            print(f"🔍 [ENRICH] Enrichhin spend metric(s) for Facebook Ads campaign insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enrichhing spend metric(s) for Facebook Ads campaign insights with {len(enrich_df_input)} row(s)...")
            enrich_df_spend = enrich_df_input.copy()
            enrich_df_spend["spend"] = pd.to_numeric(enrich_df_spend["spend"], errors="coerce").fillna(0)
            print(f"✅ [ENRICH] Successfully enriched spend metric(s) for Facebook Ads campaign insights with {len(enrich_df_spend)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched spend metric(s) for Facebook Ads campaign insights with {len(enrich_df_spend)} row(s).")
            enrich_sections_status["1.1.3. Enrich spend metric for Facebook Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.3. Enrich spend metric for Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich spend metric(s) for Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich spend metric(s) for Facebook Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich spend metric(s) for Facebook Ads campaign insights due to {e}.")

    # 1.1.4. Enrich goal to action for Facebook Ads campaign insights
        try:
            print(f"🔍 [FETCH] Enriching goal to action for Facebook Ads campaign insights with {len(enrich_df_spend)} row(s)...")
            logging.info(f"🔍 [FETCH] Enriching goal to action for Facebook Ads campaign insights with {len(enrich_df_spend)} row(s)...")
            enrich_df_goal = enrich_df_spend.copy()
            enrich_results_value = []
            enrich_results_type = []
            enrich_messages_value = []
            for idx, row in enrich_df_goal.iterrows():
                goal = row.get("optimization_goal")
                actions = row.get("actions", [])
                if not isinstance(actions, list):
                    actions = []
                enrich_result_value = None
                enrich_result_type = None
                if goal in GOAL_TO_ACTION:
                    action_type = GOAL_TO_ACTION[goal]
                    if action_type in ["reach", "impressions"]:
                        enrich_result_value = pd.to_numeric(row.get("impressions", 0), errors="coerce")
                        enrich_result_type = "impressions"
                    else:
                        for act in actions:
                            if act.get("action_type") == action_type:
                                try:
                                    enrich_result_value = float(act.get("value", 0))
                                except Exception:
                                    print(f"⚠️ [ENRICH] Failed to convert Facebook Ads campaign insights value to float for action_type {action_type}.")
                                    logging.warning(f"⚠️ [ENRICH] Failed to convert Facebook Ads campaign insights value to float for action_type {action_type}.")
                                    value = None
                                break
                        enrich_result_type = "follows_or_likes" if action_type == "like" else action_type
                if enrich_result_value is None:
                    for act in actions:
                        if act.get("action_type") == "onsite_conversion.lead_grouped":
                            try:
                                enrich_result_value = float(act.get("value", 0))
                            except Exception:
                                enrich_result_value = None
                            enrich_result_type = "onsite_conversion.lead_grouped"
                            break
                if enrich_result_value is None:
                    for act in actions:
                        if act.get("action_type") == "lead":
                            try:
                                enrich_result_value = float(act.get("value", 0))
                            except Exception:
                                enrich_result_value = None
                            enrich_result_type = "lead"
                            break
                enrich_results_value.append(enrich_result_value)
                enrich_results_type.append(enrich_result_type)
            enrich_df_goal["enrich_results_value"] = enrich_results_value
            enrich_df_goal["enrich_results_type"] = enrich_results_type
            print(f"✅ [ENRICH] Successfully enriched goal-to-action metrics for Facebook Ads campaign insights with {len(enrich_df_goal)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched goal-to-action metrics for Facebook Ads campaign insights with {len(enrich_df_goal)} row(s).")
            enrich_sections_status["1.1.4. Enrich goal-to-action for Facebook Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.4. Enrich goal-to-action for Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for Facebook Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for Facebook Ads campaign insights due to {e}.")

    # 1.1.5. Enrich purchase result(s) for Facebook Ads campaign insights
        try: 
            print(f"🔍 [ENRICH] Enriching purchase resul(s) for Facebook Ads campaign insights with {len(enrich_df_goal)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching purchase result(s) for Facebook Ads campaign insights with {len(enrich_df_goal)} row(s)...")
            enrich_df_purchase = enrich_df_goal.copy()
            enrich_purchases_value = []
            enrich_purchase_value = None
            for idx, row in enrich_df_purchase.iterrows():
                actions = row.get("actions", [])
                if not isinstance(actions, list):
                    actions = []
                enrich_purchase_value = None
                for act in actions:
                    if act.get("action_type") == "purchase":
                        try:
                            enrich_purchase_value = float(act.get("value", 0))
                        except Exception:
                            enrich_purchase_value = None
                        break
                enrich_purchases_value.append(enrich_purchase_value)
            enrich_df_purchase["enrich_purchases_value"] = enrich_purchases_value
            print(f"✅ [ENRICH] Successfully enriched purchase result(s) for Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched purchase result(s) for Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s).")
            enrich_sections_status["1.1.5. Enrich purchase result(s) for Facebook Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.5. Enrich purchase result(s) for Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich purchase result(s) for Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich purchase result(s) for Facebook Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich purchase result(s) for Facebook Ads campaign insights due to {e}.")

    # 1.1.6. Enrich messaging result(s) for Facebook Ads campaign insights
        try:
            print(f"🔍 [ENRICH] Enriching messaging result(s) for Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching messaging result(s) for Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s)...")
            enrich_df_message = enrich_df_purchase.copy()
            enrich_messages_value = []
            for idx, row in enrich_df_message.iterrows():
                actions = row.get("actions", [])
                if not isinstance(actions, list):
                    actions = []
                enrich_message_value = None
                for act in actions:
                    if act.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
                        try:
                            enrich_message_value = float(act.get("value", 0))
                        except Exception:
                            enrich_message_value = None
                        break
                enrich_messages_value.append(enrich_message_value)
            enrich_df_message["enrich_messages_value"] = enrich_messages_value
            print(f"✅ [ENRICH] Successfully enriched messaging result(s) for Facebook Ads campaign insights with {len(enrich_df_message)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched messaging result(s) for Facebook Ads campaign insights with {len(enrich_df_message)} row(s).")
            enrich_sections_status["1.1.6. Enrich messaging result(s) for Facebook Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.6. Enrich messaging result(s) for Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich messaging result(s) for Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich messaging result(s) for Facebook Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich messaging result(s) for Facebook Ads campaign insights due to {e}.")
        
    # 1.1.7. Safe cast enriched result(s) for Facebook Ads campaign insights
        try:
            print(f"🔄 [ENRICH] Casting enriched result(s to safe mode for Facebook Ads campaign insights with {len(enrich_df_message)} row(s)...")
            logging.info(f"🔄 [ENRICH] Casting enriched result(s to safe mode for Facebook Ads campaign insights with {len(enrich_df_message)} row(s)...")    
            enrich_df_casted = enrich_df_message.copy()
            enrich_df_casted["enrich_results_value"] = pd.to_numeric(enrich_df_casted["enrich_results_value"], errors="coerce")
            enrich_df_casted["enrich_results_type"] = pd.Series(enrich_df_casted["enrich_results_type"], dtype="string")
            enrich_df_casted["enrich_purchases_value"] = pd.to_numeric(enrich_df_casted["enrich_purchases_value"], errors="coerce")
            enrich_df_casted["enrich_messages_value"] = pd.to_numeric(enrich_df_casted["enrich_messages_value"], errors="coerce")
            enriched_summary_casted = {
                "results_value_nan": enrich_df_casted["enrich_results_value"].isna().sum(),
                "purchases_value_nan": enrich_df_casted["enrich_purchases_value"].isna().sum(),
                "messages_value_nan": enrich_df_casted["enrich_messages_value"].isna().sum(),
            }
            print(f"✅ [ENRICH] Successfully casted enriched result(s) for Facebook Ads campaign insights with {len(enrich_df_casted)} and coerced summary {enriched_summary_casted}.")
            logging.info(f"✅ [ENRICH] Successfully casted enriched result(s) for Facebook Ads campaign insights with {len(enrich_df_casted)} and coerced summary {enriched_summary_casted}.")
            enrich_sections_status["1.1.7. Safe cast enriched result(s) for Facebook Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.7. Safe cast enriched result(s) for Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for Facebook Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for Facebook Ads campaign insights due to {e}.")

    # 1.1.8. Summarize enrichment result(s)
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_casted.copy() if not enrich_df_casted.empty else pd.DataFrame()
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_rows_output = len(enrich_df_final)
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete Facebook Ads campaign insights enrichment due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete Facebook Ads campaign insights enrichment due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed Facebook Ads campaign insights enrichment for all {len(enrich_sections_status)} section(s) with {enrich_rows_output} row(s) output in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed Facebook Ads campaign insights enrichment for all {len(enrich_sections_status)} section(s) with {enrich_rows_output} row(s) output in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_success_all"
        return {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_rows_output": enrich_rows_output,
                "enrich_sections_total": len(enrich_sections_status),
                "enrich_sections_failed": [k for k, v in enrich_sections_status.items() if v == "failed"],
            },
        }

# 1.2. Enrich Facebook Ads ad insights from ingestion phase
def enrich_ad_insights(enrich_df_input: pd.DataFrame) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich raw Facebook Ads ad insights for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich raw Facebook Ads ad insights for {len(enrich_df_input)} row(s)....")

    # 1.2.1. Start timing the Facebook Ads ad insights enrichment process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to enrich Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to enrich Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Validate input for the Facebook Ads ad insights enrichment
    if enrich_df_input.empty:
        print("⚠️ [ENRICH] Empty Facebook Ads ad insights provided then enrichment is suspended.")
        logging.warning("⚠️ [ENRICH] Empty Facebook Ads ad insights provided then enrichment is suspended.")
        raise ValueError("⚠️ [ENRICH] Empty Facebook Ads ad insights provided then enrichment is suspended.")

    try:

    # 1.2.3. Enrich spend metric for Facebook Ads ad insights
        enrich_df_processing = enrich_df_input.copy()
        enrich_df_processing["spend"] = pd.to_numeric(enrich_df_processing["spend"], errors="coerce").fillna(0)

    # 1.2.4. Enrich goal to action
        results = []
        result_types = []
        purchases = []
        messaging_started = []
        for idx, row in enrich_df_processing.iterrows():
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

    # 1.2.5. Enrich purchase result(s) for Facebook Ads campaign insights
            purchase_value = None
            for act in actions:
                if act.get("action_type") == "purchase":
                    try:
                        purchase_value = float(act.get("value", 0))
                    except Exception:
                        purchase_value = None
                    break
            purchases.append(purchase_value)

    # 1.2.6. Enrich messaging result(s) for Facebook Ads campaign insights
            messaging_value = None
            for act in actions:
                if act.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
                    try:
                        messaging_value = float(act.get("value", 0))
                    except Exception:
                        messaging_value = None
                    break
            messaging_started.append(messaging_value)

    # 1.2.7. Safe cast enriched result(s)
        enrich_df_processing["result"] = pd.to_numeric(results, errors="coerce")
        enrich_df_processing["result_type"] = pd.Series(result_types, dtype="string")
        enrich_df_processing["purchase"] = pd.to_numeric(purchases, errors="coerce")
        enrich_df_processing["messaging_conversations_started"] = pd.to_numeric(messaging_started, errors="coerce")
    
    # 1.2.8. Summarize enrichment result(s)
        enrich_df_final = enrich_df_processing
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads ad insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads ad insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        return enrich_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to enrich Facebook Ads ad insights for {len(enrich_df_input)} row(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enrich Facebook Ads ad insights for {len(enrich_df_input)} row(s) due to {e}.")
        return pd.DataFrame()

# 2. ENRICH FACEBOOK ADS INSIGHTS FROM STAGING PHASE

# 2.1. Enrich Facebook Ads campaign insights from staging phase
def enrich_campaign_fields(enrich_df_input: pd.DataFrame, table_id: str) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich staging Facebook Ads campaign field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich staging Facebook Ads campaign fields(s)...")  

    # 2.1.1. Start timing the Facebook Ads campaign insights enrichment process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to enrich Facebook Ads campaign insights at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to enrich Facebook Ads campaign insights at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:
        enrich_df_processing = enrich_df_input.copy()

    # 2.1.2. Enrich table-level field(s) for Facebook Ads campaign insights
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",
            table_name
        )
        if match:
            enrich_df_processing["nen_tang"] = match.group("platform")
            enrich_df_processing["phong_ban"] = match.group("department")
            enrich_df_processing["tai_khoan"] = match.group("account")
        
    # 2.1.3. Enrich campaign-level field(s)
        enrich_df_processing["hinh_thuc"] = enrich_df_processing["campaign_name"].str.split("_").str    [0]
        enrich_df_processing["khu_vuc"] = enrich_df_processing["campaign_name"].str.split("_").str[1]
        enrich_df_processing["ma_ngan_sach_cap_1"] = enrich_df_processing["campaign_name"].str.split("_").str[2]
        enrich_df_processing["ma_ngan_sach_cap_2"] = enrich_df_processing["campaign_name"].str.split("_").str[3]
        enrich_df_processing["nganh_hang"] = enrich_df_processing["campaign_name"].str.split("_").str[4]
        enrich_df_processing["nhan_su"] = enrich_df_processing["campaign_name"].str.split("_").str[5]
        enrich_df_processing["chuong_trinh"] = enrich_df_processing["campaign_name"].str.split("_").str[7]
        enrich_df_processing["noi_dung"] = enrich_df_processing["campaign_name"].str.split("_").str[8]
        enrich_df_processing["thang"] = pd.to_datetime(enrich_df_processing["date_start"]).dt.strftime("%Y-%m")
    
    # 2.1.4. Summarize enrichment result(s)
        enrich_df_final = enrich_df_processing
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads campaign insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads campaign insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        return enrich_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enrich Facebook Ads campaign insights for {len(enrich_df_input)} row(s) due to {e}.")
        return pd.DataFrame()

# 2.2. Enrich Facebook Ads ad insights from staging phase
def enrich_ad_fields(enrich_df_input: pd.DataFrame, table_id: str) -> pd.DataFrame:   
    print("🚀 [ENRICH] Starting to enrich staging Facebook Ads ad field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich staging Facebook Ads ad fields(s)...")    

    # 2.2.1. Start timing the Facebook Ads ad insights enrichment process
    start_time = time.time()
    print(f"🔍 [FETCH] Proceeding to enrich Facebook Ads ad insights at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to enrich Facebook Ads ad insights at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:
        enrich_df_processing = enrich_df_input.copy()
    
    # 2.2.2. Enrich table-level field(s) for Facebook Ads ad insights
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$",
            table_name
        )
        if match:
            enrich_df_processing["nen_tang"] = match.group("platform")
            enrich_df_processing["phong_ban"] = match.group("department")
            enrich_df_processing["tai_khoan"] = match.group("account")   

    # 2.2.3. Enrich adset-level field(s) for Facebook Ads ad insights
        if "adset_name" in enrich_df_processing.columns:
            adset_parts = enrich_df_processing["adset_name"].fillna("").str.split("_")
            enrich_df_processing["vi_tri"] = adset_parts.str[0].fillna("unknown")
            enrich_df_processing["doi_tuong"] = adset_parts.str[1].fillna("unknown")
            enrich_df_processing["dinh_dang"] = adset_parts.str[2].fillna("unknown")
            enrich_df_processing["adset_name_invalid"] = adset_parts.str.len() < 3

    # 2.2.4. Enrich campaign-level field(s) for Facebook Ads ad insights
        if "campaign_name" in enrich_df_processing.columns:
            camp_parts = enrich_df_processing["campaign_name"].fillna("").str.split("_")
            enrich_df_processing["hinh_thuc"] = camp_parts.str[0].fillna("unknown")
            enrich_df_processing["khu_vuc"] = camp_parts.str[1].fillna("unknown")
            enrich_df_processing["ma_ngan_sach_cap_1"] = camp_parts.str[2].fillna("unknown")
            enrich_df_processing["ma_ngan_sach_cap_2"] = camp_parts.str[3].fillna("unknown")
            enrich_df_processing["nganh_hang"] = camp_parts.str[4].fillna("unknown")
            enrich_df_processing["nhan_su"]   = camp_parts.str[5].fillna("unknown")
            enrich_df_processing["chuong_trinh"] = camp_parts.str[7].fillna("unknown")
            enrich_df_processing["noi_dung"] = camp_parts.str[8].fillna("unknown")
            enrich_df_processing["campaign_name_invalid"] = camp_parts.str.len() < 9

    # 2.2.5. Enrich other ad-level field(s) for Facebook Ads ad insights
        if "date_start" in enrich_df_processing.columns:
           enrich_df_processing["thang"] = pd.to_datetime(enrich_df_processing["date_start"]).dt.strftime("%Y-%m")

    # 2.2.6. Summarize enrich result(s)
        enrich_df_final = enrich_df_processing
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed Facebook Ads ad insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed Facebook Ads ad insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        return enrich_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to enrich Facebook Ads ad insights for {len(enrich_df_input)} row(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enrich Facebook Ads ad insights for {len(enrich_df_input)} row(s) due to {e}.")
        return pd.DataFrame()