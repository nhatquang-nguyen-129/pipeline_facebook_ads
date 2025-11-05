"""
==================================================================
FACEBOOK ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming raw Facebook Ads insights 
into a clean, BigQuery-ready dataset optimized for advanced analytics, 
cross-platform reporting and machine learning applications.

By centralizing enrichment rules, this module ensures transparency, 
consistency, and maintainability across the marketing data pipeline to  
build insight-ready tables.

✔️ Maps `optimization_goal` to its corresponding business action type  
✔️ Standardizes campaign, ad set and ad-level naming conventions  
✔️ Extracts and normalizes key performance metrics across campaigns  
✔️ Cleans and validates data to ensure schema and field consistency  
✔️ Reduces payload size by removing redundant or raw field(s)

⚠️ This module focuses *only* on enrichment and transformation logic.  
It does **not** handle data fetching, ingestion or staging
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import (
    datetime,
    timedelta
)

# Add Python JSON ultilities for integration
import json 

# Add Python logging ultilities for integraton
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python timezone ultilities for integration
import pytz

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

    # 1.1.1. Start timing the raw Facebook Ads campaign insights enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    enrich_section_name = "[ENRICH] Start timing the raw Facebook Ads campaign insights enrichment"
    enrich_sections_status[enrich_section_name] = "succeed"
    enrich_sections_time[enrich_section_name] = 0.0  # just marker not real time
    print(f"🔍 [ENRICH] Proceeding to enrich raw Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich raw Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for the raw Facebook Ads campaign insights enrichment
    enrich_section_name = "[ENRICH] Validate input for the raw Facebook Ads campaign insights enrichment"
    enrich_section_start = time.time()
    try:
        if enrich_df_input.empty:        
            enrich_sections_status[enrich_section_name] = "failed"        
            print("⚠️ [ENRICH] Empty raw Facebook Ads campaign insights provided then enrichment is suspended.")
            logging.warning("⚠️ [ENRICH] Empty raw Facebook Ads campaign insights provided then enrichment is suspended.")
            raise ValueError("⚠️ [ENRICH] Empty raw Facebook Ads campaign insights provided then enrichment is suspended.")    
        else:
            enrich_sections_status[enrich_section_name] = "succeed"
            print("✅ [ENRICH] Successfully validated input for raw Facebook Ads campaign insights enrichment.")
            logging.info("✅ [ENRICH] Successfully validated input for raw Facebook Ads campaign insights enrichment.")
    finally:
        enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    try:

    # 1.1.3. Normalize 'actions' column for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Normalize 'actions' column for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Normalizing 'actions' column of Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")
            logging.info(f"🔄 [ENRICH] Normalizing 'actions' column of Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")
            enrich_df_normalized = enrich_df_input.copy()
            enrich_df_normalized["actions"] = enrich_df_normalized["actions"].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip().startswith("[") else (x if isinstance(x, list) else []))
            print(f"✅ [ENRICH] Successfully normalized 'actions' column of Facebook Ads campaign insights for {len(enrich_df_normalized)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully normalized 'actions' column of Facebook Ads campaign insights for {len(enrich_df_normalized)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to normalize 'actions' column of Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to normalize 'actions' column of Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.1.4. Enrich spend metric for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich spend metric for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try: 
            print(f"🔄 [ENRICH] Enriching spend metric(s) for raw Facebook Ads campaign insights with {len(enrich_df_normalized)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching spend metric(s) for raw Facebook Ads campaign insights with {len(enrich_df_normalized)} row(s)...")
            enrich_df_spend = enrich_df_normalized.copy()
            enrich_df_spend["spend"] = pd.to_numeric(enrich_df_spend["spend"], errors="coerce").fillna(0)
            print(f"✅ [ENRICH] Successfully enriched spend metric(s) for Facebook Ads campaign insights with {len(enrich_df_spend)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched spend metric(s) for raw Facebook Ads campaign insights with {len(enrich_df_spend)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich spend metric(s) for raw Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich spend metric(s) for raw Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)            

    # 1.1.5. Enrich date columns for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich date columns for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching date column(s) for raw Facebook Ads campaign insights with {len(enrich_df_spend)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching date column(s) for raw Facebook Ads campaign insights with {len(enrich_df_spend)} row(s)...")
            enrich_df_date = enrich_df_spend.copy()
            enrich_df_date = enrich_df_date.assign(
                date_start=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                date_stop=lambda df: pd.to_datetime(df["date_stop"], errors="coerce", utc=True).dt.ceil("D") - pd.Timedelta(seconds=1),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC)
            ) 
            print(f"✅ [ENRICH] Successfully enriched date column(s) for Facebook Ads campaign insights with {len(enrich_df_date)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date column(s) for Facebook Ads campaign insights with {len(enrich_df_date)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date column(s) for raw Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich date column(s) for raw Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)              

    # 1.1.6. Enrich goal to action for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich goal to action for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching goal to action for raw Facebook Ads campaign insights with {len(enrich_df_date)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching goal to action for raw Facebook Ads campaign insights with {len(enrich_df_date)} row(s)...")
            enrich_df_goal = enrich_df_date.copy()
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
            print(f"✅ [ENRICH] Successfully enriched goal-to-action metrics for raw Facebook Ads campaign insights with {len(enrich_df_goal)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched goal-to-action metrics for raw Facebook Ads campaign insights with {len(enrich_df_goal)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for raw Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for raw Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)                

    # 1.1.7. Enrich purchase result(s) for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich purchase result(s) for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try: 
            print(f"🔄 [ENRICH] Enriching purchase resul(s) for raw Facebook Ads campaign insights with {len(enrich_df_goal)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching purchase result(s) for raw Facebook Ads campaign insights with {len(enrich_df_goal)} row(s)...")
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
            print(f"✅ [ENRICH] Successfully enriched purchase result(s) for raw Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched purchase result(s) for raw Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich purchase result(s) for Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich purchase result(s) for raw Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)              

    # 1.1.8. Enrich messaging result(s) for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich messaging result(s) for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching messaging result(s) for raw Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching messaging result(s) for raw Facebook Ads campaign insights with {len(enrich_df_purchase)} row(s)...")
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
            print(f"✅ [ENRICH] Successfully enriched messaging result(s) for raw Facebook Ads campaign insights with {len(enrich_df_message)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched messaging result(s) for raw Facebook Ads campaign insights with {len(enrich_df_message)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich messaging result(s) for raw Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich messaging result(s) for raw Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)                
        
    # 1.1.9. Safe cast enriched result(s) for raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Safe cast enriched result(s) for raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Casting enriched result(s to safe mode for raw Facebook Ads campaign insights with {len(enrich_df_message)} row(s)...")
            logging.info(f"🔄 [ENRICH] Casting enriched result(s to safe mode for raw Facebook Ads campaign insights with {len(enrich_df_message)} row(s)...")    
            enrich_df_casted = enrich_df_message.copy()
            enrich_df_casted = enrich_df_casted.assign(
                enrich_results_value=lambda df: pd.to_numeric(df["enrich_results_value"], errors="coerce").fillna(0),
                enrich_results_type=lambda df: df["enrich_results_type"].astype("string").fillna("unknown"),
                enrich_purchases_value=lambda df: pd.to_numeric(df["enrich_purchases_value"], errors="coerce").fillna(0),
                enrich_messages_value=lambda df: pd.to_numeric(df["enrich_messages_value"], errors="coerce").fillna(0),
            )
            enriched_summary_casted = {
                "results_value_invalid": (enrich_df_casted["enrich_results_value"] == 0).sum(),
                "purchases_value_invalid": (enrich_df_casted["enrich_purchases_value"] == 0).sum(),
                "messages_value_invalid": (enrich_df_casted["enrich_messages_value"] == 0).sum(),
                "results_type_unknown": (enrich_df_casted["enrich_results_type"] == "unknown").sum(),
            }
            print(f"✅ [ENRICH] Successfully casted enriched result(s) for raw Facebook Ads campaign insights with {len(enrich_df_casted)} and coerced summary {enriched_summary_casted}.")
            logging.info(f"✅ [ENRICH] Successfully casted enriched result(s) for raw Facebook Ads campaign insights with {len(enrich_df_casted)} and coerced summary {enriched_summary_casted}.")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for raw Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for raw Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.1.10. Normalize enriched numeric field(s) of raw Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Normalize enriched numeric field(s) of raw Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Normalizing enriched field(s) of Facebook Ads campaign insights to standard schema with {len(enrich_df_casted)} row(s)...")
            logging.info(f"🔄 [ENRICH] Normalizing enriched field(s) of Facebook Ads campaign insights to standard schema with {len(enrich_df_casted)} row(s)...")
            enrich_df_finalized = enrich_df_casted.copy()
            enrich_df_finalized = enrich_df_finalized.assign(
                **{
                    col: pd.to_numeric(enrich_df_finalized[col], errors="coerce").fillna(0)
                    for col in ["reach", "impressions", "clicks"]
                    if col in enrich_df_finalized.columns
                },
                result=lambda df: df["enrich_results_value"],
                result_type=lambda df: df["enrich_results_type"],
                purchase=lambda df: df["enrich_purchases_value"],
                messaging_conversations_started=lambda df: df["enrich_messages_value"],
            )
            print(f"✅ [ENRICH] Successfully normalized enriched field(s) of Facebook Ads campaign insights to standard schema with {len(enrich_df_finalized)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully normalized enriched field(s) of Facebook Ads campaign insights to standard schema with {len(enrich_df_finalized)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to normalize enriched field(s) of Facebook Ads campaign insights to standard schema due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to normalize enriched field(s) of Facebook Ads campaign insights to standard schema due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)            

    # 1.1.11. Summarize enrichment result(s) for raw Facebook Ads ad insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_finalized.copy() if not enrich_df_finalized.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete raw Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to unsuccessful section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete raw Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to unsuccessful section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed raw Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed raw Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"
        enrich_sections_detail = {
            section: {
                "status": enrich_sections_status.get(section, "unknown"),
                "time": enrich_sections_time.get(section, None),
            }
            for section in set(enrich_sections_status) | set(enrich_sections_time)
        }        
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }
    return enrich_results_final   

# 1.2. Enrich Facebook Ads ad insights from ingestion phase
def enrich_ad_insights(enrich_df_input: pd.DataFrame) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich raw Facebook Ads ad insights for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich raw Facebook Ads ad insights for {len(enrich_df_input)} row(s)....")

    # 1.2.1. Start timing the raw Facebook Ads ad insights enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    enrich_section_name = "[ENRICH] Start timing the raw Facebook Ads ad insights enrichment"
    enrich_sections_status[enrich_section_name] = "succeed"  
    print(f"🔍 [FETCH] Proceeding to enrich raw Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to enrich raw Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Validate input for the raw Facebook Ads ad insights enrichment
    enrich_section_name = "[ENRICH] Validate input for the raw Facebook Ads ad insights enrichment"
    enrich_section_start = time.time()
    try:
        if enrich_df_input.empty:
            enrich_sections_status["[ENRICH] Validate input for the raw Facebook Ads ad insights enrichment"] = "failed"
            print("⚠️ [ENRICH] Empty raw Facebook Ads ad insights provided then enrichment is suspended.")
            logging.warning("⚠️ [ENRICH] Empty raw Facebook Ads ad insights provided then enrichment is suspended.")
            raise ValueError("⚠️ [ENRICH] Empty raw Facebook Ads ad insights provided then enrichment is suspended.")
        else:
            enrich_sections_status["[ENRICH] Validate input for the raw Facebook Ads ad insights enrichment"] = "succeed"
            print("✅ [ENRICH] Successfully validated input for raw Facebook Ads ad insights enrichment.")
            logging.info("✅ [ENRICH] Successfully validated input for raw Facebook Ads ad insights enrichment.")
    finally:
        enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)        

    try:

    # 1.2.3. Normalize 'actions' column for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Normalize 'actions' column for raw ad Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Normalizing 'actions' column of Facebook Ads ad insights for {len(enrich_df_input)} row(s)...")
            logging.info(f"🔄 [ENRICH] Normalizing 'actions' column of Facebook Ads ad insights for {len(enrich_df_input)} row(s)...")
            enrich_df_normalized = enrich_df_input.copy()
            enrich_df_normalized["actions"] = enrich_df_normalized["actions"].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip().startswith("[") else (x if isinstance(x, list) else []))
            print(f"✅ [ENRICH] Successfully normalized 'actions' column of Facebook Ads ad insights for {len(enrich_df_normalized)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully normalized 'actions' column of Facebook Ads ad insights for {len(enrich_df_normalized)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to normalize 'actions' column of Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to normalize 'actions' column of Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.4. Enrich spend metric for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich spend metric for raw Facebook Ads ad insights"
        enrich_section_start = time.time()        
        try: 
            print(f"🔍 [ENRICH] Enrichhin spend metric for raw Facebook Ads ad insights with {len(enrich_df_normalized)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enrichhing spend metric for raw Facebook Ads ad insights with {len(enrich_df_normalized)} row(s)...")
            enrich_df_spend = enrich_df_normalized.copy()
            enrich_df_spend["spend"] = pd.to_numeric(enrich_df_spend["spend"], errors="coerce").fillna(0)
            print(f"✅ [ENRICH] Successfully enriched spend metric for raw Facebook Ads ad insights with {len(enrich_df_spend)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched spend metric for raw Facebook Ads ad insights with {len(enrich_df_spend)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich spend metric for raw Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich spend metric for raw Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.5. Enrich date columns for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich date columns for raw Facebook Ads ad insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching date column(s) for raw Facebook Ads ad insights with {len(enrich_df_spend)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching date column(s) for raw Facebook Ads ad insights with {len(enrich_df_spend)} row(s)...")
            enrich_df_date = enrich_df_spend.copy()
            enrich_df_date = enrich_df_date.assign(
                date_start=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                date_stop=lambda df: pd.to_datetime(df["date_stop"], errors="coerce", utc=True).dt.ceil("D") - pd.Timedelta(seconds=1),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC)
            ) 
            print(f"✅ [ENRICH] Successfully enriched date column(s) for Facebook Ads ad insights with {len(enrich_df_date)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date column(s) for Facebook Ads ad insights with {len(enrich_df_date)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date column(s) for raw Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich date column(s) for raw Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.2.6. Enrich goal to action for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich goal to action for raw Facebook Ads ad insights"
        enrich_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Enriching goal to action for raw Facebook Ads ad insights with {len(enrich_df_spend)} row(s)...")
            logging.info(f"🔍 [FETCH] Enriching goal to action for raw Facebook Ads ad insights with {len(enrich_df_spend)} row(s)...")
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
                                    print(f"⚠️ [ENRICH] Failed to convert raw Facebook Ads ad insights value to float for action_type {action_type}.")
                                    logging.warning(f"⚠️ [ENRICH] Failed to convert raw Facebook Ads ad insights value to float for action_type {action_type}.")
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
            print(f"✅ [ENRICH] Successfully enriched goal-to-action metrics for raw Facebook Ads ad insights with {len(enrich_df_goal)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched goal-to-action metrics for raw Facebook Ads ad insights with {len(enrich_df_goal)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for raw Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich goal-to-action metrics for raw Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.2.7. Enrich purchase result(s) for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich purchase result(s) for raw Facebook Ads ad insights"
        enrich_section_start = time.time()        
        try: 
            print(f"🔍 [ENRICH] Enriching purchase resul(s) for raw Facebook Ads ad insights with {len(enrich_df_goal)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching purchase result(s) for raw Facebook Ads ad insights with {len(enrich_df_goal)} row(s)...")
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
            print(f"✅ [ENRICH] Successfully enriched purchase result(s) for raw Facebook Ads ad insights with {len(enrich_df_purchase)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched purchase result(s) for raw Facebook Ads ad insights with {len(enrich_df_purchase)} row(s).")
            enrich_sections_status["[ENRICH] Enrich purchase result(s) for raw Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich purchase result(s) for raw Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich purchase result(s) for raw Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich purchase result(s) for raw Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)  

    # 1.2.8. Enrich messaging result(s) for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich messaging result(s) for raw Facebook Ads ad insights"
        enrich_section_start = time.time()        
        try:
            print(f"🔍 [ENRICH] Enriching messaging result(s) for raw Facebook Ads ad insights with {len(enrich_df_purchase)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching messaging result(s) for raw Facebook Ads ad insights with {len(enrich_df_purchase)} row(s)...")
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
            print(f"✅ [ENRICH] Successfully enriched messaging result(s) for raw Facebook Ads ad insights with {len(enrich_df_message)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched messaging result(s) for raw Facebook Ads ad insights with {len(enrich_df_message)} row(s).")
            enrich_sections_status["[ENRICH] Enrich messaging result(s) for raw Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich messaging result(s) for raw Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich messaging result(s) for raw Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich messaging result(s) for raw Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.2.9. Safe cast enriched result(s) for raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Safe cast enriched result(s) for raw Facebook Ads ad insights"
        enrich_section_start = time.time()               
        try:
            print(f"🔄 [ENRICH] Casting enriched result(s to safe mode for raw acebook Ads ad insights with {len(enrich_df_message)} row(s)...")
            logging.info(f"🔄 [ENRICH] Casting enriched result(s to safe mode for raw Facebook Ads ad insights with {len(enrich_df_message)} row(s)...")    
            enrich_df_casted = enrich_df_message.copy()
            enrich_df_casted = enrich_df_casted.assign(
                enrich_results_value=lambda df: pd.to_numeric(df["enrich_results_value"], errors="coerce"),
                enrich_results_type=lambda df: df["enrich_results_type"].astype("string"),
                enrich_purchases_value=lambda df: pd.to_numeric(df["enrich_purchases_value"], errors="coerce"),
                enrich_messages_value=lambda df: pd.to_numeric(df["enrich_messages_value"], errors="coerce"),
            )
            enriched_summary_casted = {
                "results_value_nan": enrich_df_casted["enrich_results_value"].isna().sum(),
                "purchases_value_nan": enrich_df_casted["enrich_purchases_value"].isna().sum(),
                "messages_value_nan": enrich_df_casted["enrich_messages_value"].isna().sum(),
            }
            print(f"✅ [ENRICH] Successfully casted enriched result(s) for raw Facebook Ads ad insights with {len(enrich_df_casted)} and coerced summary {enriched_summary_casted}.")
            logging.info(f"✅ [ENRICH] Successfully casted enriched result(s) for raw Facebook Ads ad insights with {len(enrich_df_casted)} and coerced summary {enriched_summary_casted}.")
            enrich_sections_status["[ENRICH] Safe cast enriched result(s) for raw Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Safe cast enriched result(s) for raw Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for raw Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to cast enriched result(s) to safe mode for raw Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.2.10. Normalize enriched numeric field(s) of raw Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Normalize enriched numeric field(s) of raw Facebook Ads ad insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Normalizing enriched field(s) of Facebook Ads ad insights to standard schema with {len(enrich_df_casted)} row(s)...")
            logging.info(f"🔄 [ENRICH] Normalizing enriched field(s) of Facebook Ads ad insights to standard schema with {len(enrich_df_casted)} row(s)...")
            enrich_df_finalized = enrich_df_casted.copy()
            enrich_df_finalized = enrich_df_finalized.assign(
                **{
                    col: pd.to_numeric(enrich_df_finalized[col], errors="coerce").fillna(0)
                    for col in ["reach", "impressions", "clicks"]
                    if col in enrich_df_finalized.columns
                },
                result=lambda df: df["enrich_results_value"],
                result_type=lambda df: df["enrich_results_type"],
                purchase=lambda df: df["enrich_purchases_value"],
                messaging_conversations_started=lambda df: df["enrich_messages_value"],
            )
            print(f"✅ [ENRICH] Successfully normalized enriched field(s) of Facebook Ads ad insights to standard schema with {len(enrich_df_finalized)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully normalized enriched field(s) of Facebook Ads ad insights to standard schema with {len(enrich_df_finalized)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to normalize enriched field(s) of Facebook Ads ad insights to standard schema due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to normalize enriched field(s) of Facebook Ads ad insights to standard schema due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)              

    # 1.2.11. Summarize enrichment result(s) for raw Facebook Ads ad insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_finalized.copy() if not enrich_df_finalized.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete raw Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to unsuccessful section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete raw Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to unsuccessful section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed raw Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed raw Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"        
        enrich_sections_detail = {
            section: {
                "status": enrich_sections_status.get(section, "unknown"),
                "time": enrich_sections_time.get(section, None),
            }
            for section in set(enrich_sections_status) | set(enrich_sections_time)
        }        
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }
    return enrich_results_final 

# 2. ENRICH FACEBOOK ADS INSIGHTS FROM STAGING PHASE

# 2.1. Enrich Facebook Ads campaign insights from staging phase
def enrich_campaign_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")  

    # 2.1.1. Start timing the staging Facebook Ads campaign insights enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    enrich_section_name = "[ENRICH] Start timing the staging Facebook Ads campaign insights enrichment"
    enrich_sections_status[enrich_section_name] = "succeed"    
    print(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    # 2.1.2. Validate input for the staging Facebook Ads campaign insights enrichment
    enrich_section_name = "[ENRICH] Validate input for the staging Facebook Ads campaign insights enrichment"
    enrich_section_start = time.time()    
    try:
        if enrich_df_input.empty:
            enrich_sections_status["[ENRICH] Validate input for the staging Facebook Ads campaign insights enrichment"] = "failed"
            print("⚠️ [ENRICH] Empty staging Facebook Ads campaign insights provided then enrichment is suspended.")
            logging.warning("⚠️ [ENRICH] Empty staging Facebook Ads campaign insights provided then enrichment is suspended.")
            raise ValueError("⚠️ [ENRICH] Empty staging Facebook Ads campaign insights provided then enrichment is suspended.")
        else:
            enrich_sections_status["[ENRICH] Validate input for the staging Facebook Ads campaign insights enrichment"] = "succeed"
            print("✅ [ENRICH] Successfully validated input for staging Facebook Ads campaign insights enrichment.")
            logging.info("✅ [ENRICH] Successfully validated input for staging Facebook Ads campaign insights enrichment.")
    finally:
        enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)            

    try:

    # 2.1.3. Enrich table-level field(s) for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich table-level field(s) for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()            
        try: 
            print(f"🔍 [ENRICH] Enriching table-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_input)} row(s)...")            
            enrich_df_table = enrich_df_input.copy()
            enrich_df_table = enrich_df_table.assign(
                spend=lambda df: pd.to_numeric(df["spend"], errors="coerce").fillna(0)            )
            
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(
                r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",
                enrich_table_name
            )            
            enrich_df_table = enrich_df_table.assign(
                enrich_account_platform=match.group("platform") if match else "unknown",
                enrich_account_department=match.group("department") if match else "unknown",
                enrich_account_name=match.group("account") if match else "unknown"
            )            
            print(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s).")
            enrich_sections_status["[ENRICH] Enrich table-level field(s) for staging Facebook Ads campaign insights"] = "succeed"        
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich table-level field(s) for staging Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   
        
    # 2.1.4. Enrich campaign-level field(s) for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich campaign-level field(s) for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign = (
                enrich_df_campaign
                .assign(
                    enrich_campaign_objective=lambda df: df["campaign_name"].str.split("_").str[0],
                    enrich_campaign_region=lambda df: df["campaign_name"].str.split("_").str[1],
                    enrich_budget_group=lambda df: df["campaign_name"].str.split("_").str[2],
                    enrich_budget_type=lambda df: df["campaign_name"].str.split("_").str[3],
                    enrich_category_group=lambda df: df["campaign_name"].str.split("_").str[4],
                    enrich_campaign_personnel=lambda df: df["campaign_name"].str.split("_").str[5],
                    enrich_program_group=lambda df: df["campaign_name"].str.split("_").str[7],
                    enrich_program_type=lambda df: df["campaign_name"].str.split("_").str[8],
                    date_start=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                    date_stop=lambda df: pd.to_datetime(df["date_stop"], errors="coerce", utc=True).dt.floor("D") + timedelta(hours=23, minutes=59, seconds=59),
                    date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                )
            )
            vietnamese_map_all = {
                'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
                'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
                'đ': 'd',
                'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
                'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
                'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
                'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
                'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
            }
            vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map_all.items()}
            full_map = {**vietnamese_map_all, **vietnamese_map_upper}
            enrich_df_campaign["enrich_campaign_personnel"] = (
                enrich_df_campaign["enrich_campaign_personnel"]
                .apply(lambda x: ''.join(full_map.get(c, c) for c in x) if isinstance(x, str) else x)
            )
            enrich_df_campaign = enrich_df_campaign.assign(
                year=lambda df: df["date"].dt.strftime("%Y"),
                month=lambda df: df["date"].dt.strftime("%Y-%m"),
                last_updated_at=datetime.utcnow().replace(tzinfo=pytz.UTC)
            )            
            print(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging Facebook Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            enrich_sections_status["[ENRICH] Enrich campaign-level field(s) for staging Facebook Ads campaign insights"] = "succeed"        
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich campaign-level field(s) for staging Facebook Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   
    
    # 2.1.5. Summarize enrichment result(s) for staging Facebook campaign insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_campaign.copy() if not enrich_df_campaign.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)        
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) output in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) output in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"        
        enrich_sections_detail = {
            section: {
                "status": enrich_sections_status.get(section, "unknown"),
                "time": enrich_sections_time.get(section, None),
            }
            for section in set(enrich_sections_status) | set(enrich_sections_time)
        }          
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }
    return enrich_results_final

# 2.2. Enrich Facebook Ads ad insights from staging phase
def enrich_ad_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:   
    print(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads ad insights for {len(enrich_df_input)}...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads ad insights for {len(enrich_df_input)}...")    

    # 2.2.1. Start timing the staging Facebook Ads ad insights enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    enrich_section_name = "[ENRICH] Start timing the staging Facebook Ads ad insights enrichment"
    enrich_sections_status[enrich_section_name] = "succeed"    
    print(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 2.2.2. Validate input for the staging Facebook Ads ad insights enrichment
    enrich_section_name = "[ENRICH] Validate input for the staging Facebook Ads ad insights enrichment"
    enrich_section_start = time.time()    
    try:
        if enrich_df_input.empty:
            enrich_sections_status["[ENRICH] Validate input for the staging Facebook Ads ad insights enrichment"] = "failed"
            print("⚠️ [ENRICH] Empty staging Facebook Ads ad insights provided then enrichment is suspended.")
            logging.warning("⚠️ [ENRICH] Empty staging Facebook Ads ad insights provided then enrichment is suspended.")
            raise ValueError("⚠️ [ENRICH] Empty staging Facebook Ads ad insights provided then enrichment is suspended.")
        else:
            enrich_sections_status["[ENRICH] Validate input for the staging Facebook Ads ad insights enrichment"] = "succeed"
            print("✅ [ENRICH] Successfully validated input for staging Facebook Ads ad insights enrichment.")
            logging.info("✅ [ENRICH] Successfully validated input for staging Facebook Ads ad insights enrichment.")
    finally:
        enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)            

    try:
    
    # 2.2.3. Enrich table-level field(s) for staging Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich table-level field(s) for staging Facebook Ads ad insightst"
        enrich_section_start = time.time()   
        try:
            print(f"🔍 [ENRICH] Enriching table-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_input)} row(s)...")   
            enrich_df_table = enrich_df_input.copy()
            enrich_df_table["spend"] = pd.to_numeric(enrich_df_table["spend"], errors="coerce").fillna(0)
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$", enrich_table_name)
            enrich_df_table = enrich_df_table.assign(
                spend=lambda df: pd.to_numeric(df["spend"], errors="coerce").fillna(0),
                enrich_account_platform=match.group("platform") if match else None,
                enrich_account_department=match.group("department") if match else None,
                enrich_account_name=match.group("account") if match else None
            )
            print(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s).")
            enrich_sections_status["[ENRICH] Enrich table-level field(s) for staging Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich table-level field(s) for staging Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)        

    # 2.2.4. Enrich campaign-level field(s) for Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich table-level field(s) for staging Facebook Ads ad insightst"
        enrich_section_start = time.time()  
        try:
            print(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign = (
                enrich_df_campaign
                .assign(
                    enrich_campaign_objective=lambda df: df["campaign_name"].str.split("_").str[0],
                    enrich_campaign_region=lambda df: df["campaign_name"].str.split("_").str[1],
                    enrich_budget_group=lambda df: df["campaign_name"].str.split("_").str[2],
                    enrich_budget_type=lambda df: df["campaign_name"].str.split("_").str[3],
                    enrich_category_group=lambda df: df["campaign_name"].str.split("_").str[4],
                    enrich_campaign_personnel=lambda df: df["campaign_name"].str.split("_").str[5],
                    enrich_program_group=lambda df: df["campaign_name"].str.split("_").str[7],
                    enrich_program_type=lambda df: df["campaign_name"].str.split("_").str[8],
                    date_start=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                    date_stop=lambda df: pd.to_datetime(df["date_stop"], errors="coerce", utc=True).dt.floor("D") + timedelta(hours=23, minutes=59, seconds=59),
                    date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                )
            )
            vietnamese_map_all = {
                'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
                'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
                'đ': 'd',
                'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
                'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
                'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
                'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
                'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
            }
            vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map_all.items()}
            full_map = {**vietnamese_map_all, **vietnamese_map_upper}
            enrich_df_campaign["enrich_campaign_personnel"] = (
                enrich_df_campaign["enrich_campaign_personnel"]
                .apply(lambda x: ''.join(full_map.get(c, c) for c in x) if isinstance(x, str) else x)
            )
            enrich_df_campaign = enrich_df_campaign.assign(
                year=lambda df: df["date"].dt.strftime("%Y"),
                month=lambda df: df["date"].dt.strftime("%Y-%m"),
                last_updated_at=datetime.utcnow().replace(tzinfo=pytz.UTC)
            )
            print(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s).")
            enrich_sections_status["[ENRICH] Enrich campaign-level field(s) for Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich campaign-level field(s) for Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)        

    # 2.2.5. Enrich adset-level field(s) for Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich adset-level field(s) for Facebook Ads ad insights"
        enrich_section_start = time.time()         
        try:
            print(f"🔍 [ENRICH] Enriching adset-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching adset-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            enrich_df_adset = enrich_df_campaign.copy()
            enrich_df_adset = enrich_df_adset.assign(
                enrich_adset_location=lambda df: df["adset_name"].fillna("").str.split("_").str[0].fillna("unknown"),
                enrich_adset_audience=lambda df: df["adset_name"].fillna("").str.split("_").str[1].fillna("unknown"),
                enrich_adset_format=lambda df: df["adset_name"].fillna("").str.split("_").str[2].fillna("unknown"),
                enrich_program_strategy=lambda df: df["adset_name"].fillna("").str.split("_").str[3].fillna("unknown"),
                enrich_program_subtype=lambda df: df["adset_name"].fillna("").str.split("_").str[4].fillna("unknown")
            )
            print(f"✅ [ENRICH] Successfully enriched adset-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched adset-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s).")
            enrich_sections_status["[ENRICH] Enrich adset-level field(s) for Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich adset-level field(s) for Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich adset-level field(s) for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich adset-level field(s) for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 2.2.6. Enrich other ad-level field(s) for Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich adset-level field(s) for Facebook Ads ad insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching ad-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching ad-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s)...")
            enrich_df_ad = enrich_df_adset.copy()
            enrich_df_ad = enrich_df_ad.assign(
                date_start=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                date_stop=lambda df: pd.to_datetime(df["date_stop"], errors="coerce", utc=True).dt.floor("D") + timedelta(hours=23, minutes=59, seconds=59),
                date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                year=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y"),
                month=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC)
            )
            print(f"✅ [ENRICH] Successfully enriched ad-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_ad)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched ad-level field(s) for staging Facebook Ads ad insights with {len(enrich_df_ad)} row(s).")
            enrich_sections_status["[ENRICH] Enrich other ad-level field(s) for Facebook Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["[ENRICH] Enrich other ad-level field(s) for Facebook Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich ad-level field(s) for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich ad-level field(s) for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 2.2.7. Summarize enrich result(s) for staging Facebook ad insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_ad.copy() if not enrich_df_ad.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"        
        enrich_sections_detail = {
            section: {
                "status": enrich_sections_status.get(section, "unknown"),
                "time": enrich_sections_time.get(section, None),
            }
            for section in set(enrich_sections_status) | set(enrich_sections_time)
        }          
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }    
    return enrich_results_final