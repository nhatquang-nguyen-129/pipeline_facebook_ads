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

✔️ Maps `ptimization_goal to its corresponding business action type  
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
from datetime import datetime

# Add Python JSON ultilities for integration
import json 

# Add Python logging ultilities for integraton
import logging

# Add Python regular expression operations libraries for integraton
import re

# Add Python time ultilities for integration
import time

# Add Python NumPy libraries for integration
import numpy as np

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python timezone ultilities for integration
import pytz

# Get optimization_goal mapping to action_type used for calculating main result type
ENRICH_ACTIONS_MAPPING = {
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

# 1. ENRICH FACEBOOK ADS INSIGHTS FROM STAGING PHASE

# 1.1. Enrich Facebook Ads campaign insights from staging phase
def enrich_campaign_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s)...")  

    # 1.1.1. Start timing the staging Facebook Ads campaign insights enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    print(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Validate input for the staging Facebook Ads campaign insights enrichment
        enrich_section_name = "[ENRICH] Validate input for the staging Facebook Ads campaign insights enrichment"
        enrich_section_start = time.time()    
        try:
            if enrich_df_input.empty:
                enrich_sections_status[enrich_section_name] = "failed"
                print("⚠️ [ENRICH] Empty staging Facebook Ads campaign insights provided then enrichment is suspended.")
                logging.warning("⚠️ [ENRICH] Empty staging Facebook Ads campaign insights provided then enrichment is suspended.")
            else:
                print("✅ [ENRICH] Successfully validated input for staging Facebook Ads campaign insights enrichment.")
                logging.info("✅ [ENRICH] Successfully validated input for staging Facebook Ads campaign insights enrichment.")
                enrich_sections_status[enrich_section_name] = "succeed"
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.1.3. Enrich goal to action for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich goal to action for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching goal to action for staging Facebook Ads campaign insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching goal to action for staging Facebook Ads campaign insights with {len(enrich_df_input)} row(s)...")
            enrich_df_goal = enrich_df_input.copy()
            enrich_df_goal["actions"] = enrich_df_goal["actions"].apply(
                lambda x: (
                    x 
                    if isinstance(x, list)
                    else []
                    if (
                        x is None
                        or (isinstance(x, float) and np.isnan(x))
                        or (isinstance(x, str) and x.strip() in ["", "None", "null", "NULL"])
                    )
                    else (
                        (lambda cleaned: (
                            json.loads(cleaned)
                            if isinstance(cleaned, str) else []
                        ))(
                            re.sub(
                                r"(?<!\")'([^']*?)':",
                                r'"\1":',
                                x.replace("'", '"')
                            ).strip()
                            if isinstance(x, str) else x
                        )
                    )
                    if isinstance(x, str) else []
                )
            )
            enrich_results_value = []
            enrich_results_type = []
            for idx, row in enrich_df_goal.iterrows():
                enrich_action_mapping = row.get("optimization_goal")
                actions = row.get("actions", [])
                if not isinstance(actions, list):
                    actions = []
                enrich_result_value = None
                enrich_result_type = None
                if enrich_action_mapping in ENRICH_ACTIONS_MAPPING:
                    enrich_action_type = ENRICH_ACTIONS_MAPPING[enrich_action_mapping]
                    if enrich_action_type in ["reach", "impressions"]:
                        enrich_result_value = pd.to_numeric(row.get("impressions", 0), errors="coerce")
                        enrich_result_type = "impressions"
                    else:
                        for act in actions:
                            if act.get("action_type") == enrich_action_type:
                                try:
                                    enrich_result_value = int(act.get("value", 0))
                                except Exception:
                                    print(f"⚠️ [ENRICH] Failed to convert staging Facebook Ads campaign insights value to int for action type {enrich_action_type}.")
                                    logging.warning(f"⚠️ [ENRICH] Failed to convert staging Facebook Ads campaign insights value to int for action type {enrich_action_type}.")
                                    value = None
                                break
                        enrich_result_type = "follows_or_likes" if enrich_action_type == "like" else enrich_action_type
                if enrich_result_value is None:
                    for act in actions:
                        if act.get("action_type") == "onsite_conversion.lead_grouped":
                            try:
                                enrich_result_value = int(act.get("value", 0))
                            except Exception:
                                enrich_result_value = None
                            enrich_result_type = "onsite_conversion.lead_grouped"
                            break
                if enrich_result_value is None:
                    for act in actions:
                        if act.get("action_type") == "lead":
                            try:
                                enrich_result_value = int(act.get("value", 0))
                            except Exception:
                                enrich_result_value = None
                            enrich_result_type = "lead"
                            break
                enrich_results_value.append(enrich_result_value)
                enrich_results_type.append(enrich_result_type)
            enrich_df_goal["enrich_results_value"] = enrich_results_value
            enrich_df_goal["enrich_results_type"] = enrich_results_type
            enrich_df_goal = enrich_df_goal.assign(
                enrich_results_value=lambda df: pd.to_numeric(df["enrich_results_value"], errors="coerce").fillna(0),
                enrich_results_type=lambda df: df["enrich_results_type"].astype("string").fillna("unknown"),
                result=lambda df: df["enrich_results_value"],
                result_type=lambda df: df["enrich_results_type"],
                impressions=lambda df: pd.to_numeric(df["impressions"], errors="coerce").fillna(0) if "impressions" in df.columns else 0,
                clicks=lambda df: pd.to_numeric(df["clicks"], errors="coerce").fillna(0) if "clicks" in df.columns else 0
            )
            enrich_df_goal = enrich_df_goal.drop(columns=["optimization_goal"])
            print(f"✅ [ENRICH] Successfully enriched goal to action metrics for staging Facebook Ads campaign insights with {len(enrich_df_goal)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched goal to action metrics for staging Facebook Ads campaign insights with {len(enrich_df_goal)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich goal to action metrics for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich goal to action metrics for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2) 

    # 1.1.4. Enrich performance result for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich performance result for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching performance result for staging Facebook Ads campaign insights with {len(enrich_df_goal)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching performance result for staging Facebook Ads campaign insights with {len(enrich_df_goal)} row(s)...")
            enrich_df_performance = enrich_df_goal.copy()
            enrich_messages_value = []
            enrich_purchases_value = []
            for idx, row in enrich_df_performance.iterrows():
                actions = row.get("actions", [])
                if not isinstance(actions, list):
                    actions = []
                enrich_message_value = None
                enrich_purchase_value = None
                for act in actions:
                    if not isinstance(act, dict):
                        continue
                    ingest_action_type = act.get("action_type")
                    ingest_action_value = act.get("value", 0)
                    if ingest_action_type == "onsite_conversion.messaging_conversation_started_7d":
                        try:
                            enrich_message_value = int(ingest_action_value)
                        except:
                            enrich_message_value = None
                    elif ingest_action_type == "purchase":
                        try:
                            enrich_purchase_value = int(ingest_action_value)
                        except:
                            enrich_purchase_value = None
                enrich_messages_value.append(enrich_message_value)
                enrich_purchases_value.append(enrich_purchase_value)
            enrich_df_performance = enrich_df_performance.assign(
                enrich_messages_value=pd.Series(enrich_messages_value).fillna(0).astype(int),
                enrich_purchases_value=pd.Series(enrich_purchases_value).fillna(0).astype(int),
                messaging_conversations_started=lambda df: df["enrich_messages_value"],
                purchase=lambda df: df["enrich_purchases_value"]
            )
            enrich_df_performance = enrich_df_performance.drop(columns=["actions"])
            print(f"✅ [ENRICH] Successfully enriched performance result for staging Facebook Ads campaign insights with {len(enrich_df_performance)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched performance result for staging Facebook Ads campaign insights with {len(enrich_df_performance)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich performance result for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich performance result for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.1.5. Enrich table fields for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich table fields for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()            
        try: 
            print(f"🔍 [ENRICH] Enriching table fields for staging Facebook Ads campaign insights with {len(enrich_df_performance)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table fields for staging Facebook Ads campaign insights with {len(enrich_df_performance)} row(s)...")
            enrich_df_table = enrich_df_performance.copy()
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
            print(f"✅ [ENRICH] Successfully enriched table fields for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table fields for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"        
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table fields for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table fields for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   
        
    # 1.1.6. Enrich campaign fields for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich campaign fields for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching campaign fields for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign fields for staging Facebook Ads campaign insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign = (
                enrich_df_campaign
                .assign(
                    enrich_campaign_objective=lambda df: df["campaign_name"].str.split("_").str[0].fillna("unknown"),
                    enrich_campaign_region=lambda df: df["campaign_name"].str.split("_").str[1].fillna("unknown"),
                    enrich_budget_group=lambda df: df["campaign_name"].str.split("_").str[2].fillna("unknown"),
                    enrich_budget_type=lambda df: df["campaign_name"].str.split("_").str[3].fillna("unknown"),
                    enrich_category_group=lambda df: df["campaign_name"].str.split("_").str[4].fillna("unknown"),
                    enrich_campaign_personnel=lambda df: df["campaign_name"].str.split("_").str[5].fillna("unknown"),
                    enrich_program_track=lambda df: df["campaign_name"].str.split("_").str[7].fillna("unknown"),
                    enrich_program_group=lambda df: df["campaign_name"].str.split("_").str[8].fillna("unknown"),
                    enrich_program_type=lambda df: df["campaign_name"].str.split("_").str[9].fillna("unknown"),
                )
            )
            vietnamese_accents_mapping = {
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
            vietnamese_cases_upper = {k.upper(): v.upper() for k, v in vietnamese_accents_mapping.items()}
            vietnamese_characters_all = {**vietnamese_accents_mapping, **vietnamese_cases_upper}
            enrich_df_campaign["enrich_campaign_personnel"] = (
                enrich_df_campaign["enrich_campaign_personnel"]
                .apply(lambda x: ''.join(vietnamese_characters_all.get(c, c) for c in x) if isinstance(x, str) else x)
            )           
            print(f"✅ [ENRICH] Successfully enriched campaign fields for staging Facebook Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign fields for staging Facebook Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"        
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign fields for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign fields for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.1.7. Enrich date fields for staging Facebook Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich date fields for staging Facebook Ads campaign insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching date fields for staging Facebook Ads campaign insights with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching date fields for staging Facebook Ads campaign insights with {len(enrich_df_campaign)} row(s)...")
            enrich_df_date = enrich_df_campaign.copy()
            enrich_df_date = enrich_df_date.assign(           
                date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                year=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y"),
                month=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC),
            ).drop(columns=["date_start", "date_stop"], errors="ignore")
            print(f"✅ [ENRICH] Successfully enriched date fields for staging Facebook Ads campaign insights with {len(enrich_df_date)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date fields for staging Facebook Ads campaign insights with {len(enrich_df_date)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date fields for staging Facebook Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich date fields for staging Facebook Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)    

    # 1.1.8. Summarize enrichment result(s) for staging Facebook Ads campaign insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_date.copy() if not enrich_df_date.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) output in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging Facebook Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) output in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"                
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

# 1.2. Enrich Facebook Ads ad insights from staging phase
def enrich_ad_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:   
    print(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads ad insights for {len(enrich_df_input)}...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging Facebook Ads ad insights for {len(enrich_df_input)}...")    

    # 1.2.1. Start timing the staging Facebook Ads ad insights enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    print(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging Facebook Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.2.2. Validate input for the staging Facebook Ads ad insights enrichment
        enrich_section_name = "[ENRICH] Validate input for the staging Facebook Ads ad insights enrichment"
        enrich_section_start = time.time()    
        try:
            if enrich_df_input.empty:
                enrich_sections_status[enrich_section_name] = "failed"
                print("⚠️ [ENRICH] Empty staging Facebook Ads ad insights provided then enrichment is suspended.")
                logging.warning("⚠️ [ENRICH] Empty staging Facebook Ads ad insights provided then enrichment is suspended.")
            else:
                print("✅ [ENRICH] Successfully validated input for staging Facebook Ads ad insights enrichment.")
                logging.info("✅ [ENRICH] Successfully validated input for staging Facebook Ads ad insights enrichment.")
                enrich_sections_status[enrich_section_name] = "succeed"
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.2.3. Enrich goal to action for staging Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich goal to action for staging Facebook Ads ad insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching goal to action for staging Facebook Ads ad insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching goal to action for staging Facebook Ads ad insights with {len(enrich_df_input)} row(s)...")
            enrich_df_goal = enrich_df_input.copy()
            enrich_df_goal["actions"] = enrich_df_goal["actions"].apply(
                lambda x: (
                    x if isinstance(x, list) else
                    [] if x is None or (isinstance(x, float) and np.isnan(x)) or (isinstance(x, str) and x.strip() in ["", "None"]) else
                    (lambda s: (
                        json.loads(re.sub(r"(?<!\")'([^']*?)':", r'"\1":', s))
                    ) if isinstance(x, str) else [])
                )
            )
            enrich_results_value = []
            enrich_results_type = []
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
                                    enrich_result_value = (
                                        0 if act.get("value") is None 
                                        else int(float(act.get("value"))) if str(act.get("value")).strip().lower() not in ["none", ""] 
                                        else 0
                                    )
                                except Exception:
                                    print(f"⚠️ [ENRICH] Failed to convert staging Facebook Ads ad insights value to float for action_type {action_type}.")
                                    logging.warning(f"⚠️ [ENRICH] Failed to convert staging Facebook Ads ad insights value to float for action_type {action_type}.")
                                    enrich_result_value = 0
                                break
                        enrich_result_type = "follows_or_likes" if action_type == "like" else action_type

                if enrich_result_value is None:
                    for act in actions:
                        if act.get("action_type") == "onsite_conversion.lead_grouped":
                            try:
                                enrich_result_value = (
                                    0 if act.get("value") is None 
                                    else int(float(act.get("value"))) if str(act.get("value")).strip().lower() not in ["none", ""] 
                                    else 0
                                )
                            except Exception:
                                enrich_result_value = 0
                            enrich_result_type = "onsite_conversion.lead_grouped"
                            break

                if enrich_result_value is None:
                    for act in actions:
                        if act.get("action_type") == "lead":
                            try:
                                enrich_result_value = (
                                    0 if act.get("value") is None 
                                    else int(float(act.get("value"))) if str(act.get("value")).strip().lower() not in ["none", ""] 
                                    else 0
                                )
                            except Exception:
                                enrich_result_value = 0
                            enrich_result_type = "lead"
                            break
                enrich_results_value.append(enrich_result_value)
                enrich_results_type.append(enrich_result_type)
            enrich_df_goal["enrich_results_value"] = enrich_results_value
            enrich_df_goal["enrich_results_type"] = enrich_results_type
            enrich_df_goal = enrich_df_goal.assign(
                enrich_results_value=lambda df: pd.to_numeric(df["enrich_results_value"], errors="coerce").fillna(0),
                enrich_results_type=lambda df: df["enrich_results_type"].astype("string").fillna("unknown"),
                result=lambda df: df["enrich_results_value"],
                result_type=lambda df: df["enrich_results_type"],
                spend=lambda df: pd.to_numeric(df["spend"], errors="coerce").fillna(0) if "spend" in df.columns else 0,
                impressions=lambda df: pd.to_numeric(df["impressions"], errors="coerce").fillna(0) if "impressions" in df.columns else 0,
                clicks=lambda df: pd.to_numeric(df["clicks"], errors="coerce").fillna(0) if "clicks" in df.columns else 0
            )
            enrich_df_goal = enrich_df_goal.drop(columns=["optimization_goal"])
            print(f"✅ [ENRICH] Successfully enriched goal to action metrics for staging Facebook Ads ad insights with {len(enrich_df_goal)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched goal to action metrics for staging Facebook Ads ad insights with {len(enrich_df_goal)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich goal to action metrics for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich goal to action metrics for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2) 

    # 1.2.4. Enrich performance result for staging Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich performance result for staging Facebook Ads ad insights"
        enrich_section_start = time.time()
        try:
            print(f"🔄 [ENRICH] Enriching performance result for staging Facebook Ads ad insights with {len(enrich_df_goal)} row(s)...")
            logging.info(f"🔄 [ENRICH] Enriching performance result for staging Facebook Ads ad insights with {len(enrich_df_goal)} row(s)...")
            enrich_df_performance = enrich_df_goal.copy()
            enrich_messages_value = []
            enrich_purchases_value = []
            for idx, row in enrich_df_performance.iterrows():
                actions = row.get("actions", [])
                if not isinstance(actions, list):
                    actions = []
                enrich_message_value = None
                enrich_purchase_value = None
                for action in actions:
                    if action.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
                        try:
                            enrich_message_value = int(action.get("value", 0))
                        except Exception:
                            enrich_message_value = None
                    elif action.get("action_type") == "purchase":
                        try:
                            enrich_purchase_value = int(action.get("value", 0))
                        except Exception:
                            enrich_purchase_value = None                            
                enrich_messages_value.append(enrich_message_value)
                enrich_purchases_value.append(enrich_purchase_value)
            enrich_df_performance = enrich_df_performance.assign(
                enrich_messages_value=pd.Series(enrich_messages_value).fillna(0).astype(int),
                enrich_purchases_value=pd.Series(enrich_purchases_value).fillna(0).astype(int),
                messaging_conversations_started=lambda df: df["enrich_messages_value"],
                purchase=lambda df: df["enrich_purchases_value"]
            )
            enrich_df_performance = enrich_df_performance.drop(columns=["actions"])
            print(f"✅ [ENRICH] Successfully enriched performance result for staging Facebook Ads ad insights with {len(enrich_df_performance)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched performance result for staging Facebook Ads ad insights with {len(enrich_df_performance)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich performance result for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich performance result for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)
   
    # 1.2.5. Enrich table fields for staging Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich table fields for staging Facebook Ads ad insights"
        enrich_section_start = time.time()   
        try:
            print(f"🔍 [ENRICH] Enriching table fields for staging Facebook Ads ad insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table fields for staging Facebook Ads ad insights with {len(enrich_df_input)} row(s)...")
            enrich_df_table = enrich_df_performance.copy()
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$", enrich_table_name)
            enrich_df_table = enrich_df_table.assign(
                spend=lambda df: pd.to_numeric(df["spend"], errors="coerce").fillna(0),
                enrich_account_platform=match.group("platform") if match else None,
                enrich_account_department=match.group("department") if match else None,
                enrich_account_name=match.group("account") if match else None
            )
            print(f"✅ [ENRICH] Successfully enriched table fields for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table fields for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table fields for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table fields for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)        

    # 1.2.6. Enrich campaign fields for Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich campaign fields for Facebook Ads ad insights"
        enrich_section_start = time.time()  
        try:
            print(f"🔍 [ENRICH] Enriching campaign field(s) for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign field(s) for staging Facebook Ads ad insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign = (
                enrich_df_campaign
                .assign(
                    enrich_campaign_objective=lambda df: df["campaign_name"].str.split("_").str[0].fillna("unknown"),
                    enrich_campaign_region=lambda df: df["campaign_name"].str.split("_").str[1].fillna("unknown"),
                    enrich_budget_group=lambda df: df["campaign_name"].str.split("_").str[2].fillna("unknown"),
                    enrich_budget_type=lambda df: df["campaign_name"].str.split("_").str[3].fillna("unknown"),
                    enrich_category_group=lambda df: df["campaign_name"].str.split("_").str[4].fillna("unknown"),
                    enrich_campaign_personnel=lambda df: df["campaign_name"].str.split("_").str[5].fillna("unknown"),
                    enrich_program_track=lambda df: df["campaign_name"].str.split("_").str[7].fillna("unknown"),
                    enrich_program_group=lambda df: df["campaign_name"].str.split("_").str[8].fillna("unknown"),
                    enrich_program_type=lambda df: df["campaign_name"].str.split("_").str[9].fillna("unknown"),
                )
            )
            vietnamese_accents_mapping = {
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
            vietnamese_cases_upper = {k.upper(): v.upper() for k, v in vietnamese_accents_mapping.items()}
            vietnamese_characters_all = {**vietnamese_accents_mapping, **vietnamese_cases_upper}
            enrich_df_campaign["enrich_campaign_personnel"] = (
                enrich_df_campaign["enrich_campaign_personnel"]
                .apply(lambda x: ''.join(vietnamese_characters_all.get(c, c) for c in x) if isinstance(x, str) else x)
            )  
            print(f"✅ [ENRICH] Successfully enriched campaign fields for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign fields for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign fields for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign fields for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)        

    # 1.2.7. Enrich adset fields for Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich adset fields for Facebook Ads ad insights"
        enrich_section_start = time.time()         
        try:
            print(f"🔍 [ENRICH] Enriching adset fields for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching adset fields for staging Facebook Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            enrich_df_adset = enrich_df_campaign.copy()
            enrich_df_adset = enrich_df_adset.assign(
                enrich_adset_location=lambda df: df["adset_name"].fillna("").str.split("_").str[0].fillna("unknown"),
                enrich_adset_audience=lambda df: df["adset_name"].fillna("").str.split("_").str[1].fillna("unknown"),
                enrich_adset_format=lambda df: df["adset_name"].fillna("").str.split("_").str[2].fillna("unknown"),
                enrich_adset_strategy=lambda df: df["adset_name"].fillna("").str.split("_").str[3].fillna("unknown"),
                enrich_adset_subtype=lambda df: df["adset_name"].fillna("").str.split("_").str[4].fillna("unknown")
            )
            print(f"✅ [ENRICH] Successfully enriched adset fields for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched adset fields for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich adset fields for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich adset fields for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.8. Enrich date fields for Facebook Ads ad insights
        enrich_section_name = "[ENRICH] Enrich date fields for Facebook Ads ad insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching date fields for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching date fields for staging Facebook Ads ad insights with {len(enrich_df_adset)} row(s)...")
            enrich_df_date = enrich_df_adset.copy()
            enrich_df_date = enrich_df_date.assign(
                date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                year=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y"),
                month=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC),
            )
            print(f"✅ [ENRICH] Successfully enriched date fields for staging Facebook Ads ad insights with {len(enrich_df_date)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date fields for staging Facebook Ads ad insights with {len(enrich_df_date)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date fields for staging Facebook Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich date fields for staging Facebook Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.9. Summarize enrich result(s) for staging Facebook ad insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_date.copy() if not enrich_df_date.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging Facebook Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"                 
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