import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import json
import numpy as np
import pandas as pd
import re

def transform_ad_insights(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Facebook Ads ad insights
    ---
    Principles:
        1. Validate input
        2. Parse actions
        3. Resolve results
        4. Normalize date dimension
        5. Enforce numeric schema
    ---
    Returns:
        1. pandas.DataFrame:
            Enforced Facebook Ads ad insights records
    """

    # Validate input
    print(
        "🔄 [TRANSFORM] Validating column(s) for "
        f"{len(df)} row(s) of Facebook Ads ad insights..."
    )

    if df.empty:

        raise ValueError(
            "❌ [TRANSFORM] Failed to validate column(s) for Facebook Ads ad insights due to empty input DataFrame."
        )

    required_cols = {
        "date_start",
        "date_stop",
    }

    actual_cols = {
        str(col).strip()
        for col in df.columns
    }

    missing_cols = required_cols - actual_cols

    extra_cols = actual_cols - required_cols

    print(
        "✅ [TRANSFORM] Successfully validated DataFrame for Facebook Ads ad insights with "
        f"{df.shape} shape with total column(s) "
        f"{len(actual_cols)}/{len(required_cols)} total column including "
        f"{len(missing_cols)} missing column(s) and "
        f"{len(extra_cols)} extra column(s)."
    )

    if missing_cols:

        raise ValueError(
            "❌ [TRANSFORM] Failed to transform validated DataFrame for Facebook Ads ad insights due to missing required column(s) "
            f"{sorted(missing_cols)}"
        )

    # Parse actions columns
    _MAPPING_GOAL_ACTION = {
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

    parsed_actions = []

    for x in df.get("actions", []):
        
        if isinstance(x, list):
        
            parsed_actions.append(x)
        
        elif x is None or (isinstance(x, float) and np.isnan(x)):
        
            parsed_actions.append([])
        
        elif isinstance(x, str):
        
            cleaned = x.strip().lower()
        
            if cleaned in ["", "none", "null"]:
        
                parsed_actions.append([])
        
            else:
        
                try:
        
                    normalized = re.sub(
                        r"(?<!\")'([^']*?)':",
                        r'"\1":',
                        x.replace("'", '"')
                    )
        
                    parsed = json.loads(normalized)
        
                    parsed_actions.append(parsed if isinstance(parsed, list) else [])
        
                except Exception:
        
                    parsed_actions.append([])
        
        else:
        
            parsed_actions.append([])

    df["actions"] = parsed_actions

    # Parse result columns
    results_value = []
    
    results_type = []

    for _, row in df.iterrows():
    
        optimization_goal = row.get("optimization_goal")
    
        actions = row.get("actions", [])

        value = None
    
        rtype = None

        if optimization_goal in _MAPPING_GOAL_ACTION:
    
            mapped_action = _MAPPING_GOAL_ACTION[optimization_goal]

            if mapped_action in {"reach", "impressions"}:
    
                value = pd.to_numeric(row.get("impressions", 0), errors="coerce")
    
                rtype = mapped_action
    
            else:
    
                for act in actions:
    
                    if act.get("action_type") == mapped_action:
    
                        value = pd.to_numeric(act.get("value", 0), errors="coerce")
    
                        rtype = (
                            "follows_or_likes"
                            if mapped_action == "like"
                            else mapped_action
                        )
    
                        break

        if value is None:
    
            for act in actions:
    
                if act.get("action_type") == "onsite_conversion.lead_grouped":
    
                    value = pd.to_numeric(act.get("value", 0), errors="coerce")
    
                    rtype = "onsite_conversion.lead_grouped"
    
                    break

        if value is None:
    
            for act in actions:
    
                if act.get("action_type") == "lead":
    
                    value = pd.to_numeric(act.get("value", 0), errors="coerce")
    
                    rtype = "lead"
    
                    break

        results_value.append(value if value is not None else 0)
    
        results_type.append(rtype if rtype else "unknown")

    df["result"] = (pd.to_numeric(pd.Series(results_value), errors="coerce").fillna(0))
    
    df["result_type"] = pd.Series(results_type, dtype="string").fillna("unknown")

    # Parse performance columns
    messaging_values = []
    
    purchase_values = []

    for actions in df["actions"]:
    
        msg_val = 0
    
        pur_val = 0

        for act in actions:
    
            if not isinstance(act, dict):
    
                continue
    
            if act.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
    
                msg_val = pd.to_numeric(act.get("value", 0), errors="coerce")
    
            elif act.get("action_type") == "purchase":
    
                pur_val = pd.to_numeric(act.get("value", 0), errors="coerce")

        messaging_values.append(int(msg_val) if not pd.isna(msg_val) else 0)
    
        purchase_values.append(int(pur_val) if not pd.isna(pur_val) else 0)

    df["messaging_conversations_started"] = messaging_values
    
    df["purchase"] = purchase_values

    # Normalize numeric metrics
    for col in [
        "impressions", 
        "clicks", 
        "spend"
    ]:
    
        if col in df.columns:
    
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Normalize date dimension
    if "date_start" in df.columns:
    
        dt = pd.to_datetime(df["date_start"], errors="coerce", utc=True)
    
        df["date"] = dt.dt.floor("D")
    
        df["year"] = dt.dt.year
    
        df["month"] = dt.dt.strftime("%Y-%m")

    # Drop raw columns
    df = df.drop(
        columns=["actions", "optimization_goal", "date_start", "date_stop"],
        errors="ignore"
    )

    print(
        "✅ [TRANSFORM] Successfully transformed Facebook Ads ad insights with "
        f"{len(df)} row(s)."
    )

    return df