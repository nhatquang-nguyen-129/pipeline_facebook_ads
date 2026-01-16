import json
import re
import numpy as np
import pandas as pd

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

def transform_campaign_insights(df_input: pd.DataFrame) -> pd.DataFrame:

    if df_input.empty:
        return df_input.copy()

    df = df_input.copy()

    # Parse actions
    parsed_actions = []

    for x in df["actions"]:
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

    # Resolve common main result
    results_value = []
    results_type = []

    for _, row in df.iterrows():
        optimization_goal = row.get("optimization_goal")
        actions = row.get("actions", [])

        value = None
        rtype = None

        if optimization_goal in ENRICH_ACTIONS_MAPPING:
            mapped_action = ENRICH_ACTIONS_MAPPING[optimization_goal]

            if mapped_action in ["reach", "impressions"]:
                value = pd.to_numeric(row.get("impressions", 0), errors="coerce")
                rtype = "impressions"
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

    # Resolve lead_grouped main result
        if value is None:
            for act in actions:
                if act.get("action_type") == "onsite_conversion.lead_grouped":
                    value = pd.to_numeric(act.get("value", 0), errors="coerce")
                    rtype = "onsite_conversion.lead_grouped"
                    break

    # Resolve lead main result
        if value is None:
            for act in actions:
                if act.get("action_type") == "lead":
                    value = pd.to_numeric(act.get("value", 0), errors="coerce")
                    rtype = "lead"
                    break

        results_value.append(value if value is not None else 0)
        results_type.append(rtype if rtype is not None else "unknown")

    df["result"] = pd.to_numeric(results_value, errors="coerce").fillna(0)
    df["result_type"] = pd.Series(results_type, dtype="string").fillna("unknown")

    # Extract most-used metrics
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

    # Normalize numeric fields
    if "impressions" in df.columns:
        df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0)

    if "clicks" in df.columns:
        df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0)

    # Drop pre-parsing columns
    df = df.drop(columns=[
        "actions", 
        "optimization_goal"
        ], errors="ignore")

    return df