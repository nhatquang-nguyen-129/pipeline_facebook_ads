import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd

def transform_campaign_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Facebook Ads campaign metadata
    ---------
    Workflow:
        1. Validate required columns
        2. Parse campaign naming convention
        3. Enrich platform dimension
    ---------
    Returns:
        1. DataFrame:
            Enforced campaign metadata  
    """

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Facebook Ads campaign metadata..."
    )
    print(msg)
    logging.info(msg)

    if df.empty:
        msg = "⚠️ [TRANSFORM] Empty campaign metadata then transformation will be suspended."
        print(msg)
        logging.warning(msg)
        return df

    required_cols = {
        "account_id",
        "campaign_id",
        "campaign_name"
        }
    
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError (
            "❌ [TRANSFORM] Failed to transform Facebook Ads campaign metadata due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()
    df["platform"] = "Facebook"
    df = df.assign(
        objective=df["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        budget_group=df["campaign_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        region=df["campaign_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        category_level_1=df["campaign_name"].fillna("").str.split("_").str[3].fillna("unknown"),

        track_group=df["campaign_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        pillar_group=df["campaign_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content_group=df["campaign_name"].fillna("").str.split("_").str[8].fillna("unknown"),
    )

    msg = (
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Facebook Ads campaign metadata."
    )
    print(msg)
    logging.info(msg)

    return df