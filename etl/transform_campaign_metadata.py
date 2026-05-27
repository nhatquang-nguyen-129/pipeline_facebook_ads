import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_campaign_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Facebook Ads campaign metadata
    ---
    Principles:
        1. Validate input Dataframe
        2. Validate required schema columns
        3. Create copy to prevent side effects
        4. Parse structured naming convention
        5. Enrich Dataframe
    ---
    Returns:
        1. pandas.DataFrame:
            Enforced Facebook Ads campaign metadata records
    """

    print(
        "🔄 [TRANSFORM] Transforming Facebook Ads campaign metadata with "
        f"{len(df)} row(s)..."
    )

    if df.empty:
        
        print(
            "⚠️ [TRANSFORM] Failed to transform Facebook Ads campaign metadata due to no input DataFrame then transformation will be suspended."
        )
        
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
        objective=df["campaign_name"].fillna("").str.split("|").str[0].fillna("unknown"),
        budget_group=df["campaign_name"].fillna("").str.split("|").str[1].fillna("unknown"),        
        region=df["campaign_name"].fillna("").str.split("|").str[2].fillna("unknown"),
        category_level_1=df["campaign_name"].fillna("").str.split("|").str[3].fillna("unknown"),
        optimization=df["campaign_name"].fillna("").str.split("|").str[6].fillna("unknown"),
        track=df["campaign_name"].fillna("").str.split("|").str[7].fillna("unknown"),
        pillar=df["campaign_name"].fillna("").str.split("|").str[8].fillna("unknown"),
        group=df["campaign_name"].fillna("").str.split("|").str[9].fillna("unknown"),
    )

    print(
        "✅ [TRANSFORM] Successfully transformed Facebook Ads campaign metadata with "
        f"{len(df)} row(s)."
    )

    return df