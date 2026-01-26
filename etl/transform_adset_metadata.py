import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd

def transform_adset_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Facebook Ads adset metadata..."
    )
    print(msg)
    logging.info(msg)

    if df.empty:
        msg = "⚠️ [TRANSFORM] Empty adset metadata then transformation will be suspended."
        print(msg)
        logging.warning(msg)
        return df

    required_cols = {
        "account_id",
        "adset_id",
        "adset_name"
        }
    
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError (
            "❌ [TRANSFORM] Failed to transform Facebook Ads adset metadata due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()
    df = df.assign(
        location=lambda df: df["adset_name"].fillna("").str.split("|").str[0].fillna("unknown"),
        gender=lambda df: df["adset_name"].fillna("").str.split("|").str[1].fillna("unknown"),
        age=lambda df: df["adset_name"].fillna("").str.split("|").str[2].fillna("unknown"),
        audience=lambda df: df["adset_name"].fillna("").str.split("|").str[3].fillna("unknown"),
        format=lambda df: df["adset_name"].fillna("").str.split("|").str[4].fillna("unknown"),
        strategy=lambda df: df["adset_name"].fillna("").str.split("|").str[5].fillna("unknown"),
        type=lambda df: df["adset_name"].fillna("").str.split("|").str[6].fillna("unknown"),
        pillar=lambda df: df["adset_name"].fillna("").str.split("|").str[7].fillna("unknown"),
        content=lambda df: df["adset_name"].fillna("").str.split("|").str[8].fillna("unknown")
    )  

    msg = (
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Facebook Ads adset metadata."
    )
    print(msg)
    logging.info(msg)

    return df