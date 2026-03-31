import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_adset_metadata(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load Facebook Ads adset metadata
    ---
    Principles:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to account_id and adset_id
        4. Use UPSERT mode with temporary table for deduplication
        5. Make internalGoogleBigQueryLoader API call
    ---
    Returns:
        None
    """      

    if df.empty:
        
        print(
            "⚠️ [LOADER] Failed to load Facebook Ads adset metadata due to no input DataFrame then loading will be suspended."
        )
        
        return

    print(
        "🔄 [LOADER] Loading Facebook Ads adset metadata with "
        f"{len(df)} row(s) to Google BigQuery table "
        f"{direction}..."
    )
    
    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=[
            "account_id", 
            "adset_id"
        ],
        partition=None,
        cluster=[
            "adset_id"
        ],
    )