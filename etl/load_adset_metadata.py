import os
import sys
import logging
import pandas as pd
from plugins.google_bigquery import GoogleBigqueryLoader
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../"
        )
    )
)

def load_adset_metadata(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Facebook Ads adset metadata loader
    ----------------------
    Workflow:
        1. Validate input DataFrame
            Empty input DataFrame triggers 'return'
        2. Log loading metadata
            Row count, destination table, partition, cluster...
        3. Trigger GoogleBigqueryLoader
           UPSERT mode, adset_id-based deduplication
    ---------
    Parameters:
        1. df: pd.DataFrame 
            Facebook Ad adset metadata flattended DataFrame
        2. direction: str
            Must be 'project.dataset.table' for Google BigQuery
    ---------
    Returns:
        1. None
            This function performs data loading as a side effect and does not return any value.
    """    

    if df.empty:
        msg = ("⚠️ [LOADER] Empty Facebook Ads adset metadata Dataframe then loading will be skipped.")
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of Facebook Ads adset metadata to Google BigQuery table "
        f"{direction}..."
        )
    GoogleBigqueryLoader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=[
            "account_id", 
            "adset_id"
            ],
        partition=None,
        cluster=None,
    )