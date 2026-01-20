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

def load_ad_insights(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Facebook Ads ad insights loader
    ----------------------
    Workflow:
        1. Validate input DataFrame
            Empty input DataFrame triggers 'return'
        2. Log loading metadata
            Row count, destination table, partition, cluster...
        3. Trigger GoogleBigqueryLoader
           UPSERT mode, date-based deduplication, partition on date
    ---------
    Parameters:
        1. df: pd.DataFrame 
            Facebook Ads ad insights flattended DataFrame
        2. direction: str
            Must be 'project.dataset.table' for Google BigQuery
    ---------
    Returns:
        1. None
            This function performs data loading as a side effect and does not return any value.
    """   

    if df.empty:
        msg = ("⚠️ [LOADER] Empty Facebook Ads ad insights Dataframe then loading will be skipped.")
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of Facebook Ads ad insights to Google BigQuery table "
        f"{direction}..."
        )
    GoogleBigqueryLoader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=["date"],
        partition=["date"],
        cluster=[
            "account_id",
            "ad_id"
            ],
    )