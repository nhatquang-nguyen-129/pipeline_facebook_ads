import logging
import pandas as pd
from plugins.google_bigquery import GoogleBigqueryLoader

def load_ad_insights(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Facebook Ads ad insights loader
    ----------------------
    Workflow:
        1. Get input DataFrame
        2. Execute loader
    ---------
    Parameters:

    ---------
    Returns:
   
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
            "campaign_id"
            ],
    )