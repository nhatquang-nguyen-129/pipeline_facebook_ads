import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import logging
import pandas as pd
import time

from etl.extract_campaign_insights import extract_campaign_insights
from etl.extract_campaign_metadata import extract_campaign_metadata
from etl.extract_ad_insights import extract_ad_insights
from etl.extract_ad_metadata import extract_ad_metadata
from etl.extract_adset_metadata import extract_adset_metadata
from etl.extract_ad_creative import extract_ad_creative

from dbt.run import dbt_facebook_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

def dags_facebook_ads(
    *,
    facebook_ads_client,
    customer_id: str,
    start_date: str,
    end_date: str,
):

    msg = (
        "🔁 [DAGS] Trigger to update Facebook Ads campaign insights with customer_id "
        f"{customer_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

# ETL for Facebook Ads campaign insights
    DAGS_MAX_ATTEMPTS = 3
    DAGS_MIN_COOLDOWN = 60
    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    total_campaign_ids: set[str] = set()

    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        msg = (
            "🔁 [DAGS] Trigger to extract Facebook Ads campaign insights from customer_id "
            f"{customer_id} for "
            f"{dags_split_date}..."
        )
        print(msg)
        logging.info(msg)

        for attempt in range(1, DAGS_MAX_ATTEMPTS + 1):
            try:

    # Extract               
                insights = extract_campaign_insights(
                    google_ads_client=facebook_ads_client,
                    customer_id=customer_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if insights.empty:
                    msg = (
                        "⚠️ [DAGS] No Facebook Ads campaign insights returned from customer_id "
                        f"{customer_id} then DAG execution "
                        f"{dags_split_date} will be skipped."
                    )
                    print(msg)
                    logging.warning(msg)
                    break

    # Transform
                insights = transform_campaign_insights(insights)

    # Load
                dags_split_year = pd.to_datetime(insights["date"].dropna().iloc[0]).year
                dags_split_month = pd.to_datetime(insights["date"].dropna().iloc[0]).month

                dags_campaign_insights = (
                    f"{PROJECT}."
                    f"{COMPANY}_dataset_google_api_raw."
                    f"{COMPANY}_table_google_{DEPARTMENT}_{ACCOUNT}_campaign_m{dags_split_month:02d}{dags_split_year}"
                )

                msg = (
                    "🔁 [DAGS] Trigger to load Google Ads campaign insights from customer_id "
                    f"{customer_id} for "
                    f"{dags_split_date} to direction "
                    f"{dags_campaign_insights}..."
                )
                print(msg)
                logging.info(msg)

                daily_campaign_ids = set(insights["campaign_id"].unique())
                total_campaign_ids.update(daily_campaign_ids)

                load_campaign_insights(
                    df=insights,
                    direction=dags_campaign_insights
                )

                break

            except Exception as e:
                retryable = getattr(e, "retryable", False)
                msg = (
                    f"⚠️ [DAGS] Failed to extract Google Ads campaign insights for {dags_split_date} in "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )
                print(msg)
                logging.warning(msg)

                if not retryable:
                    raise RuntimeError(
                        f"❌ [DAGS] Failed to extract Google Ads campaign insights for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be aborting."
                    ) from e

                if attempt == DAGS_MAX_ATTEMPTS:
                    raise RuntimeError(
                        "❌ [DAGS] Failed to extract Google Ads campaign insights for "
                        f"{dags_split_date} in "
                        f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to exceeded attempt limit then DAG execution will be aborting."
                    ) from e

                wait_to_retry = 2 ** attempt
                
                msg = (
                    "🔁 [DAGS] Waiting "
                    f"{wait_to_retry} second(s) before retrying Google Ads API "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s)..."
                )
                print(msg)
                logging.warning(msg)

                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)
        
        if dags_start_date <= dags_end_date:
            msg = (
                "🔁 [DAGS] Waiting "
                f"{DAGS_MIN_COOLDOWN} second(s) cooldown before processing next date..."
            )
            print(msg)
            logging.info(msg)

            time.sleep(DAGS_MIN_COOLDOWN)

# ETL for Google Ads campaign metadata
    if not total_campaign_ids:
        msg = (
            "⚠️ [DAGS] No Google Ads campaign_id appended for customer_id "
            f"{customer_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔁 [DAGS] Trigger to extracting Google Ads campaign metadata for "
        f"{len(total_campaign_ids)} campaign_id(s)..."
    )
    print(msg)
    logging.info(msg)

    # Extract
    df_campaign_metadatas = extract_campaign_metadata(
        google_ads_client=google_ads_client,
        customer_id=customer_id,
        campaign_id_list=list(total_campaign_ids),
    )

    if df_campaign_metadatas.empty:
        msg = "⚠️ [DAGS] Empty campaign metadata extracted then DAG execution will be suspended."
        print(msg)
        logging.warning(msg)
        return

    # Transform
    df_campaign_metadatas = transform_campaign_metadata(df_campaign_metadatas)

    # Load
    campaign_metadata_table = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_google_api_raw."
        f"{COMPANY}_table_google_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    )

    msg = (
        "🔁 [DAGS] Trigger to load Google Ads campaign metadata to "
        f"{campaign_metadata_table}..."
    )
    print(msg)
    logging.info(msg)

    load_campaign_metadata(
        df=df_campaign_metadatas,
        direction=campaign_metadata_table,
    )

# Materialization with dbt
    msg = ("🔁 [DAGS] Trigger to materialize Google Ads campaign insights with dbt...")
    print(msg)
    logging.info(msg)
    
    dbt_google_ads(
        google_cloud_project=PROJECT,
    )