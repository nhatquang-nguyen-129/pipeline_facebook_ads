import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import logging
import pandas as pd
import time

from etl.extract_ad_insights import extract_ad_insights
from etl.extract_ad_metadata import extract_ad_metadata
from etl.extract_ad_creative import extract_ad_creative
from etl.extract_adset_metadata import extract_adset_metadata
from etl.extract_campaign_metadata import extract_campaign_metadata
from etl.transform_ad_insights import transform_ad_insights
from etl.transform_adset_metadata import transform_adset_metadata
from etl.transform_campaign_metadata import transform_campaign_metadata
from etl.load_ad_insights import load_ad_insights
from etl.load_ad_metadata import load_ad_metadata
from etl.load_ad_creative import load_ad_creative
from etl.load_adset_metadata import load_adset_metadata
from etl.load_campaign_metadata import load_campaign_metadata

from dbt.run import dbt_facebook_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

def dags_ad_insights(
    *,
    account_id: str,
    start_date: str,
    end_date: str,
):
    msg = (
        "🔁 [DAGS] Trigger to update Facebook Ads ad insights with account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

# ETL for Facebook Ads ad insights
    DAGS_MAX_ATTEMPTS = 3
    DAGS_MIN_COOLDOWN = 60

    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date   = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_ad_ids: set[str] = set()

    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        msg = (
            "🔁 [DAGS] Trigger to extract Facebook Ads ad insights from account_id "
            f"{account_id} at "
            f"{dags_split_date}..."
        )
        print(msg)
        logging.info(msg)

        for attempt in range(1, DAGS_MAX_ATTEMPTS + 1):
            try:
                
    # Extract
                insights = extract_ad_insights(
                    account_id=account_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if insights.empty:
                    msg = (
                        "⚠️ [DAGS] No Facebook Ads ad insights returned from account_id "
                        f"{account_id} then DAG execution "
                        f"{dags_split_date} will be skipped."
                    )
                    print(msg)
                    logging.warning(msg)
                    break

    # Transform
                msg = (
                    "🔁 [DAGS] Trigger to transform Facebook Ads ad insights from "
                    f"{account_id} with "
                    f"{dags_split_date} for "
                    f"{len(insights)} row(s)..."
                )
                print(msg)
                logging.info(msg)                

                insights = transform_ad_insights(insights)

    # Load
                year  = pd.to_datetime(insights["date"].iloc[0]).year
                month = pd.to_datetime(insights["date"].iloc[0]).month

                _ad_insights_direction = (
                    f"{PROJECT}."
                    f"{COMPANY}_dataset_facebook_api_raw."
                    f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_ad_m{month:02d}{year}"
                )

                daily_ad_ids = set(insights["ad_id"].dropna().unique())
                total_ad_ids.update(daily_ad_ids)

                load_ad_insights(
                    df=insights,
                    direction=_ad_insights_direction,
                )

                break

            except Exception as e:
                retryable = getattr(e, "retryable", False)
                msg = (
                    f"⚠️ [DAGS] Failed to extract Facebook Ads ad insights for {dags_split_date} in "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )
                print(msg)
                logging.warning(msg)

                if not retryable:
                    raise RuntimeError(
                        f"❌ [DAGS] Failed to extract Facebook Ads ad insights for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be aborting."
                    ) from e

                if attempt == DAGS_MAX_ATTEMPTS:
                    raise RuntimeError(
                        "❌ [DAGS] Failed to extract Facebook Ads ad insights for "
                        f"{dags_split_date} in "
                        f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to exceeded attempt limit then DAG execution will be aborting."
                    ) from e

                wait_to_retry = 2 ** attempt
                
                msg = (
                    "🔁 [DAGS] Waiting "
                    f"{wait_to_retry} second(s) before retrying Facebook Ads API "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s)..."
                )
                print(msg)
                logging.warning(msg)

                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)
        
        if dags_start_date <= dags_end_date:
            msg = (
                "🔁 [DAGS] Waiting "
                f"{DAGS_MIN_COOLDOWN} second(s) cooldown before processing next date of Facebook Ads ad insights..."
            )
            print(msg)
            logging.info(msg)

            time.sleep(DAGS_MIN_COOLDOWN)

# ETL for Facebook Ads ad metadata
    if not total_ad_ids:
        msg = (
            "⚠️ [DAGS] No Facebook Ads ad_id appended for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        print(msg)
        logging.warning(msg)
        return

    # Extract
    msg = (
        "🔁 [DAGS] Trigger to extract Facebook Ads ad metadata for "
        f"{len(total_ad_ids)} ad_id(s)..."
    )
    print(msg)
    logging.info(msg)

    df_ad_metadatas = extract_ad_metadata(
        account_id=account_id,
        ad_id_list=list(total_ad_ids),
    )

    if df_ad_metadatas.empty:
        msg = "⚠️ [DAGS] Empty Facebook Ads ad metadata extracted then DAG execution will be suspended."
        print(msg)
        logging.warning(msg)
        return

    # Transform

    # Load    
    _ad_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    )
    
    msg = (
        "🔁 [DAGS] Trigger to load Facebook Ads ad metadata to "
        f"{_ad_metadata_direction} for "
        f"{len(df_ad_metadatas)} row(s)..."
    )
    print(msg)
    logging.info(msg)

    load_ad_metadata(
        df=df_ad_metadatas,
        direction=_ad_metadata_direction,
    )

# ETL for Facebook Ads ad metadata
    total_adset_ids = set(df_ad_metadatas["adset_id"].dropna().unique())

    if total_adset_ids:

    # Extract        
        msg = (
            "🔁 [DAGS] Trigger to extract Facebook Ads adset metadata for "
            f"{len(total_adset_ids)} adset_id(s)..."
        )
        print(msg)
        logging.info(msg)

        df_adset_metadatas = extract_adset_metadata(
            account_id=account_id,
            adset_id_list=list(total_adset_ids),
        )

    # Transform
        msg = (
            "🔁 [DAGS] Trigger to transform Facebook Ads adset metadata for "
            f"{len(df_adset_metadatas)} row(s)..."
        )
        print(msg)
        logging.info(msg)

        df_adset_metadatas = transform_adset_metadata(df_adset_metadatas)

    # Load
        _adset_metadata_direction = (
            f"{PROJECT}."
            f"{COMPANY}_dataset_facebook_api_raw."
            f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
        )        
        
        msg = (
            "🔁 [DAGS] Trigger to load Facebook Ads adset metadata to "
            f"{_adset_metadata_direction} for "
            f"{len(df_adset_metadatas)} row(s)..."
        )

        load_adset_metadata(
            df=df_adset_metadatas,
            direction=_adset_metadata_direction,
        )

    # =========================
    # 4. Campaign Metadata
    # =========================
    campaign_ids = set(df_ads["campaign_id"].dropna().unique())

    if campaign_ids:
        msg = f"🔁 [DAGS] Trigger to extract Facebook Ads campaign metadata for {len(campaign_ids)} campaign_id(s)..."
        print(msg)
        logging.info(msg)

        df_campaigns = extract_campaign_metadata(
            account_id=account_id,
            campaign_id_list=list(campaign_ids),
        )

        df_campaigns = transform_campaign_metadata(df_campaigns)

        load_campaign_metadata(
            df=df_campaigns,
            direction=(
                f"{PROJECT}."
                f"{COMPANY}_dataset_facebook_api_raw."
                f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
            ),
        )

    # =========================
    # 5. dbt Materialization
    # =========================
    msg = "🔁 [DAGS] Trigger to materialize Facebook Ads ad insights with dbt..."
    print(msg)
    logging.info(msg)

    dbt_facebook_ads(
        google_cloud_project=PROJECT,
        level="ad",
    )
