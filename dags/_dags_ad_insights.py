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

from dbt.run import dbt_facebook_ads

def dags_ad_insights(
    *,
    account_id: str,
    start_date: str,
    end_date: str,
):
    msg = (
        "🔁 [DAGS] Trigger to update Facebook Ads ad insights with account_id "
        f"{account_id} from {start_date} to {end_date}..."
    )
    print(msg)
    logging.info(msg)

    DAGS_MAX_ATTEMPTS = 3
    DAGS_MIN_COOLDOWN = 60

    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date   = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_ad_ids: set[str] = set()

    # =========================
    # 1. ETL Ad Insights (daily)
    # =========================
    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        msg = (
            "🔁 [DAGS] Trigger to extract Facebook Ads ad insights from account_id "
            f"{account_id} at {dags_split_date}..."
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
                    logging.warning(
                        f"⚠️ [DAGS] No ad insights returned for {dags_split_date}, skipping."
                    )
                    break

                # Transform
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
                logging.warning(
                    f"⚠️ [DAGS] Failed ad insights for {dags_split_date} "
                    f"attempt {attempt}/{DAGS_MAX_ATTEMPTS} due to {e}"
                )

                if attempt == DAGS_MAX_ATTEMPTS:
                    raise

                time.sleep(2 ** attempt)

        dags_start_date += timedelta(days=1)

        if dags_start_date <= dags_end_date:
            time.sleep(DAGS_MIN_COOLDOWN)

    # =========================
    # 2. Ad Metadata
    # =========================
    if not total_ad_ids:
        logging.warning("⚠️ [DAGS] No ad_id collected, aborting DAG.")
        return

    msg = f"🔁 [DAGS] Trigger to extract Facebook Ads ad metadata for {len(total_ad_ids)} ad_id(s)..."
    print(msg)
    logging.info(msg)

    df_ads = extract_ad_metadata(
        account_id=account_id,
        ad_id_list=list(total_ad_ids),
    )

    if df_ads.empty:
        logging.warning("⚠️ [DAGS] Empty ad metadata, aborting.")
        return

    df_ads = transform_ad_metadata(df_ads)

    load_ad_metadata(
        df=df_ads,
        direction=(
            f"{PROJECT}."
            f"{COMPANY}_dataset_facebook_api_raw."
            f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        ),
    )

    # =========================
    # 3. Adset Metadata
    # =========================
    adset_ids = set(df_ads["adset_id"].dropna().unique())

    if adset_ids:
        msg = f"🔁 [DAGS] Trigger to extract Facebook Ads adset metadata for {len(adset_ids)} adset_id(s)..."
        print(msg)
        logging.info(msg)

        df_adsets = extract_adset_metadata(
            account_id=account_id,
            adset_id_list=list(adset_ids),
        )

        df_adsets = transform_adset_metadata(df_adsets)

        load_adset_metadata(
            df=df_adsets,
            direction=(
                f"{PROJECT}."
                f"{COMPANY}_dataset_facebook_api_raw."
                f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
            ),
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
