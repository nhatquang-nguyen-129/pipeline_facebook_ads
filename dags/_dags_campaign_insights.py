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

from dbt.run import dbt_facebook_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")


def dags_campaign_insights(
    *,
    customer_id: str,
    start_date: str,
    end_date: str,
):
    msg = (
        "🔁 [DAGS] Trigger to update Facebook Ads campaign insights with customer_id "
        f"{customer_id} from {start_date} to {end_date}..."
    )
    print(msg)
    logging.info(msg)

    # =========================
    # ETL – Campaign Insights
    # =========================
    DAGS_MAX_ATTEMPTS = 3
    DAGS_MIN_COOLDOWN = 60

    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_campaign_ids: set[str] = set()

    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        msg = (
            "🔁 [DAGS] Trigger to extract Facebook Ads campaign insights for "
            f"{customer_id} at {dags_split_date}..."
        )
        print(msg)
        logging.info(msg)

        for attempt in range(1, DAGS_MAX_ATTEMPTS + 1):
            try:
                # Extract
                insights = extract_campaign_insights(
                    customer_id=customer_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if insights.empty:
                    msg = (
                        "⚠️ [DAGS] No Facebook Ads campaign insights returned for "
                        f"{customer_id} at {dags_split_date}, skipping."
                    )
                    print(msg)
                    logging.warning(msg)
                    break

                # Transform
                insights = transform_campaign_insights(insights)

                # Load
                dags_split_year = pd.to_datetime(insights["date"].dropna().iloc[0]).year
                dags_split_month = pd.to_datetime(insights["date"].dropna().iloc[0]).month

                dags_campaign_insights_table = (
                    f"{PROJECT}."
                    f"{COMPANY}_dataset_facebook_api_raw."
                    f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_campaign_m{dags_split_month:02d}{dags_split_year}"
                )

                msg = (
                    "🔁 [DAGS] Loading Facebook Ads campaign insights to "
                    f"{dags_campaign_insights_table}..."
                )
                print(msg)
                logging.info(msg)

                daily_campaign_ids = set(insights["campaign_id"].unique())
                total_campaign_ids.update(daily_campaign_ids)

                load_campaign_insights(
                    df=insights,
                    direction=dags_campaign_insights_table,
                )

                break

            except Exception as e:
                retryable = getattr(e, "retryable", False)
                msg = (
                    f"⚠️ [DAGS] Failed Facebook Ads campaign insights for {dags_split_date} "
                    f"attempt {attempt}/{DAGS_MAX_ATTEMPTS} due to {e}."
                )
                print(msg)
                logging.warning(msg)

                if not retryable:
                    raise RuntimeError(
                        "❌ [DAGS] Non-retryable error, aborting DAG."
                    ) from e

                if attempt == DAGS_MAX_ATTEMPTS:
                    raise RuntimeError(
                        "❌ [DAGS] Exceeded retry limit, aborting DAG."
                    ) from e

                wait_to_retry = 2 ** attempt
                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)

        if dags_start_date <= dags_end_date:
            time.sleep(DAGS_MIN_COOLDOWN)

    # =========================
    # ETL – Campaign Metadata
    # =========================
    if not total_campaign_ids:
        msg = (
            "⚠️ [DAGS] No campaign_id collected, suspend DAG execution."
        )
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔁 [DAGS] Extracting Facebook Ads campaign metadata for "
        f"{len(total_campaign_ids)} campaign(s)..."
    )
    print(msg)
    logging.info(msg)

    df_campaign_metadatas = extract_campaign_metadata(
        customer_id=customer_id,
        campaign_id_list=list(total_campaign_ids),
    )

    if df_campaign_metadatas.empty:
        msg = "⚠️ [DAGS] Empty campaign metadata extracted, suspend DAG."
        print(msg)
        logging.warning(msg)
        return

    df_campaign_metadatas = transform_campaign_metadata(df_campaign_metadatas)

    campaign_metadata_table = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    )

    msg = (
        "🔁 [DAGS] Loading Facebook Ads campaign metadata to "
        f"{campaign_metadata_table}..."
    )
    print(msg)
    logging.info(msg)

    load_campaign_metadata(
        df=df_campaign_metadatas,
        direction=campaign_metadata_table,
    )

    # =========================
    # DBT Materialization
    # =========================
    msg = "🔁 [DAGS] Trigger dbt materialization for Facebook Ads campaign..."
    print(msg)
    logging.info(msg)

    dbt_facebook_ads(
        google_cloud_project=PROJECT,
    )
