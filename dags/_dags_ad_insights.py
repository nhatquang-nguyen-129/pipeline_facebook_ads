import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import logging
import pandas as pd
import time

from facebook_business.api import FacebookAdsApi

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
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    msg = (
        "🔄 [DAGS] Trigger to update Facebook Ads ad insights with account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

    # Initialize Facebook Ads SDK client
    try:
        msg = (
            "🔍 [DAGS] Initializing Facebook Ads SDK client for account_id "
            f"{account_id}..."
        )
        print(msg)
        logging.info(msg)

        FacebookAdsApi.init(
            access_token=access_token,
            timeout=180,
        )

        msg = (
            "✅ [DAGS] Successfully initialized Facebook Ads SDK client for account_id "
            f"{account_id}."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [DAGS] Failed to initialize Facebook Ads SDK client for ad_insights DAG due to "
            f"{e}."
        ) from e

# ETL for Facebook Ads ad insights
    DAGS_INSIGHTS_ATTEMPTS = 3
    DAGS_INSIGHTS_COOLDOWN = 60

    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date   = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_ad_ids: set[str] = set()

    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        for attempt in range(1, DAGS_INSIGHTS_ATTEMPTS + 1):
            try:
                
    # Extract
                msg = (
                    "🔄 [DAGS] Trigger to extract Facebook Ads ad insights from account_id "
                    f"{account_id} at "
                    f"{dags_split_date} for "
                    f"{attempt} attempt(s)..."
                )
                print(msg)
                logging.info(msg)                
                
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
                    "🔄 [DAGS] Trigger to transform Facebook Ads ad insights from "
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

                msg = (
                    "🔄 [DAGS] Trigger to load Facebook Ads ad insights from account_id "
                    f"{account_id} for "
                    f"{dags_split_date} to direction "
                    f"{_ad_insights_direction}..."
                )
                print(msg)
                logging.info(msg)

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
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )
                print(msg)
                logging.warning(msg)

                if not retryable:
                    raise RuntimeError(
                        f"❌ [DAGS] Failed to extract Facebook Ads ad insights for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be aborting."
                    ) from e

                if attempt == DAGS_INSIGHTS_ATTEMPTS:
                    raise RuntimeError(
                        "❌ [DAGS] Failed to extract Facebook Ads ad insights for "
                        f"{dags_split_date} in "
                        f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s) due to exceeded attempt limit then DAG execution will be aborting."
                    ) from e

                wait_to_retry = 60 + (attempt - 1) * 30
                
                msg = (
                    "🔄 [DAGS] Waiting "
                    f"{wait_to_retry} second(s) before retrying Facebook Ads API "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s)..."
                )
                print(msg)
                logging.warning(msg)

                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)
        
        if dags_start_date <= dags_end_date:
            msg = (
                "🔄 [DAGS] Waiting "
                f"{DAGS_INSIGHTS_COOLDOWN} second(s) cooldown before processing next date of Facebook Ads ad insights..."
            )
            print(msg)
            logging.info(msg)

            time.sleep(DAGS_INSIGHTS_COOLDOWN)

# ETL for Facebook Ads ad metadata
    DAGS_AD_ATTEMPTS = 3
   
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
    remaining_ad_ids = list(total_ad_ids)
    dfs_ad_metadata = []

    for attempt in range(1, DAGS_AD_ATTEMPTS + 1):
        msg = (
            "🔄 [DAGS] Trigger to extract Facebook Ads ad metadata for "
            f"{len(remaining_ad_ids)} ad_id(s) in "
            f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
        )
        print(msg)
        logging.info(msg)
    
        df_ad_metadata = extract_ad_metadata(
            account_id=account_id,
            ad_ids=remaining_ad_ids,
        )

        if not df_ad_metadata.empty:
            dfs_ad_metadata.append(df_ad_metadata)

        failed_ad_ids = getattr(df_ad_metadata, "failed_ad_ids", [])
        retryable = getattr(df_ad_metadata, "retryable", False)

        if not failed_ad_ids:
            msg = (
                "✅ [DAGS] Successfully triggered to extract Facebook ad metadata with "
                f"{len(set(pd.concat(dfs_ad_metadata)["ad_id"].dropna()))}/{len(remaining_ad_ids)} row(s)."
            )
            print(msg)
            logging.info(msg)
            break

        if not retryable:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads ad metadata for "
                f"{len(remaining_ad_ids)} ad_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        if attempt == DAGS_AD_ATTEMPTS:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads ad metadata for "
                f"{len(remaining_ad_ids)} ad_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        remaining_ad_ids = failed_ad_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        msg = (
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads API "
                f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
            )
        print(msg)
        logging.info(msg)
        
        time.sleep(wait_to_retry)

    df_ad_metadatas = pd.concat(dfs_ad_metadata, ignore_index=True)

    # Transform

        # Nothing to transform with ad metadata

    # Load    
    _ad_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    )
    
    msg = (
        "🔄 [DAGS] Trigger to load Facebook Ads ad metadata for "       
        f"{len(df_ad_metadatas)} row(s) for "
        f"{_ad_metadata_direction}..."
    )
    print(msg)
    logging.info(msg)

    load_ad_metadata(
        df=df_ad_metadatas,
        direction=_ad_metadata_direction,
    )

# ETL for Facebook Ads ad creative
    DAGS_CREATIVE_ATTEMPTS = 3
    
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
    remaining_ad_ids = list(total_ad_ids)
    dfs_ad_creative = []
    
    for attempt in range(1, DAGS_CREATIVE_ATTEMPTS + 1):
        msg = (
            "🔄 [DAGS] Trigger to extract Facebook Ads ad creative for "
            f"{len(remaining_ad_ids)} ad_id(s) in "
            f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempt(s)..."
        )
        print(msg)
        logging.info(msg)

        df_ad_creative = extract_ad_creative(
            account_id=account_id,
            ad_ids=remaining_ad_ids,
        )

        if not df_ad_creative.empty:
            dfs_ad_creative.append(df_ad_creative)

        failed_ad_ids = getattr(df_ad_creative, "failed_ad_ids", [])
        retryable = getattr(df_ad_creative, "retryable", False)

        if not failed_ad_ids:
            msg = (
                "✅ [DAGS] Successfully triggered to extract Facebook ad creative with "
                f"{len(set(pd.concat(dfs_ad_creative)["ad_id"].dropna()))}/{len(remaining_ad_ids)} row(s)."
            )
            print(msg)
            logging.info(msg)
            break

        if not retryable:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads ad creative for "
                f"{len(remaining_ad_ids)} ad_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        if attempt == DAGS_CREATIVE_ATTEMPTS:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads ad creative for "
                f"{len(remaining_ad_ids)} ad_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        remaining_ad_ids = failed_ad_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        msg = (
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads API "
                f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempt(s)..."
            )
        print(msg)
        logging.info(msg)
        
        time.sleep(wait_to_retry)

    df_ad_creatives = pd.concat(dfs_ad_creative, ignore_index=True)

    # Transform

        # Nothing to transform with ad creative

    # Load
    _ad_creative_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_ad_creative"
    )

    msg = (
        "🔄 [DAGS] Trigger to load Facebook Ads ad creative for "
        f"{len(df_ad_creatives)} row(s) to "
        f"{_ad_creative_direction}..."
    )
    print(msg)
    logging.info(msg)

    load_ad_creative(
        df=df_ad_creatives,
        direction=_ad_creative_direction,
    )

# ETL for Facebook Ads adset metadata
    DAGS_ADSET_ATTEMPTS = 3

    total_adset_ids = set(df_ad_metadatas["adset_id"].dropna().unique())

    if not total_adset_ids:
        msg = (
            "⚠️ [DAGS] No Facebook Ads adset_id appended for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        print(msg)
        logging.warning(msg)
        return
    
    # Extract
    remaining_adset_ids = list(total_adset_ids)
    dfs_adset_metadata = []
    
    for attempt in range(1, DAGS_ADSET_ATTEMPTS + 1):
        msg = (
            "🔄 [DAGS] Trigger to extract Facebook Ads adset metadata for "
            f"{len(remaining_adset_ids)} ad_id(s) in "
            f"{attempt}/{DAGS_ADSET_ATTEMPTS} attempt(s)..."
        )
        print(msg)
        logging.info(msg)
    
        df_adset_metadata = extract_adset_metadata(
            account_id=account_id,
            adset_ids=remaining_adset_ids,
        )

        if not df_adset_metadata.empty:
            dfs_adset_metadata.append(df_adset_metadata)

        failed_adset_ids = getattr(df_adset_metadata, "failed_adset_ids", [])
        retryable = getattr(df_adset_metadata, "retryable", False)

        if not failed_adset_ids:
            msg = (
                "✅ [DAGS] Successfully triggered to extract Facebook adset metadata with "
                f"{len(set(pd.concat(dfs_adset_metadata)["adset_id"].dropna()))}/{len(remaining_adset_ids)} row(s)."
            )
            print(msg)
            logging.info(msg)
            break

        if not retryable:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads adset metadata for "
                f"{len(remaining_adset_ids)} adset_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        if attempt == DAGS_ADSET_ATTEMPTS:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads adset metadata for "
                f"{len(remaining_adset_ids)} adset_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        remaining_adset_ids = failed_adset_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        msg = (
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads API "
                f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
            )
        print(msg)
        logging.info(msg)
        
        time.sleep(wait_to_retry)

    df_adset_metadatas = pd.concat(dfs_adset_metadata, ignore_index=True)

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
        "🔁 [DAGS] Trigger to load Facebook Ads adset metadata for "
        f"{len(df_adset_metadatas)} row(s) to"
        f"{_adset_metadata_direction}..."
        
    )

    load_adset_metadata(
        df=df_adset_metadatas,
        direction=_adset_metadata_direction,
    )

# ETL for Facebook Ads campaign metadata
    DAGS_CAMPAIGN_ATTEMPTS = 3

    total_campaign_ids = set(df_ad_metadatas["campaign_id"].dropna().unique())

    if not total_campaign_ids:
        msg = (
            "⚠️ [DAGS] No Facebook Ads campaign_id appended for account_id "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        print(msg)
        logging.warning(msg)
        return

    # Extract
    remaining_campaign_ids = list(total_campaign_ids)
    dfs_campaign_metadata = []
    
    for attempt in range(1, DAGS_CAMPAIGN_ATTEMPTS + 1):
        msg = (
            "🔄 [DAGS] Trigger to extract Facebook Ads campaign metadata for "
            f"{len(remaining_campaign_ids)} campaign_id(s) in "
            f"{attempt}/{DAGS_CAMPAIGN_ATTEMPTS} attempt(s)..."
        )
        print(msg)
        logging.info(msg)
    
        df_campaign_metadata = extract_campaign_metadata(
            account_id=account_id,
            campaign_ids=remaining_campaign_ids,
        )

        if not df_campaign_metadata.empty:
            dfs_campaign_metadata.append(df_campaign_metadata)

        failed_campaign_ids = getattr(df_campaign_metadata, "failed_campaign_ids", [])
        retryable = getattr(df_campaign_metadata, "retryable", False)

        if not failed_campaign_ids:
            msg = (
                "✅ [DAGS] Successfully triggered to extract Facebook campaign metadata with "
                f"{len(set(pd.concat(dfs_campaign_metadata)["campaign_id"].dropna()))}/{len(remaining_campaign_ids)} row(s)."
            )
            print(msg)
            logging.info(msg)
            break

        if not retryable:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads campaign metadata for "
                f"{len(remaining_campaign_ids)} campaign_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        if attempt == DAGS_CAMPAIGN_ATTEMPTS:
            msg = (
                "❌ [DAGS] Failed to extract Facebook Ads campaign metadata for "
                f"{len(remaining_campaign_ids)} campaign_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            print(msg)
            logging.warning(msg)
            break

        remaining_campaign_ids = failed_campaign_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        msg = (
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads API "
                f"{attempt}/{DAGS_CAMPAIGN_ATTEMPTS} attempt(s)..."
            )
        print(msg)
        logging.info(msg)
        
        time.sleep(wait_to_retry)

    df_campaign_metadatas = pd.concat(dfs_campaign_metadata, ignore_index=True)

    # Transform
    msg = (
        "🔁 [DAGS] Trigger to transform Facebook Ads campaign metadata for "
        f"{len(df_campaign_metadatas)} row(s)..."
    )
    print(msg)
    logging.info(msg)

    df_campaign_metadatas = transform_campaign_metadata(df_campaign_metadatas)

    # Load
    _campaign_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    )  

    msg = (
        "🔁 [DAGS] Trigger to load Facebook Ads campaign metadata for "
        f"{len(df_campaign_metadatas)} row(s) to"
        f"{_campaign_metadata_direction}..."
        
    )

    load_campaign_metadata(
        df=df_campaign_metadatas,
        direction=_campaign_metadata_direction,
    )

# Materialization with dbt
    msg = "🔁 [DAGS] Trigger to materialize Facebook Ads ad insights with dbt..."
    print(msg)
    logging.info(msg)

    dbt_facebook_ads(
        google_cloud_project=PROJECT,
        select="tag:mart,tag:ad"
    )