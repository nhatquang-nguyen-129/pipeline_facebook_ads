import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import pandas as pd
import time

from etl.extract_campaign_insights import extract_campaign_insights
from etl.extract_campaign_metadata import extract_campaign_metadata
from etl.transform_campaign_insights import transform_campaign_insights
from etl.transform_campaign_metadata import transform_campaign_metadata
from etl.load_campaign_insights import load_campaign_insights
from etl.load_campaign_metadata import load_campaign_metadata

from dbt.run import dbt_facebook_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

def dags_campaign_insights(
    *,
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    """
    DAG Orchestration for Facebook Ads campaign insights
    ---
    Principles:
        1. Trigger Facebook Ads campaign insights extraction
        2. Transform Facebook Ads campaign insights into validated schema
        3. Load transformed Facebook Ads campaign insights records into Google BigQuery
        4. Set Facebook Ads API cooldown between each day
        5. Execute dbt models for materialization
    ---
    Returns:
        1. None:
    """     

    print(
        "🔄 [DAGS] Trigger to update Facebook Ads campaign insights with account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )

    # ETL for Facebook Ads campaign insights
    DAGS_INSIGHTS_ATTEMPTS = 3
    
    DAGS_INSIGHTS_COOLDOWN = 60

    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    
    dags_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_campaign_ids: set[str] = set()

    while dags_start_date <= dags_end_date:

        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        for attempt in range(1, DAGS_INSIGHTS_ATTEMPTS + 1):

            try:
    
        # Extract
                print(
                    "🔄 [DAGS] Trigger to extract Facebook Ads campaign insights from account_id "
                    f"{account_id} for "
                    f"{dags_split_date} with "
                    f"{attempt} attempt(s)..."
                )  

                insights = extract_campaign_insights(
                    access_token=access_token,
                    account_id=account_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if insights.empty:

                    break

        # Transform
                print(
                    "🔄 [DAGS] Trigger to transform Facebook Ads campaign insights from "
                    f"{account_id} for "
                    f"{dags_split_date} with "
                    f"{len(insights)} row(s)..."
                )       
                
                insights = transform_campaign_insights(insights)

        # Load
                dags_split_year = pd.to_datetime(insights["date"].dropna().iloc[0]).year
        
                dags_split_month = pd.to_datetime(insights["date"].dropna().iloc[0]).month

                _campaign_insights_direction = (
                    f"{PROJECT}."
                    f"{COMPANY}_dataset_facebook_api_raw."
                    f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_campaign_m{dags_split_month:02d}{dags_split_year}"
                )
                
                print(
                    "🔄 [DAGS] Trigger to load Facebook Ads campaign insights from account_id "
                    f"{account_id} for "
                    f"{dags_split_date} to direction "
                    f"{_campaign_insights_direction}..."
                )

                daily_campaign_ids = set(insights["campaign_id"].unique())
                
                total_campaign_ids.update(daily_campaign_ids)

                load_campaign_insights(
                    df=insights,
                    direction=_campaign_insights_direction,
                )

                break

            except Exception as e:
                
                retryable = getattr(e, "retryable", False)
                
                print(
                    "⚠️ [DAGS] Failed to trigger Facebook Ads campaign insights extraction for "
                    f"{dags_split_date} with "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )

                if not retryable:
                    
                    raise RuntimeError(
                        f"❌ [DAGS] Failed to trigger Facebook Ads campaign insights extraction for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be suspended."
                    ) from e

                if attempt == DAGS_INSIGHTS_ATTEMPTS:
                    
                    raise RuntimeError(
                        "❌ [DAGS] Failed to trigger Facebook Ads campaign insights extraction for "
                        f"{dags_split_date} with "
                        f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s) due to exceeded attempt limit then DAG execution will be suspended."
                    ) from e

                wait_to_retry = 60 + (attempt - 1) * 30
                
                print(
                    "🔄 [DAGS] Waiting "
                    f"{wait_to_retry} second(s) before retrying Facebook Ads campaign insights extraction with "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s)..."
                )

                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)
        
        if dags_start_date <= dags_end_date:
            
            print(
                "🔄 [DAGS] Waiting "
                f"{DAGS_INSIGHTS_COOLDOWN} second(s) before processing next date of Facebook Ads campaign insights..."
            )

            time.sleep(DAGS_INSIGHTS_COOLDOWN)

    # ETL for Facebook Ads campaign metadata
    DAGS_CAMPAIGN_ATTEMPTS = 3
    
    if not total_campaign_ids:
        
        print(
            "⚠️ [DAGS] Failed to trigger Facebook Ads campaign metadata extraction for "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to no campaign_id appended then DAG execution will be suspended."
        )
        
        return

    remaining_campaign_ids = list(total_campaign_ids)

    dfs_campaign_metadata = []
    
    for attempt in range(1, DAGS_CAMPAIGN_ATTEMPTS + 1):

        # Extract
        print(
            "🔄 [DAGS] Trigger to extract Facebook Ads campaign metadata for "
            f"{len(remaining_campaign_ids)} campaign_id(s) with "
            f"{attempt}/{DAGS_CAMPAIGN_ATTEMPTS} attempt(s)..."
        )
    
        df_campaign_metadata = extract_campaign_metadata(
            access_token=access_token,
            account_id=account_id,
            campaign_ids=remaining_campaign_ids,
        )

        if not df_campaign_metadata.empty:
        
            dfs_campaign_metadata.append(df_campaign_metadata)

        failed_campaign_ids = getattr(df_campaign_metadata, "failed_campaign_ids", [])
        
        retryable = getattr(df_campaign_metadata, "retryable", False)

        if not failed_campaign_ids:
      
            print(
                "✅ [DAGS] Successfully triggered Facebook Ads campaign metadata extraction with "
                f"{len(set(pd.concat(dfs_campaign_metadata)["campaign_id"].dropna()))}/{len(remaining_campaign_ids)} row(s)."
            )
            
            break

        if not retryable:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads campaign metadata extraction for "
                f"{len(remaining_campaign_ids)} campaign_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            
            break

        if attempt == DAGS_CAMPAIGN_ATTEMPTS:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads campaign metadata extraction for "
                f"{len(remaining_campaign_ids)} campaign_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            
            break

        remaining_campaign_ids = failed_campaign_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        print(
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads campaign metadata extraction with "
            f"{attempt}/{DAGS_CAMPAIGN_ATTEMPTS} attempt(s)..."
        )
        
        time.sleep(wait_to_retry)

    df_campaign_metadatas = pd.concat(dfs_campaign_metadata, ignore_index=True)

        # Transform
    print(
        "🔄 [DAGS] Trigger to transform Facebook Ads campaign metadata with "
        f"{len(df_campaign_metadatas)} row(s)..."
    )

    df_campaign_metadatas = transform_campaign_metadata(df_campaign_metadatas)

        # Load
    _campaign_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    )

    print(
        "🔄 [DAGS] Trigger to load Facebook Ads campaign metadata for "       
        f"{len(df_campaign_metadatas)} row(s) to direction "
        f"{_campaign_metadata_direction}..."
    )

    load_campaign_metadata(
        df=df_campaign_metadatas,
        direction=_campaign_metadata_direction,
    )

    # Materialization with dbt
    print(
        "🔄 [DAGS] Trigger to materialize Facebook Ads campaign insights with dbt..."
    )

    dbt_facebook_ads(
        google_cloud_project=PROJECT,
        select="tag:mart,tag:campaign"
    )