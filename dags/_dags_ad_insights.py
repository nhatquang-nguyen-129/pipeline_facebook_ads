import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
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
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    """
    DAG Orchestration for Facebook Ads ad insights
    ---
    Principles:
        1. Trigger Facebook Ads ad insights extraction
        2. Transform Facebook Ads ad insights into validated schema
        3. Load transformed Facebook Ads ad insights records into Google BigQuery
        4. Set Facebook Ads API cooldown between each day
        5. Execute dbt models for materialization
    ---
    Returns:
        1. None:
    """    
    
    print(
        "🔄 [DAGS] Trigger to update Facebook Ads ad insights for account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )

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
                print(
                    "🔄 [DAGS] Trigger to extract Facebook Ads ad insights from account_id "
                    f"{account_id} for "
                    f"{dags_split_date} with "
                    f"{attempt} attempt(s)..."
                )            
                
                insights = extract_ad_insights(
                    access_token=access_token,
                    account_id=account_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if insights.empty:

                    break

        # Transform
                print(
                    "🔄 [DAGS] Trigger to transform Facebook Ads ad insights from "
                    f"{account_id} for "
                    f"{dags_split_date} with "
                    f"{len(insights)} row(s)..."
                )         

                insights = transform_ad_insights(insights)

        # Load
                year  = pd.to_datetime(insights["date"].iloc[0]).year
                
                month = pd.to_datetime(insights["date"].iloc[0]).month

                _ad_insights_direction = (
                    f"{PROJECT}."
                    f"{COMPANY}_dataset_facebook_api_raw."
                    f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_ad_m{month:02d}{year}"
                )

                print(
                    "🔄 [DAGS] Trigger to load Facebook Ads ad insights from account_id "
                    f"{account_id} for "
                    f"{dags_split_date} to direction "
                    f"{_ad_insights_direction}..."
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
                
                print(
                    "⚠️ [DAGS] Failed to trigger Facebook Ads ad insights extraction for "
                    f"{dags_split_date} with "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )

                if not retryable:
                    
                    raise RuntimeError(
                        f"❌ [DAGS] Failed to trigger Facebook Ads ad insights extraction for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be suspended."
                    ) from e

                if attempt == DAGS_INSIGHTS_ATTEMPTS:
                    
                    raise RuntimeError(
                        "❌ [DAGS] Failed to trigger Facebook Ads ad insights extraction for "
                        f"{dags_split_date} with "
                        f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s) due to exceeded attempt limit then DAG execution will be suspended."
                    ) from e

                wait_to_retry = 60 + (attempt - 1) * 30
                
                print(
                    "🔄 [DAGS] Waiting "
                    f"{wait_to_retry} second(s) before retrying Facebook Ads ad insights extraction with "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempt(s)..."
                )

                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)
        
        if dags_start_date <= dags_end_date:
            
            print(
                "🔄 [DAGS] Waiting "
                f"{DAGS_INSIGHTS_COOLDOWN} second(s) before processing next date of Facebook Ads ad insights..."
            )

            time.sleep(DAGS_INSIGHTS_COOLDOWN)

    # ETL for Facebook Ads ad metadata
    DAGS_AD_ATTEMPTS = 3
   
    if not total_ad_ids:
        
        print(
            "⚠️ [DAGS] Failed to trigger Facebook Ads ad metadata extraction for "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to no ad_id appended then DAG execution will be suspended."
        )
        
        return

    remaining_ad_ids = list(total_ad_ids)

    dfs_ad_metadata = []

    for attempt in range(1, DAGS_AD_ATTEMPTS + 1):

        # Extract
        print(
            "🔄 [DAGS] Trigger to extract Facebook Ads ad metadata for "
            f"{len(remaining_ad_ids)} ad_id(s) with "
            f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
        )
    
        df_ad_metadata = extract_ad_metadata(
            access_token=access_token,
            account_id=account_id,
            ad_ids=remaining_ad_ids,
        )

        if not df_ad_metadata.empty:
            
            dfs_ad_metadata.append(df_ad_metadata)

        failed_ad_ids = getattr(df_ad_metadata, "failed_ad_ids", [])
        
        retryable = getattr(df_ad_metadata, "retryable", False)

        if not failed_ad_ids:
        
            print(
                "✅ [DAGS] Successfully triggered Facebook Ads ad metadata extraction with "
                f"{len(set(pd.concat(dfs_ad_metadata)["ad_id"].dropna()))}/{len(remaining_ad_ids)} row(s)."
            )
            
            break

        if not retryable:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads ad metadata extraction for "
                f"{len(remaining_ad_ids)} ad_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            
            break

        if attempt == DAGS_AD_ATTEMPTS:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads ad metadata extraction for "
                f"{len(remaining_ad_ids)} ad_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            
            break

        remaining_ad_ids = failed_ad_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        print(
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads ad metadata extraction with "
            f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
        )
        
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
    
    print(
        "🔄 [DAGS] Trigger to load Facebook Ads ad metadata for "       
        f"{len(df_ad_metadatas)} row(s) to direction "
        f"{_ad_metadata_direction}..."
    )

    load_ad_metadata(
        df=df_ad_metadatas,
        direction=_ad_metadata_direction,
    )

    # ETL for Facebook Ads ad creative
    DAGS_CREATIVE_ATTEMPTS = 3
    
    if not total_ad_ids:
        
        print(
            "⚠️ [DAGS] Failed to trigger Facebook Ads ad creative extraction for "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to no ad_id appended then DAG execution will be suspended."
        )
        
        return

    remaining_ad_ids = list(total_ad_ids)
    
    dfs_ad_creative = []
    
    for attempt in range(1, DAGS_CREATIVE_ATTEMPTS + 1):
        
        # Extract        
        print(
            "🔄 [DAGS] Trigger to extract Facebook Ads ad creative for "
            f"{len(remaining_ad_ids)} ad_id(s) with "
            f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempt(s)..."
        )

        df_ad_creative = extract_ad_creative(
            access_token=access_token,
            account_id=account_id,
            ad_ids=remaining_ad_ids,
        )

        if not df_ad_creative.empty:
            
            dfs_ad_creative.append(df_ad_creative)

        failed_ad_ids = getattr(df_ad_creative, "failed_ad_ids", [])
        
        retryable = getattr(df_ad_creative, "retryable", False)

        if not failed_ad_ids:
            
            print(
                "✅ [DAGS] Successfully triggered to Facebook Ads ad creative extraction with "
                f"{len(set(pd.concat(dfs_ad_creative)["ad_id"].dropna()))}/{len(remaining_ad_ids)} row(s)."
            )
            
            break

        if not retryable:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads ad creative extraction for "
                f"{len(remaining_ad_ids)} ad_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            
            break

        if attempt == DAGS_CREATIVE_ATTEMPTS:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads ad creative extraction for "
                f"{len(remaining_ad_ids)} ad_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            
            break

        remaining_ad_ids = failed_ad_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        print(
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads ad creative extraction with "
            f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempt(s)..."
        )
        
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

    print(
        "🔄 [DAGS] Trigger to load Facebook Ads ad creative with "
        f"{len(df_ad_creatives)} row(s) to "
        f"{_ad_creative_direction}..."
    )

    load_ad_creative(
        df=df_ad_creatives,
        direction=_ad_creative_direction,
    )

    # ETL for Facebook Ads adset metadata
    DAGS_ADSET_ATTEMPTS = 3

    total_adset_ids = set(df_ad_metadatas["adset_id"].dropna().unique())

    if not total_adset_ids:
        
        print(
            "⚠️ [DAGS] Failed to trigger Facebook Ads adset metadata extraction for "
            f"{account_id} from "
            f"{start_date} to "
            f"{end_date} due to no adset_id appended then DAG execution will be suspended."
        )
        
        return
    
    remaining_adset_ids = list(total_adset_ids)
    
    dfs_adset_metadata = []
    
    for attempt in range(1, DAGS_ADSET_ATTEMPTS + 1):

        # Extract
        print(
            "🔄 [DAGS] Trigger to extract Facebook Ads adset metadata for "
            f"{len(remaining_adset_ids)} adset_id(s) with "
            f"{attempt}/{DAGS_ADSET_ATTEMPTS} attempt(s)..."
        )
    
        df_adset_metadata = extract_adset_metadata(
            access_token=access_token,
            account_id=account_id,
            adset_ids=remaining_adset_ids,
        )

        if not df_adset_metadata.empty:
            
            dfs_adset_metadata.append(df_adset_metadata)

        failed_adset_ids = getattr(df_adset_metadata, "failed_adset_ids", [])
        
        retryable = getattr(df_adset_metadata, "retryable", False)

        if not failed_adset_ids:
            
            print(
                "✅ [DAGS] Successfully triggered to Facebook Ads adset metadata extraction with "
                f"{len(set(pd.concat(dfs_adset_metadata)["adset_id"].dropna()))}/{len(remaining_adset_ids)} row(s)."
            )
            
            break

        if not retryable:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads adset metadata extraction for "
                f"{len(remaining_adset_ids)} adset_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            
            break

        if attempt == DAGS_ADSET_ATTEMPTS:
            
            print(
                "❌ [DAGS] Failed to trigger Facebook Ads adset metadata extraction for "
                f"{len(remaining_adset_ids)} adset_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            
            break

        remaining_adset_ids = failed_adset_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        print(
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying Facebook Ads adset metadata extraction "
            f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
            )
        
        time.sleep(wait_to_retry)

    df_adset_metadatas = pd.concat(dfs_adset_metadata, ignore_index=True)

        # Transform
    print(
        "🔁 [DAGS] Trigger to transform Facebook Ads adset metadata with "
        f"{len(df_adset_metadatas)} row(s)..."
    )

    df_adset_metadatas = transform_adset_metadata(df_adset_metadatas)

        # Load
    _adset_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_facebook_api_raw."
        f"{COMPANY}_table_facebook_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
    )        
    
    print(
        "🔄 [DAGS] Trigger to load Facebook Ads adset metadata with "
        f"{len(df_adset_metadatas)} row(s) to direction "
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
                "✅ [DAGS] Successfully triggered Facebook campaign metadata extraction with "
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
        "🔄 [DAGS] Trigger to load Facebook Ads campaign metadata with "
        f"{len(df_campaign_metadatas)} row(s) to"
        f"{_campaign_metadata_direction}..."        
    )

    load_campaign_metadata(
        df=df_campaign_metadatas,
        direction=_campaign_metadata_direction,
    )

    # Materialization with dbt
    print(
        "🔄 [DAGS] Trigger to materialize Facebook Ads ad insights with dbt..."
    )

    dbt_facebook_ads(
        google_cloud_project=PROJECT,
        select="tag:mart,tag:ad"
    )