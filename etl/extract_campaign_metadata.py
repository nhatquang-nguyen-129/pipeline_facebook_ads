import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def extract_campaign_metadata(
    access_token: str,
    account_id: str,
    campaign_ids: list[str],
) -> pd.DataFrame:
    """
    Extract Facebook Ads campaign metadata
    ---------
    Workflow:
        1. Validate input campaign_ids
        2. Make API call for AdAccount endpoint
        3. Make API call for Campaign(campaign_id) endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened campaign metadata records
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_campaign_ids: list[str] = []
    retryable = False

    # Validate input
    if not campaign_ids:
        msg = (
            "⚠️ [EXTRACT] No input campaign_ids for Facebook Ads account_id "
            f"{account_id} then empty DataFrame returned."
        )
        print(msg)
        logging.warning(msg)

        df = pd.DataFrame(
            columns=[
                "campaign_id",
                "campaign_name",
                "status",
                "account_id",
                "account_name",
            ]
        )
        df.failed_campaign_ids = []
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = 0
        df.rows_output = 0
        return df

    # Initialize Facebook Ads SDK client
    try:
        msg = (
            "🔍 [EXTRACT] Initializing Facebook Ads SDK client with account_id "
            f"{account_id} for campaign metadata extraction..."
        )
        print(msg)
        logging.info(msg)

        FacebookAdsApi.init(
            access_token=access_token,
            timeout=180,
        )

        msg = (
            "✅ [EXTRACT] Successfully initialized Facebook Ads SDK client for account_id "
            f"{account_id} for campaign metadata extraction."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to initialize Facebook Ads SDK client for account_id "
            f"{account_id} for campaign metadata extraction due to "
            f"{e}."
        ) from e

    # Make Facebook Ads API call for ad account information
    try:
        msg = (
            "🔍 [EXTRACT] Extracting Facebook Ads account_name for account_id "
            f"{account_id}..."
        )
        print(msg)
        logging.info(msg)

        account_info = AdAccount(f"act_{account_id}").api_get(fields=["name"])
        account_name = account_info.get("name")

        msg = (
            "✅ [EXTRACT] Successfully extracted Facebook Ads account_name "
            f"{account_name} for account_id "
            f"{account_id}."
        )
        print(msg)
        logging.info(msg)

    except FacebookRequestError as e:
        api_error_code = None
        http_status = None

        try:
            api_error_code = e.api_error_code()
            http_status = e.http_status()
        except Exception:
            pass

        # Expired token error
        if api_error_code == 190:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
                f"{account_id} due to expired or invalid access token then manual token refresh is required."
            ) from e
        
        # Unexpected retryable API error
        if (
            (http_status and http_status >= 500)
            or api_error_code in {
                1, 
                2, 
                4, 
                17, 
                80000
            }
        ):
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
                f"{account_id} due to API error "
                f"{e} then this request is eligible to retry."
            ) from e

        # Unexpected non-retryable API error
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Facebook Ads account_name for account_id "
            f"{account_id} due to API error "
            f"{e} then this request is not eligible to retry."
        ) from e

    # Make Facebook Ads API call for campaign metadata
    msg = (
        "🔍 [EXTRACT] Extracting Facebook Ads campaign metadata for account_id "
        f"{account_id} with "
        f"{len(campaign_ids)} campaign_id(s)..."
    )
    print(msg)
    logging.info(msg)
        
    for campaign_id in campaign_ids:
        try:
            campaign = Campaign(campaign_id).api_get(
                fields=[
                    "id",
                    "name",
                    "status",
                    "account_id",
                ]
            )

            rows.append(
                {
                    "campaign_id": campaign.get("id"),
                    "campaign_name": campaign.get("name"),
                    "status": campaign.get("status"),
                    "account_id": account_id,
                    "account_name": account_name,
                }
            )

        except FacebookRequestError as e:
            api_error_code = None
            http_status = None

            try:
                api_error_code = e.api_error_code()
                http_status = e.http_status()
            except Exception:
                pass

        # Expired token error
            if api_error_code == 190:
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract Facebook Ads campaign metadata for account_id "
                    f"{account_id} due to expired or invalid access token then manual token refresh is required."
                ) from e

        # Unexpected retryable API error
            if (
                (http_status and http_status >= 500)
                or api_error_code in {
                    1, 
                    2, 
                    4, 
                    17, 
                    80000
                }
            ):
                failed_campaign_ids.append(campaign_id)
                retryable = True

                msg = (
                    "⚠️ [EXTRACT] Failed to extract Facebook Ads campaign metadata for campaign_id "
                    f"{campaign_id} due to API error "
                    f"{e} then this request is eligible to retry."
                )
                print(msg)
                logging.warning(msg)

                rows.append(
                    {
                        "campaign_id": campaign_id,
                        "campaign_name": None,
                        "status": None,
                        "account_id": account_id,
                        "account_name": account_name,
                    }
                )
                continue

        # Unexpected non-retryable API error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads campaign metadata for campaign_id "
                f"{campaign_id} due to API error "
                f"{e} then this request is not eligible to retry."
            ) from e

        # Unknown non-retryable error        
        except Exception as e:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads campaign metadata for campaign_id "
                f"{campaign_id} due to "
                f"{e}."
            ) from e

    df = pd.DataFrame(rows)

    msg = (
        "✅ [EXTRACT] Successfully extracted "
        f"{len(df)}/{len(campaign_ids)} row(s) of Facebook Ads campaign metadata."
    )
    print(msg)
    logging.info(msg) 

    df.failed_campaign_ids = failed_campaign_ids
    df.retryable = retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = len(campaign_ids)
    df.rows_output = len(df)

    return df