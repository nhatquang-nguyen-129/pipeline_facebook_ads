import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def extract_ad_metadata(
    account_id: str,
    ad_ids: list[str],
) -> pd.DataFrame:
    """
    Extract Facebook Ads ad metadata
    ---------
    Workflow:
        1. Validate input ad_ids
        2. Make API call for AdAccount endpoint
        3. Make API call for Ad(ad_id) endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
        1. DataFrame:
            Flattened adset metadata records
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_ad_ids: list[str] = []
    retryable = False

    # Validate input
    if not ad_ids:
        df = pd.DataFrame(
            columns=[
                "ad_id",
                "ad_name",
                "adset_id",
                "campaign_id",
                "status",
                "account_id",
                "account_name",
            ]
        )
        df.failed_ad_ids = []
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = 0
        df.rows_output = 0
        return df

    # Make Facebook Ads API call for ad account information
    try:
        msg = (
            "🔍 [EXTRACT] Extracting Facebook Ads account_name for account_id "
            f"{account_id}..."
        )
        print(msg)
        logging.info(msg)

        account = AdAccount(account_id).api_get(fields=["name"])
        account_name = account.get("name")

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

    # Make Facebook Ads API call for ad metadata
    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id).api_get(
                fields=[
                    "id",
                    "name",
                    "adset_id",
                    "campaign_id",
                    "status",
                    "account_id",
                ]
            )

            rows.append(
                {
                    "ad_id": ad.get("id"),
                    "ad_name": ad.get("name"),
                    "adset_id": ad.get("adset_id"),
                    "campaign_id": ad.get("campaign_id"),
                    "status": ad.get("status"),
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
                    "❌ [EXTRACT] Failed to extract Facebook Ads ad metadata for account_id "
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
                failed_ad_ids.append(ad_id)
                retryable = True

                msg = (
                    "⚠️ [EXTRACT] Failed to extract Facebook Ads ad metadata for ad_id "
                    f"{ad_id} due to API error "
                    f"{e} then this request is eligible to retry."
                )
                print(msg)
                logging.warning(msg)

                rows.append(
                    {
                        "ad_id": ad_id,
                        "ad_name": None,
                        "adset_id": None,
                        "campaign_id": None,
                        "status": None,
                        "account_id": account_id,
                        "account_name": account_name,
                    }
                )
                continue

        # Unexpected non-retryable API error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads ad metadata for ad_id "
                f"{ad_id} due to API error "
                f"{e} then this request is not eligible to retry."
            ) from e

        # Unknown non-retryable error        
        except Exception as e:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads ad metadata for ad_id "
                f"{ad_id} due to "
                f"{e}."
            ) from e

    df = pd.DataFrame(rows)
    df.failed_ad_ids = failed_ad_ids
    df.retryable = retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = len(ad_ids)
    df.rows_output = len(df)

    return df