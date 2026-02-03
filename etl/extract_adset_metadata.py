import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def extract_adset_metadata(
    account_id: str,
    adset_id: str,
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
    Returns:
        DataFrame with retry metadata
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_adset_ids: list[str] = []
    retryable = True

    if not adset_id:
        df = pd.DataFrame(
            columns=[
                "adset_id",
                "adset_name",
                "campaign_id",
                "account_id",
                "account_name",
            ]
        )
        df.failed_adset_ids = []
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = 0
        df.rows_output = 0
        return df

    try:
        adset = AdSet(adset_id).api_get(
            fields=[
                "id",
                "name",
                "campaign_id",
                "account_id",
            ]
        )

        account = AdAccount(account_id).api_get(fields=["name"])

        rows.append(
            {
                "adset_id": adset.get("id"),
                "adset_name": adset.get("name"),
                "campaign_id": adset.get("campaign_id"),
                "account_id": account_id,
                "account_name": account.get("name"),
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
            raise RuntimeError("❌ [EXTRACT] Failed to extract Facebook Ads adset metadata due to token expired or invalid then manual token refresh is required.") from e

        # Unexpected retryable error
        if (
            (http_status and http_status >= 500)
            or api_error_code in {1, 2, 4, 17, 80000}
        ):
            failed_adset_ids.append(adset_id)

            msg = (
                "⚠️ [EXTRACT] Failed to extract Facebook Ads adset metadata for adset_id "
                f"{adset_id} due to API request error "
                f"{e} then this ad_id is eligible to retry."
            )
            print(msg)
            logging.warning(msg)

            rows.append(
                {
                    "adset_id": adset_id,
                    "adset_name": None,
                    "campaign_id": None,
                    "account_id": account_id,
                    "account_name": None,
                }
            )

        else:
        # Unexpected non-retryable error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads adset metadata for adset_id "
                f"{adset_id} due to unexpected API error "
                f"{e}."
            ) from e

    except Exception as e:
        # Unknown non-retryable error
        raise RuntimeError(
            f"❌ [EXTRACT] Failed to extract Facebook Ads adset metadata for adset_id "
            f"{adset_id} due to "
            f"{e}."
        ) from e

    df = pd.DataFrame(rows)
    df.failed_adset_ids = failed_adset_ids
    df.retryable = bool(failed_adset_ids) and retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = 1
    df.rows_output = len(df)

    return df