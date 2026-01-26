import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import pandas as pd

from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError


def extract_ad_creative(
    account_id: str,
    ad_ids: list[str],
) -> pd.DataFrame:
    """
    Extract Facebook Ads ad creative
    ---------
    Workflow:
        1. Validate input ad_ids list[dict]
        2. Make API call for Ad(ad_id) endpoint
        3. Append extracted JSON data to list[dict]
        4. Enforce List[dict] to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened ad creative records
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_ad_ids: list[str] = []
    retryable = True

    if not ad_ids:
        df = pd.DataFrame(
            columns=[
                "account_id",
                "ad_id",
                "creative_id",
                "thumbnail_url",
            ]
        )
        df.failed_ad_ids = []
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = 0
        df.rows_output = 0
        return df

    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id).api_get(fields=["creative"])
            creative_id = ad.get("creative", {}).get("id")

            if not creative_id:
                rows.append(
                    {
                        "account_id": account_id,
                        "ad_id": ad_id,
                        "creative_id": None,
                        "thumbnail_url": None,
                    }
                )
                continue

            creative = AdCreative(creative_id).api_get(
                fields=["thumbnail_url"]
            )

            rows.append(
                {
                    "account_id": account_id,
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": creative.get("thumbnail_url"),
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
                    "❌ [EXTRACT] Failed to extract Facebook Ads ad creative due to token expired or invalid then manual token refresh is required.") from e

            # Unexpected retryable error
            if (
                (http_status and http_status >= 500)
                or api_error_code in {1, 2, 4, 17, 80000}
            ):
                failed_ad_ids.append(ad_id)

                msg = (
                    "⚠️ [EXTRACT] Failed to extract Facebook Ads ad creative for ad_id "
                    f"{ad_id} due to API request error "
                    f"{e} then this ad_id is eligible to retry."
                )
                print(msg)
                logging.warning(msg)

                rows.append(
                    {
                        "account_id": account_id,
                        "ad_id": ad_id,
                        "creative_id": None,
                        "thumbnail_url": None,
                    }
                )
                continue
            
            # Unexpected non-retryable error
            raise RuntimeError(
                f"❌ [EXTRACT] Failed to extract Facebook Ads ad creative for ad_id "
                f"{ad_id} due to unexpected API error {e}."
            ) from e

        except Exception as e:
            
            # Unknown non-retryable error
            raise RuntimeError(
                f"❌ [EXTRACT] Failed to extract Facebook Ads creative for ad_id "
                f"{ad_id} due to {e}."
            ) from e

    df = pd.DataFrame(rows)
    df.failed_ad_ids = failed_ad_ids
    df.retryable = bool(failed_ad_ids) and retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = len(ad_ids)
    df.rows_output = len(df)

    return df