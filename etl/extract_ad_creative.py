import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
from typing import List
import pandas as pd

from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError

def extract_ad_creative(ad_ids: List[str]) -> pd.DataFrame:
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

    rows: list[dict] = []

    if not ad_ids:
        return pd.DataFrame(
            columns=[
                "ad_id",
                "creative_id",
                "thumbnail_url",
                "error",
                "retryable",
            ]
        )

    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id).api_get(fields=["creative"])
            creative_id = ad.get("creative", {}).get("id")

            if not creative_id:
                rows.append(
                    {
                        "ad_id": ad_id,
                        "creative_id": None,
                        "thumbnail_url": None,
                        "error": "NO_CREATIVE",
                        "retryable": False,
                    }
                )
                continue

            creative = AdCreative(creative_id).api_get(
                fields=["thumbnail_url"]
            )

            rows.append(
                {
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": creative.get("thumbnail_url"),
                    "error": None,
                    "retryable": False,
                }
            )

        except FacebookRequestError as err:
            retryable = False
            try:
                if err.http_status() >= 500:
                    retryable = True
                elif err.api_error_code() in {1, 2, 17, 80000}:
                    retryable = True
            except Exception:
                pass

            logging.warning(
                "[FB_AD_CREATIVE_ERROR] ad_id=%s | http=%s | code=%s | subcode=%s | retryable=%s",
                ad_id,
                err.http_status(),
                err.api_error_code(),
                err.api_error_subcode(),
                retryable,
            )

            rows.append(
                {
                    "ad_id": ad_id,
                    "creative_id": None,
                    "thumbnail_url": None,
                    "error": str(err),
                    "retryable": retryable,
                }
            )

        except Exception as err:
            logging.exception(
                "[FB_AD_CREATIVE_UNKNOWN_ERROR] ad_id=%s", ad_id
            )

            rows.append(
                {
                    "ad_id": ad_id,
                    "creative_id": None,
                    "thumbnail_url": None,
                    "error": str(err),
                    "retryable": False,
                }
            )

    return pd.DataFrame(rows)