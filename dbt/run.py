import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import os
import subprocess


def dbt_facebook_ads(
    *,
    google_cloud_project: str,
    select: str
):
    """
    Run dbt for Facebook Ads
    ---------
    Workflow:
        1. Initialize dbt execution environment
        2. Trigger dbt build command for dbt models
        3. Capture dbt execution logs with stdout and stderr
    ---------
    Returns:
        None
    """

    msg = (
        "🔁 [DBT] Running dbt build for Facebook Ads with selector "
        f"{select} to Google Cloud Project "
        f"{google_cloud_project}..."
    )
    print(msg)
    logging.info(msg)

    cmd = [
        "dbt",
        "build",
        "--project-dir", "dbt",
        "--profiles-dir", "dbt",
        "--select", select,
    ]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ},
        )

        # Stream dbt logs realtime (tránh treo)
        for line in process.stdout:
            print(line, end="")
            logging.info(line.rstrip())

        return_code = process.wait()

        if return_code != 0:
            raise RuntimeError(
                f"❌ [DBT] dbt build failed with return code {return_code}"
            )

        msg = (
            "✅ [DBT] Successfully completed dbt build for Facebook Ads with selector "
            f"{select} to Google Cloud Project "
            f"{google_cloud_project}."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [DBT] Failed to complete dbt build for Facebook Ads with selector "
            f"{select} to Google Cloud Project "
            f"{google_cloud_project} due to {e}."
        ) from e