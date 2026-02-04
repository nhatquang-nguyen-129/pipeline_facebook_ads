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

    cmd = [
        "dbt",
        "build",
        "--project-dir", "dbt",
        "--profiles-dir", "dbt",
        "--select", select,
    ]

    msg = (
        "🔄 [DBT] Executing dbt build for Facebook Ads "
        f"{select} insights to Google Cloud Project "
        f"{google_cloud_project}..."
    )
    print(msg)
    logging.info(msg)

    try:
        result = subprocess.run(
            cmd,
            cwd="dbt",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=os.environ,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "❌ [DBT] Failed to execute dbt build for Facebook Ads "
                f"{select} insights to Google Cloud Project "
                f"{google_cloud_project} due to dbt execution error."
                + (
                    "\n\nDBT error:\n"
                    + "\n".join(
                        line
                        for line in (result.stdout or "").splitlines()
                        if "Error" in line
                    )
                    if (result.stdout or "")
                    else ""
                )
        )   

        msg = (
            "✅ [DBT] Successfully executed dbt build for Facebook Ads "
            f"{select} insights to Google Cloud Project "
            f"{google_cloud_project}."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [DBT] Failed to execute dbt build for Facebook Ads "
            f"{select} insights to Google Cloud Project "
            f"{google_cloud_project} due to "
            f"{e}."
        ) from e