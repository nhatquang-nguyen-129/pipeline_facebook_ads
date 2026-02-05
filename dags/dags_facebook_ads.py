import sys
import time
import logging
import io
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr

ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from dags._dags_campaign_insights import dags_campaign_insights
from dags._dags_ad_insights import dags_ad_insights

def dags_facebook_ads(
    *,
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
    max_workers: int = 2,
):

    print(
        f"🔄 [DAGS] Triggering to update Facebook Ads for {account_id} "
        f"from {start_date} to {end_date} with {max_workers} workers..."
    )

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    spinner_frames = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
    spinner_idx = 0

    start_time = time.time()
    failures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}

        for name, fn in tasks.items():
            future = executor.submit(
                fn,
                access_token=access_token,
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
            )
            future_map[future] = name

        print(f"🔄 [DAGS] Waiting for {len(future_map)} task(s) to complete...")

        # -------- SPINNER LOOP (CHẠY THẬT) --------
        while True:
            done = [f for f in future_map if f.done()]
            if done:
                break

            frame = spinner_frames[spinner_idx % len(spinner_frames)]
            spinner_idx += 1
            print(f"\r{frame} [DAGS] Running Facebook Ads DAGs...", end="", flush=True)
            time.sleep(0.1)

        print("\r" + " " * 80 + "\r", end="", flush=True)
        # ------------------------------------------

        for future in as_completed(future_map):
            name = future_map[future]

            print("=" * 120)
            print(f"[DAGS] TASK EXECUTION SUMMARY FOR {name}")
            print("=" * 120)

            try:
                future.result()
                print(f"✅ [DAGS:{name}] Completed successfully")
            except Exception as e:
                print(f"❌ [DAGS:{name}] Failed: {e}")
                logging.exception(e)
                failures.append(name)

            print("=" * 120 + "\n")

    total_elapsed = round(time.time() - start_time, 2)

    if failures:
        raise RuntimeError(
            f"❌ [DAGS] Failed tasks: {', '.join(failures)} in {total_elapsed}s"
        )

    print(
        f"✅ [DAGS] Successfully updated Facebook Ads in {total_elapsed}s."
    )