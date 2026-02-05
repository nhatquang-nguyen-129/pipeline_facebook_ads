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

    msg = (
        "🔄 [DAGS] Triggering to update Facebook Ads for account_id "
        f"{account_id} from {start_date} to {end_date} "
        f"using ThreadPoolExecutor with {max_workers} max_workers..."
    )
    print(msg)
    logging.info(msg)

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    start_time = time.time()
    failures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}

        # ---------- submit tasks ----------
        for task_name, task_fn in tasks.items():
            future = executor.submit(
                lambda name=task_name, fn=task_fn: (
                    name,
                    fn,
                )
            )
            future_map[future] = task_name

        print(
            f"🔍 [DAGS] Waiting for {len(future_map)} ThreadPoolExecutor task(s) to complete..."
        )

        # ---------- execute tasks ----------
        for future in as_completed(future_map):
            task_name = future_map[future]

            print("\n" + "=" * 120)
            print(f"📦 [DAGS:{task_name}] LOG OUTPUT (flush after completion)")
            print("=" * 120)

            buffer = io.StringIO()
            task_start = time.time()

            try:
                _, task_fn = future.result()

                with redirect_stdout(buffer), redirect_stderr(buffer):
                    task_fn(
                        access_token=access_token,
                        account_id=account_id,
                        start_date=start_date,
                        end_date=end_date,
                    )

                elapsed = round(time.time() - task_start, 2)

                if buffer.getvalue().strip():
                    print(buffer.getvalue(), end="")

                print(
                    f"\n✅ [DAGS:{task_name}] Completed successfully "
                    f"in {elapsed}s"
                )

            except Exception as e:
                print(f"❌ [DAGS:{task_name}] FAILED due to {e}")
                logging.exception(e)
                failures.append(task_name)

            print("=" * 120 + "\n")

    total_elapsed = round(time.time() - start_time, 2)

    if failures:
        raise RuntimeError(
            "❌ [DAGS] Failed to update Facebook Ads with "
            f"{', '.join(failures)} failed task(s) "
            f"in {total_elapsed}s elapsed time."
        )

    msg = (
        "✅ [DAGS] Successfully updated Facebook Ads using ThreadPoolExecutor in "
        f"{total_elapsed}s elapsed time."
    )
    print(msg)
    logging.info(msg)