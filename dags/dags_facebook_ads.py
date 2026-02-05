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
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date} using ThreadPoolExecutor with "
        f"{max_workers} max_workers..."
    )
    print(msg)
    logging.info(msg)

    # List of DAG tasks to run in parallel
    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    start_time = time.time()
    failures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}

        # Submit tasks to ThreadPoolExecutor
        for task_name, task_fn in tasks.items():
            future = executor.submit(
                lambda name=task_name, fn=task_fn: (
                    name,
                    fn,
                )
            )
            future_map[future] = task_name

        msg = (
            "🔄 [DAGS] Waiting for "
            f"{len(future_map)} ThreadPoolExecutor task(s) to complete..."
        )
        print(msg)
        logging(msg)

        # Braille spinner printing in the terminal while executing tasks
        spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        spinner_idx = 0

        # Process tasks as soon as they finish and whichever task finishes first will have its logs printed first
        for future in as_completed(future_map):
            task_name = future_map[future]

            print("\n" + "=" * 120)
            print(f"[DAGS] TASK EXECUTION SUMMARY FOR {task_name}")
            print("=" * 120)

        # Redirect all stdout/stderr from terminal into in-memory buffer to prevent interleaving between threds
            buffer = io.StringIO()
            task_start = time.time()

            try:
                _, task_fn = future_map[future], None  # placeholder, future already submitted

        # Keep overwriting spinner loop the same terminal line
                while not future.done():
                    frame = spinner_frames[spinner_idx % len(spinner_frames)]
                    spinner_idx += 1

                    print(
                        f"\r{frame} [DAGS:{task_name}] Running...",
                        end="",
                        flush=True,
                    )
                    time.sleep(0.1)

                print("\r" + " " * 80 + "\r", end="", flush=True)

                with redirect_stdout(buffer), redirect_stderr(buffer):
                    future.result()

                elapsed = round(time.time() - task_start, 2)

        # Print entire buffered log as a single continuous block at once after task complettion
                if buffer.getvalue().strip():
                    print(buffer.getvalue(), end="")

                print(
                    "✅ [DAGS] Successfully completed "
                    f"{task_name} task using ThreadPoolExecutor in "
                    f"{elapsed}s."
                )

            except Exception as e:
                print(
                    f"❌ [DAGS] Failed to complete {task_name} "
                    f"using ThreadPoolExecutor: {e}"
                )
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