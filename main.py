"""
==================================================================
FACEBOOK MAIN ENTRYPOINT
------------------------------------------------------------------
This script serves as the unified CLI controller for triggering  
ads data updates from Facebook Ads based on command-line arguments 
and environment variables.

It supports incremental daily ingestion for selected data layers  
(e.g. campaign, ad) and allows flexible control over date ranges.

✔️ Dynamic routing to the correct update module based on PLATFORM  
✔️ CLI flags to select data layers and date range mode  
✔️ Shared logging and error handling across update jobs  
✔️ Supports scheduled jobs or manual on-demand executions 
✔️ Automatic update summary and execution timing for each step 

⚠️ This script does *not* contain data processing logic itself.  
It simply delegates update tasks to platform-specific modules .
==================================================================
"""

# Add project root to sys.path to enable absolute imports
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import (
    datetime, 
    timedelta
)

# Add Python dynamic import ultilities for integration
import importlib

# Add Python logging ultilities for integration
import logging

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# Get validated environment variables
if not all([COMPANY, PLATFORM, ACCOUNT, LAYER, MODE]):
    raise EnvironmentError("❌ [MAIN] Missing required environment variables COMPANY/PLATFORM/ACCOUNT/LAYER/MODE.")

# 1. MAIN ENTRYPOINT CONTROLLER

# 1.1. Validate input for main entrypoint function
if PLATFORM != "facebook":
    raise ValueError("❌ [MAIN] Only 'facebook' platform is supported in this script.")
try:
    update_module_location = importlib.import_module(f"src.update")
except ModuleNotFoundError:
    raise ImportError(f"❌ [MAIN] Platform '{PLATFORM}' is not supported so please ensure src/update.py exists.")

# 1.2. Execution controller
def main():
    main_date_today = datetime.today()
    if PLATFORM == "facebook":
        try:
            update_campaign_insights = update_module_location.update_campaign_insights
            update_ad_insights = update_module_location.update_ad_insights
        except AttributeError:
            raise ImportError(f"❌ [MAIN] Failed to locate Facebook Ads update module due to update_campaign_insights and update_ad_insights must be defined.")
        main_layers_variable = [layer.strip() for layer in LAYER.split(",") if layer.strip()]
        if len(main_layers_variable) != 1:
            raise ValueError("⚠️ [MAIN] Only one layer is supported per execution so please run separately for each layer.")
        if MODE == "today":
            main_date_start = main_date_end = main_date_today.strftime("%Y-%m-%d")
        elif MODE == "last3days":
            main_date_start = (main_date_today - timedelta(days=3)).strftime("%Y-%m-%d")
            main_date_end = main_date_today.strftime("%Y-%m-%d")
        elif MODE == "last7days":
            main_date_start = (main_date_today - timedelta(days=7)).strftime("%Y-%m-%d")
            main_date_end = main_date_today.strftime("%Y-%m-%d")
        elif MODE == "thismonth":
            main_date_start = main_date_today.replace(day=1).strftime("%Y-%m-%d")
            main_date_end = main_date_today.strftime("%Y-%m-%d")
        elif MODE == "lastmonth":
            main_date_start = (main_date_today.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
            main_date_end = (main_date_today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            raise ValueError(f"⚠️ [MAIN] Unsupported mode {MODE} for Facebook Ads main entrypoint so please re-check input environment variable.")
        if "campaign" in main_layers_variable:
            try:
                print(f"🚀 [MAIN] Starting to update '{PLATFORM}' campaign performance of '{COMPANY}' company in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end}...")
                logging.info(f"🚀 [MAIN] Starting to update '{PLATFORM}' campaign performance of '{COMPANY}' company in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end}...")
                update_campaign_insights(update_date_start=main_date_start, update_date_end=main_date_end)
            except Exception as e:
                print(f"❌ [MAIN] Failed to trigger update '{PLATFORM}' campaign insights of '{COMPANY}' in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end} due to {e}.")
                logging.error(f"❌ [MAIN] Failed to trigger update '{PLATFORM}' campaign insights of '{COMPANY}' in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end} due to {e}.")
        if "ad" in main_layers_variable:
            try:
                print(f"🚀 [MAIN] Starting to update '{PLATFORM}' ad performance of '{COMPANY}' in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end}...")
                logging.info(f"🚀 [MAIN] Starting to update '{PLATFORM}' ad performance of '{COMPANY}' in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end}...")
                update_ad_insights(update_date_start=main_date_start, update_date_end=main_date_end)
            except Exception as e:
                print(f"❌ [MAIN] Failed to trigger update '{PLATFORM}' ad insights of '{COMPANY}' in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end} due to {e}.")
                logging.error(f"❌ [MAIN] Failed to trigger update '{PLATFORM}' ad insights of '{COMPANY}' in '{MODE}' mode and '{LAYER}' layer from {main_date_start} to {main_date_end} due to {e}.")

# 1.3. Execute main entrypoint function
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Update failed: {e}")
        sys.exit(1)