# ETL for Facebook Ads

## Purpose

- **Extract** Facebook Ads data from `Facebook Ads API`

- **Transform** raw records into `normalized analytical schema`

- **Load** transformed data into `Google BigQuery` using idempotent `UPSERT` strategy

---

## Extract

- The extractor initializes Facebook Ads client using `FacebookSession` with `access_token`

- The extractor uses Facebook Marketing API endpoints `AdAccount`, `Campaign`, `AdSet`, `Ad`

- The extractor invokes `.api_get()` for metadata extraction and `.get_insights()` for performance extraction

- The extractor enforces insights extraction using `params = {"level": "campaign" | "adset" | "ad"}`

- The extractor applies date filtering using `params = {"time_range": {"since": start_date, "until": end_date}}`

- The extractor transforms Facebook Ads API response objects into flattened `List[dict]` records

- The extractor converts extracted records into `pandas.DataFrame`

- The extractor propagates structured error metadata using `error.retryable` flag to support orchestration-level retry logic

- The extractor normalizes account identifiers by enforcing `act_{account_id}` prefix before API invocation

---

## Transform

- The transformer normalizes `account_id`, `campaign_id`, `ad_id` to `STRING` type

- The transformer enforces numeric schema for `impressions` and `clicks` as `INT64`

- The transformer converts cost into spend by casting to `INT64`

- The transformer enforces floating-point schema for `result`

- The transformer normalizes date into `UTC` timestamp and floors to daily granularity

- The transformer derives `year` dimension from normalized date

- The transformer derives `month` dimension using `YYYY-MM` format from normalized date

- The transformer enriches a constant platform column with value `Facebook`

- The transformer parses `campaign_name` using underscore `_` delimiter to derive structured dimensions

- The transformer parses `adset_name` using underscore `_` delimiter to derive structured dimensions

- The transformer fills missing parsed values with `unknown` to preserve schema consistency

---

## Load

- The loader uses `mode="upsert"` to support idempotent loading and deduplication

- The loader uses primary key(s) defined in `keys=[...]` to overwrite existing matching records

- The loader delegates execution to `internalGoogleBigqueryLoader` for standardized BigQuery operations

- The loader uses `keys=["date"]` to deduplicate campaign and ad insights records at daily granularity

- The loader applies table partitioning on `partition={"field": "date"}` for campaign and ad insights

- The loader applies table clustering on `cluster=["campaign_id"]` for campaign and insights

- The loader uses composite primary keys `keys=["account_id", "campaign_id"]` for campaign metadata

- The loader uses composite primary keys `keys=["account_id", "adset_id"]` for adset metadata

- The loader uses composite primary keys `keys=["account_id", "ad_id"]` for ad metadata

- The loader applies table clustering on `cluster=["campaign_id"]` for campaign metadata

- The loader applies table clustering on `cluster=["adset_id"]` for campaign metadata

- The loader applies table clustering on `cluster=["ad_id"]` for ad metadata

- The loader does not apply table partitioning for campaign, adset or ad metadata