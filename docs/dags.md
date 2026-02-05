# Multi-thread Execution with Python ThreadPool

## Problem statements

Multi-thread Execution and Facebook Ads SDK Initialization Strategy
Background

Facebook Ads ETL pipeline supports parallel execution across multiple extract streams:

Campaign insights

Ad insights

Ad metadata

Ad creatives

Parallelism is required to:

Reduce end-to-end pipeline latency

Isolate failures between independent extract units

Scale horizontally on Cloud Run / Airflow / Kubernetes

However, naive multi-thread execution can introduce data inconsistency and race conditions if SDK clients are improperly shared.

Problem Statement
Shared SDK client in multi-threaded execution

The Facebook Ads Python SDK maintains internal mutable state, including but not limited to:

HTTP session

Retry counters

Pagination cursors

Request context

Error handling and backoff state

When a single global SDK client is initialized and reused across multiple threads:

Threads may overwrite request state of each other

Retry logic from one extract stream can interfere with another

Pagination cursors may be corrupted

Failures become non-deterministic and hard to reproduce

Data may be:

Missing

Duplicated

Incorrectly attributed to the wrong entity (ad_id, creative_id, etc.)

This issue does not always surface immediately, making it especially dangerous in production environments.

Why Sequential Execution Is Not Sufficient

Running extract steps sequentially guarantees consistency but introduces critical drawbacks:

Increased total execution time

Inefficient use of Cloud Run / compute resources

Reduced fault isolation (one failure blocks the entire pipeline)

Therefore, parallel execution is required, but it must be implemented safely.

Design Principle

Each extract unit must own its own Facebook Ads SDK client instance.

This principle applies regardless of whether parallelism is implemented via:

Python threads

Multiprocessing

Cloud Run multiple containers

Airflow tasks

Kubernetes jobs

Architecture Decision
SDK Initialization Responsibility

main.py does NOT initialize Facebook Ads SDK

Each extract function:

Initializes its own SDK client

Uses the same access token and account_id

Maintains full isolation from other extract units

This shifts SDK lifecycle ownership downstream to the extract layer.

Execution Model
Incorrect (Shared Client)
main.py
 └─ FacebookAdsApi.init(...)
 └─ ThreadPoolExecutor
     ├─ extract_ad_creatives()
     ├─ extract_ad_metadata()


❌ Shared mutable SDK state
❌ High risk of race conditions
❌ Non-deterministic failures

Correct (Isolated Clients)
main.py
 └─ ThreadPoolExecutor
     ├─ extract_ad_creatives()
     │    └─ FacebookAdsApi.init(...)
     ├─ extract_ad_metadata()
          └─ FacebookAdsApi.init(...)


✅ Thread-safe execution
✅ Independent retry and pagination logic
✅ Deterministic and reproducible behavior

Why This Works

Facebook Ads access tokens are stateless

Multiple SDK clients can safely authenticate using the same token

SDK initialization cost is negligible compared to:

API calls

Retry backoff

Pagination loops

Each client maintains:

Its own HTTP session

Its own retry lifecycle

Its own error context

Benefits
Data Consistency

No shared state across extract streams

Guaranteed request isolation

Correct attribution of metrics and metadata

Fault Isolation

Failure in ad creative extraction does not affect ad insights

Retries are scoped to the failing extract unit only

Scalability

Seamless migration to:

Cloud Run parallel containers

Airflow task-level parallelism

Distributed workers

Observability

Cleaner logs

Easier debugging

Clear ownership of failures per extract stream

Final Recommendation

Always initialize Facebook Ads SDK inside extract functions

Never share SDK clients across threads

Treat each extract unit as an isolated execution context

This design ensures correctness first, while preserving performance and scalability of the ETL pipeline.