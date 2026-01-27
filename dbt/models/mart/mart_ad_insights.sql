{{ 
  config(
    alias = var('company') ~ '_table_facebook_all_all_ad_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["account_id", "ad_id"]
  ) 
}}

select
    -- ======================
    -- Date
    -- ======================
    date,
    month,
    year,

    -- ======================
    -- Scope
    -- ======================
    department,
    account,

    -- ======================
    -- Grain IDs
    -- ======================
    account_id,
    campaign_id,
    adset_id,
    ad_id,

    -- ======================
    -- Ad
    -- ======================
    ad_name,
    ad_status,
    thumbnail_url,

    -- ======================
    -- Metrics
    -- ======================
    impressions,
    clicks,
    spend,

    result,
    result_type,
    messaging_conversations_started,
    purchase,

    -- ======================
    -- Campaign dimensions
    -- ======================
    campaign_name,
    platform,
    objective,
    budget_group,
    region,
    category_level_1,
    track_group,
    pillar_group,
    content_group,

    -- ======================
    -- Adset dimensions
    -- ======================
    location,
    gender,
    age,
    audience,
    format,
    strategy,
    type,
    pillar,
    content

from {{ ref('int_ad_insights') }}