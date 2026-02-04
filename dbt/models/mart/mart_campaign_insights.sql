{{ 
  config(
    alias = var('company') ~ '_table_facebook_all_all_campaign_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["account_id", "campaign_id"],
    tags = ['mart', 'facebook', 'campaign']    
  ) 
}}

select
    date,
    month,
    year,

    department,
    account,

    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_status,
    
    impressions,
    clicks,
    spend,
    
    result,
    result_type,
    messaging_conversations_started,
    purchase,

    platform,
    objective,
    region,
    budget_group_1,
    budget_group_2,
    category_level_1,
    personnel,
    track_group,
    pillar_group,
    content_group
from {{ ref('int_campaign_insights') }}