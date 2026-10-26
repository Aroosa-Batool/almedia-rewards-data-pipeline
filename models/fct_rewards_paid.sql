-- materialized: incremental
-- unique_key: event_id
-- Only rows ingested since the last watermark (minus a safety lookback) are processed.
-- Upserting on unique_key makes the overlap harmless.
SELECT
    event_id,
    user_id,
    offer_id,
    advertiser,
    country,
    reward_cents,
    occurred_at,
    event_date,
    ingested_at
FROM {{ ref('stg_events') }}
WHERE event_type = 'reward_paid'
  AND {{ incremental_filter('ingested_at') }}
