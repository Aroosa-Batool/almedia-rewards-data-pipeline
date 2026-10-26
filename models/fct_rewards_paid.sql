-- materialized: incremental
-- unique_key: event_id
-- Only new rows since the last run are processed (watermark on ingested_at).
-- Re-running is safe: rows are upserted on unique_key.
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
