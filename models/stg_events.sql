-- materialized: view
-- Staging: one clean row per event. Renames, casts and derived columns only.
SELECT
    event_id,
    event_type,
    user_id,
    offer_id,
    advertiser,
    country,
    reward_cents,
    occurred_at,
    (occurred_at AT TIME ZONE 'UTC')::date                     AS event_date,
    ingested_at,
    EXTRACT(EPOCH FROM ingested_at - occurred_at) / 60.0       AS arrival_delay_min
FROM raw_events
