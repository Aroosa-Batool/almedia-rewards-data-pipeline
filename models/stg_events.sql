-- materialized: view
-- Staging: one clean row per event. Renames, casts and derived columns only, no business logic.
SELECT
    event_id,
    event_type,
    user_id,
    offer_id,
    advertiser,
    country,
    reward_cents,
    occurred_at,
    DATE(occurred_at)  AS event_date,
    ingested_at,
    (JULIANDAY(ingested_at) - JULIANDAY(occurred_at)) * 24 * 60 AS arrival_delay_min
FROM raw_events
