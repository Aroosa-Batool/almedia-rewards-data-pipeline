-- materialized: table
-- Funnel per offer. Counts DISTINCT users so repeat clicks don't inflate conversion.
SELECT
    offer_id,
    advertiser,
    COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'offer_click')    AS clickers,
    COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'offer_complete') AS completers,
    COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'reward_paid')    AS paid_users,
    ROUND(COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'offer_complete')::numeric
          / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'offer_click'), 0), 4) AS click_to_complete
FROM {{ ref('stg_events') }}
GROUP BY offer_id, advertiser
