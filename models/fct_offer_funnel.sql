-- materialized: table
-- Funnel per offer: how many users clicked, completed, got paid. Counts DISTINCT users
-- so repeated clicks by the same user do not inflate conversion.
SELECT
    offer_id,
    advertiser,
    COUNT(DISTINCT CASE WHEN event_type = 'offer_click'    THEN user_id END) AS clickers,
    COUNT(DISTINCT CASE WHEN event_type = 'offer_complete' THEN user_id END) AS completers,
    COUNT(DISTINCT CASE WHEN event_type = 'reward_paid'    THEN user_id END) AS paid_users,
    ROUND(1.0 * COUNT(DISTINCT CASE WHEN event_type = 'offer_complete' THEN user_id END)
          / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'offer_click' THEN user_id END), 0), 4) AS click_to_complete
FROM {{ ref('stg_events') }}
GROUP BY offer_id, advertiser
