-- materialized: table
-- Payouts per advertiser per country per day: what finance and account managers read.
SELECT
    event_date,
    advertiser,
    country,
    COUNT(*)                 AS rewards_paid,
    SUM(reward_cents)::bigint AS payout_cents,
    COUNT(DISTINCT user_id)  AS paying_users
FROM {{ ref('fct_rewards_paid') }}
GROUP BY event_date, advertiser, country
