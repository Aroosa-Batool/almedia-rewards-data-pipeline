-- materialized: table
-- What finance / account managers look at: payouts per advertiser per country per day.
SELECT
    event_date,
    advertiser,
    country,
    COUNT(*)                 AS rewards_paid,
    SUM(reward_cents)        AS payout_cents,
    COUNT(DISTINCT user_id)  AS paying_users
FROM {{ ref('fct_rewards_paid') }}
GROUP BY event_date, advertiser, country
