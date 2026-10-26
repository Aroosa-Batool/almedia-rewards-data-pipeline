-- materialized: table
-- Lifetime value per user, with a simple segment. Window function ranks users per country.
SELECT
    user_id,
    country,
    lifetime_cents,
    rewards,
    first_paid_at,
    last_paid_at,
    CASE WHEN lifetime_cents >= 3000 THEN 'whale'
         WHEN lifetime_cents >= 1000 THEN 'regular'
         ELSE 'casual' END AS segment,
    RANK() OVER (PARTITION BY country ORDER BY lifetime_cents DESC) AS country_rank
FROM (
    SELECT user_id,
           MAX(country)       AS country,
           SUM(reward_cents)  AS lifetime_cents,
           COUNT(*)           AS rewards,
           MIN(occurred_at)   AS first_paid_at,
           MAX(occurred_at)   AS last_paid_at
    FROM {{ ref('fct_rewards_paid') }}
    GROUP BY user_id
)
