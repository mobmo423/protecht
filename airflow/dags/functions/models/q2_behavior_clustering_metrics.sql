DROP TABLE IF EXISTS {{ schema }}.{{ target_table }};

CREATE TABLE {{ schema }}.{{ target_table }} AS (

WITH rate_changes AS (
    SELECT
        er.currency_symbol,
        c.currency_name,
        er.rate_date,
        er.exchange_rate,
        LAG(er.exchange_rate) OVER (
            PARTITION BY er.currency_symbol ORDER BY er.rate_date
        ) AS prev_rate
    FROM {{ schema }}.exchange_rates er
    JOIN {{ schema }}.currency c
        ON er.currency_symbol = c.currency_symbol
),

streak_flags AS (
    SELECT *,
        CASE WHEN exchange_rate > prev_rate THEN 1 ELSE 0 END AS is_increase
    FROM rate_changes
),

streaks AS (
    SELECT *,
        SUM(CASE WHEN is_increase = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY currency_symbol ORDER BY rate_date
        ) AS streak_group
    FROM streak_flags
),

streak_lengths AS (
    SELECT
        currency_symbol,
        streak_group,
        COUNT(*) AS streak_len
    FROM streaks
    WHERE is_increase = 1
    GROUP BY currency_symbol, streak_group
),

long_positive_streaks AS (
    SELECT
        currency_symbol,
        COUNT(*) AS num_long_streaks
    FROM streak_lengths
    WHERE streak_len >= 3
    GROUP BY currency_symbol
),

volatility AS (
    SELECT
        currency_symbol,
        STDDEV(exchange_rate) / NULLIF(AVG(exchange_rate), 0) AS volatility_index,
        COUNT(*) AS total_days
    FROM rate_changes
    GROUP BY currency_symbol
),

metrics AS (
    SELECT
        v.currency_symbol,
        c.currency_name,
        v.volatility_index,
        COALESCE(lps.num_long_streaks, 0)::FLOAT / v.total_days AS momentum_consistency
    FROM volatility v
    JOIN {{ schema }}.currency c ON v.currency_symbol = c.currency_symbol
    LEFT JOIN long_positive_streaks lps ON v.currency_symbol = lps.currency_symbol
),

clustered AS (
    SELECT *,
        CASE
            WHEN volatility_index >= 0.05 AND momentum_consistency < 0.01 THEN 'A - High Vol, Low Momentum'
            WHEN volatility_index < 0.05 AND momentum_consistency >= 0.01 THEN 'B - Low Vol, High Momentum'
            ELSE 'C - Balanced'
        END AS behavior_cluster
    FROM metrics
)

SELECT *
FROM clustered
ORDER BY behavior_cluster, currency_name

);
