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
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS streak_group
    FROM streak_flags
),

streaks_with_bounds AS (
    SELECT
        *,
        FIRST_VALUE(exchange_rate) OVER (
            PARTITION BY currency_symbol, streak_group ORDER BY rate_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS start_rate,
        LAST_VALUE(exchange_rate) OVER (
            PARTITION BY currency_symbol, streak_group ORDER BY rate_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS end_rate
    FROM streaks
    WHERE is_increase = 1
),

streak_windows AS (
    SELECT
        currency_name,
        currency_symbol,
        streak_group,
        COUNT(*) AS cons_pos_days,
        MIN(rate_date) AS start_date,
        MAX(rate_date) AS end_date,
        MIN(start_rate) AS start_rate,
        MAX(end_rate) AS end_rate
    FROM streaks_with_bounds
    GROUP BY currency_name, currency_symbol, streak_group
),

qualified_streaks AS (
    SELECT *,
        100.0 * (end_rate - start_rate) / start_rate AS percent_change
    FROM streak_windows
    WHERE cons_pos_days >= 2
),

aggregates AS (
    SELECT
        currency_name,
        AVG(cons_pos_days) AS avg_cons_pos_days,
        AVG(percent_change) AS avg_cons_perc_change
    FROM qualified_streaks
    GROUP BY currency_name
),

ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY avg_cons_pos_days DESC) AS avg_cons_pos_days_rank,
        RANK() OVER (ORDER BY avg_cons_perc_change DESC) AS avg_cons_perc_change_rank
    FROM aggregates
)

SELECT
    currency_name,
    avg_cons_pos_days,
    avg_cons_perc_change,
    avg_cons_pos_days_rank,
    avg_cons_perc_change_rank
FROM ranked
WHERE avg_cons_pos_days_rank <= 5

UNION ALL

SELECT
    currency_name,
    avg_cons_pos_days,
    avg_cons_perc_change,
    avg_cons_pos_days_rank,
    avg_cons_perc_change_rank
FROM ranked
WHERE avg_cons_perc_change_rank <= 5

);
