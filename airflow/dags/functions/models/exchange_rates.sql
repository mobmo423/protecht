DROP TABLE IF EXISTS {{ schema }}.{{ target_table }};

CREATE TABLE {{ schema }}.{{ target_table }} AS (
    SELECT DISTINCT
        currency as currency_symbol,
        date as rate_date,
        exchange_rate
    FROM {{ schema }}.raw_forex
);