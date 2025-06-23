DROP TABLE IF EXISTS {{ schema }}.{{ target_table }};

CREATE TABLE {{ schema }}.{{ target_table }} AS (
    SELECT DISTINCT
        currency as currency_symbol,
        currency_name
    FROM {{ schema }}.raw_forex
);



