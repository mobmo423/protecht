CREATE TABLE IF NOT EXISTS {{ schema }}.currency_behavior_metrics_scd (
    currency_symbol VARCHAR,
    currency_name VARCHAR,
    volatility_index DOUBLE PRECISION,
    momentum_consistency DOUBLE PRECISION,
    behavior_cluster VARCHAR,
    valid_from TIMESTAMP WITHOUT TIME ZONE,
    valid_to TIMESTAMP WITHOUT TIME ZONE,
    is_current BOOLEAN DEFAULT TRUE
);
