{{ config(materialized='table') }}

SELECT
    symbol,
    market_timestamp,

    current_price,
    day_open,
    day_high,
    day_low,
    prev_close,

    change_amount,
    change_percent,

    fetched_at

FROM {{ ref('stg_stocks') }}