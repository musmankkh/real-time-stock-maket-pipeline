{{ config(materialized='table') }}

SELECT DISTINCT
    market_timestamp,

    DATE(market_timestamp) AS trade_date,
    EXTRACT(HOUR  FROM market_timestamp) AS hour,
    EXTRACT(DAY   FROM market_timestamp) AS day,
    EXTRACT(MONTH FROM market_timestamp) AS month,
    EXTRACT(YEAR  FROM market_timestamp) AS year

FROM {{ ref('stg_stocks') }}
WHERE market_timestamp IS NOT NULL