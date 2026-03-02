{{ config(materialized='table') }}

SELECT DISTINCT
    symbol AS stock_symbol,
    symbol AS stock_name
FROM {{ ref('stg_stocks') }}
WHERE symbol IS NOT NULL