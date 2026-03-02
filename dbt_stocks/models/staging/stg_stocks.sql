{{ config(
    materialized='table',
) }}

WITH source_data AS (

    SELECT
        r.symbol,

        r.raw_record:c::FLOAT  AS current_price,
        r.raw_record:d::FLOAT  AS change_amount,
        r.raw_record:dp::FLOAT AS change_percent,
        r.raw_record:h::FLOAT  AS day_high,
        r.raw_record:l::FLOAT  AS day_low,
        r.raw_record:o::FLOAT  AS day_open,
        r.raw_record:pc::FLOAT AS prev_close,

        r.raw_record:t::TIMESTAMP AS market_timestamp,
        r.fetched_at::TIMESTAMP   AS fetched_at

    FROM {{ source('raw', 'RAWSTOCKS') }} r
),

-- ✅ Data Quality Checks
cleaned_data AS (

    SELECT
        symbol,
        current_price,
        ROUND(day_high, 2) AS day_high,
        ROUND(day_low, 2) AS day_low,
        ROUND(day_open, 2) AS day_open,
        ROUND(prev_close, 2) AS prev_close,
        change_amount,
        ROUND(change_percent, 4) AS change_percent,
        market_timestamp,
        fetched_at

    FROM source_data
    WHERE
        symbol IS NOT NULL
        AND current_price IS NOT NULL
        AND market_timestamp IS NOT NULL
)

SELECT * FROM cleaned_data