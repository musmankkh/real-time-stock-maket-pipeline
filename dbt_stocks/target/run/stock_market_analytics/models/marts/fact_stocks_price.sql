
  
    

create or replace transient table STOCKMARKET._MARTS.fact_stocks_price
    
    
    
    as (

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

FROM STOCKMARKET._STAGING.stg_stocks
    )
;


  