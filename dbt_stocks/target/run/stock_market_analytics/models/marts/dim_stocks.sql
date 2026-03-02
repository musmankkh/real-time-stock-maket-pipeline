
  
    

create or replace transient table STOCKMARKET._MARTS.dim_stocks
    
    
    
    as (

SELECT DISTINCT
    symbol AS stock_symbol,
    symbol AS stock_name
FROM STOCKMARKET._STAGING.stg_stocks
WHERE symbol IS NOT NULL
    )
;


  