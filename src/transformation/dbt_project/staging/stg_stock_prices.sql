with raw_data as (
    -- 1. EXTRACTION : On va chercher la donnée brute injectée par notre pipeline DLT
    select * from etl_stocks.bronze.raw_stock_prices
),

cleaned as (
    -- 2. TRANSFORMATION : On nettoie et on standardise
    select
        ticker as symbol,       -- On standardise le vocabulaire boursier
        date::date as date,     -- On s'assure que le format est strictement une date
        open,
        high,
        low,
        close,
        volume
        
    from raw_data
    -- 3. RÈGLES DE QUALITÉ : On rejette les données corrompues à la source
    where close is not null 
      and ticker is not null
)

select * from cleaned