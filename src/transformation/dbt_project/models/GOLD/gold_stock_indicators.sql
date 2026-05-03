with silver_data as (
    -- On appelle la table de la couche Silver grâce à la fonction ref() de dbt
    select * from {{ ref('stg_stock_prices') }}
),

analytical_indicators as (
    select
        symbol,
        date,
        close as current_price,
        volume,
        
        -- 1. Calcul de la variation journalière en pourcentage (%)
        round(((close - open) / open * 100)::numeric, 2) as daily_variation_pct,

        -- 2. Calcul de la moyenne mobile sur 7 jours (Fonction de fenêtrage SQL)
        round(avg(close) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        )::numeric, 2) as moving_avg_7d

    from silver_data
)

select * from analytical_indicators