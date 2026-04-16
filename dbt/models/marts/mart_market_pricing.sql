-- mart_market_pricing
-- Analyse des prix du marché pièces auto par catégorie, marque, constructeur.
-- KPI principal : comparer les prix catalogue vs prix marché réel (marketplace).

{{
  config(
    materialized='table',
    tags=['marts', 'pricing', 'gold']
  )
}}

with catalog_prices as (
    select
        category,
        brand,
        brand_type,
        count(*)                                                as nb_references,
        count(case when is_available then 1 end)               as nb_available,
        round(avg(price_eur), 2)                               as catalog_avg_price,
        round(percentile_cont(0.5) within group
              (order by price_eur), 2)                          as catalog_median_price,
        round(min(price_eur), 2)                               as catalog_min_price,
        round(max(price_eur), 2)                               as catalog_max_price,
        round(avg(reliability_score), 3)                       as avg_reliability_score,
        count(case when is_price_outlier then 1 end)           as nb_price_outliers,
        count(case when nb_sources >= 2 then 1 end)            as nb_multi_source_refs
    from {{ ref('int_parts_enriched') }}
    where price_eur is not null
    group by 1, 2, 3
),

market_prices as (
    select
        category,
        brand,
        count(*)                                                as nb_listings,
        round(avg(asking_price_eur), 2)                        as market_avg_asking,
        round(avg(negotiated_price_eur), 2)                    as market_avg_negotiated,
        round(percentile_cont(0.5) within group
              (order by asking_price_eur), 2)                  as market_median_price,
        round(avg(discount_rate) * 100, 2)                     as avg_discount_pct,
        round(avg(days_to_sell), 1)                            as avg_days_to_sell,
        count(case when is_fast_sale then 1 end)               as nb_fast_sales,
        count(case when condition = 'new' then 1 end)          as nb_new_condition,
        count(case when condition = 'reconditioned' then 1 end) as nb_reconditioned
    from {{ ref('stg_market_listings') }}
    group by 1, 2
),

final as (
    select
        coalesce(cp.category, mp.category)                     as category,
        coalesce(cp.brand, mp.brand)                           as brand,
        cp.brand_type,

        -- Catalogue
        coalesce(cp.nb_references, 0)                         as nb_catalog_references,
        coalesce(cp.nb_available, 0)                          as nb_catalog_available,
        cp.catalog_avg_price,
        cp.catalog_median_price,
        cp.catalog_min_price,
        cp.catalog_max_price,
        cp.avg_reliability_score,
        coalesce(cp.nb_price_outliers, 0)                     as nb_catalog_price_outliers,
        coalesce(cp.nb_multi_source_refs, 0)                  as nb_multi_source_refs,

        -- Marché
        coalesce(mp.nb_listings, 0)                           as nb_market_listings,
        mp.market_avg_asking,
        mp.market_avg_negotiated,
        mp.market_median_price,
        mp.avg_discount_pct,
        mp.avg_days_to_sell,
        coalesce(mp.nb_fast_sales, 0)                         as nb_fast_sales,

        -- Écart catalogue vs marché (signal pricing)
        case
            when cp.catalog_median_price > 0 and mp.market_median_price > 0
            then round(
                (mp.market_median_price - cp.catalog_median_price)
                / cp.catalog_median_price * 100,
                2
            )
            else null
        end                                                    as market_vs_catalog_pct,

        -- Liquidité (combien de temps pour vendre)
        case
            when mp.avg_days_to_sell <= 7  then 'haute'
            when mp.avg_days_to_sell <= 21 then 'moyenne'
            else                                'faible'
        end                                                    as market_liquidity,

        current_timestamp                                      as updated_at

    from catalog_prices cp
    full outer join market_prices mp
        on cp.category = mp.category and cp.brand = mp.brand
)

select * from final
order by nb_catalog_references desc
