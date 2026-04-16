-- mart_supplier_performance
-- KPIs fournisseurs pour la marketplace Partakus :
-- fiabilité catalogue, compétitivité prix, liquidité stock, couverture marque.

{{
  config(
    materialized='table',
    tags=['marts', 'suppliers', 'gold']
  )
}}

with catalog_perf as (
    select
        supplier,
        brand_type,
        count(distinct part_number)                            as nb_unique_refs,
        count(distinct category)                               as nb_categories,
        count(distinct brand)                                  as nb_brands,
        round(avg(price_eur), 2)                               as avg_price,
        round(avg(reliability_score), 3)                       as avg_reliability_score,
        count(case when is_available then 1 end)               as nb_available_refs,
        count(case when is_price_outlier then 1 end)           as nb_price_outliers,
        count(case when nb_sources >= 2 then 1 end)            as nb_validated_by_multi_source,
        -- Taux de disponibilité
        round(
            count(case when is_available then 1 end) * 100.0
            / nullif(count(*), 0),
            1
        )                                                      as availability_rate_pct,
        -- Part des références multi-sources (proxy qualité catalogue)
        round(
            count(case when nb_sources >= 2 then 1 end) * 100.0
            / nullif(count(*), 0),
            1
        )                                                      as multi_source_validation_pct
    from {{ ref('int_parts_enriched') }}
    where supplier is not null
    group by 1, 2
),

market_perf as (
    select
        seller_type                                            as supplier_type,
        seller_region,
        count(*)                                               as nb_transactions,
        round(avg(asking_price_eur), 2)                        as avg_asking_price,
        round(avg(discount_rate) * 100, 2)                     as avg_discount_pct,
        round(avg(days_to_sell), 1)                            as avg_days_to_sell,
        round(
            count(case when is_fast_sale then 1 end) * 100.0
            / nullif(count(*), 0),
            1
        )                                                      as fast_sale_rate_pct,
        count(distinct vehicle_make)                           as nb_vehicle_makes_covered,
        count(distinct category)                               as nb_categories_covered
    from {{ ref('stg_market_listings') }}
    group by 1, 2
),

catalog_final as (
    select
        supplier,
        brand_type,
        nb_unique_refs,
        nb_categories,
        nb_brands,
        avg_price,
        avg_reliability_score,
        nb_available_refs,
        nb_price_outliers,
        nb_validated_by_multi_source,
        availability_rate_pct,
        multi_source_validation_pct,

        -- Score global fournisseur (0-100)
        round(
            (
                least(availability_rate_pct / 100.0, 1.0)           * 35
                + avg_reliability_score                               * 35
                + least(multi_source_validation_pct / 100.0, 1.0)   * 20
                + case
                    when nb_price_outliers = 0            then 10
                    when nb_price_outliers / nullif(nb_unique_refs::float, 0) < 0.02 then 7
                    when nb_price_outliers / nullif(nb_unique_refs::float, 0) < 0.05 then 4
                    else 0
                  end
            ) * 100,
            1
        )                                                            as supplier_score,

        -- Segment fournisseur
        case
            when avg_reliability_score >= 0.8 and availability_rate_pct >= 80 then 'Tier 1 — Stratégique'
            when avg_reliability_score >= 0.6 and availability_rate_pct >= 60 then 'Tier 2 — Fiable'
            when avg_reliability_score >= 0.4                                  then 'Tier 3 — À surveiller'
            else                                                                     'Tier 4 — Risqué'
        end                                                          as supplier_tier,

        current_timestamp                                            as updated_at

    from catalog_perf
)

select * from catalog_final
order by supplier_score desc nulls last
