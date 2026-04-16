-- mart_catalog_coverage
-- Analyse de la couverture catalogue par constructeur / modèle.
-- Répond à la question métier : pour quel véhicule a-t-on le plus/moins de pièces ?
-- Identique à ce que fait Parts.IO chez Kadensis.

{{
  config(
    materialized='table',
    tags=['marts', 'coverage', 'gold']
  )
}}

with parts_by_vehicle as (
    -- Parts catalog ne contient pas directement vehicle_make dans Silver,
    -- on utilise les listings comme proxy de couverture marché
    select
        vehicle_make,
        vehicle_model,
        vehicle_year,
        category,
        count(distinct part_number)                            as nb_distinct_parts,
        count(*)                                               as nb_listings,
        round(avg(asking_price_eur), 2)                        as avg_price,
        count(distinct brand)                                  as nb_brands,
        count(case when condition = 'new' then 1 end)          as nb_new_parts,
        round(
            count(case when condition = 'new' then 1 end) * 100.0
            / nullif(count(*), 0),
            1
        )                                                      as new_parts_pct
    from {{ ref('stg_market_listings') }}
    group by 1, 2, 3, 4
),

vehicle_totals as (
    select
        vehicle_make,
        vehicle_model,
        count(distinct vehicle_year)                           as years_covered,
        count(distinct category)                               as categories_covered,
        sum(nb_distinct_parts)                                 as total_parts,
        sum(nb_listings)                                       as total_listings,
        round(avg(avg_price), 2)                               as global_avg_price,
        round(avg(nb_brands), 1)                               as avg_brands_per_category,
        -- Score couverture : catégories couvertes / 7 catégories possibles
        round(count(distinct category) * 100.0 / 7, 1)        as coverage_score_pct
    from parts_by_vehicle
    group by 1, 2
),

recalls_by_vehicle as (
    select
        vehicle_make,
        vehicle_model,
        count(*)                                               as total_recalls,
        sum(potentially_affected)                              as total_vehicles_affected,
        count(case when severity = 'critique' then 1 end)     as critical_recalls,
        count(distinct component_category)                     as affected_component_categories
    from {{ ref('stg_vehicle_recalls') }}
    group by 1, 2
),

final as (
    select
        vt.vehicle_make,
        vt.vehicle_model,
        vt.years_covered,
        vt.categories_covered,
        vt.total_parts,
        vt.total_listings,
        vt.global_avg_price,
        vt.avg_brands_per_category,
        vt.coverage_score_pct,

        -- Rappels associés
        coalesce(rv.total_recalls, 0)                         as total_recalls,
        coalesce(rv.total_vehicles_affected, 0)               as vehicles_affected_by_recalls,
        coalesce(rv.critical_recalls, 0)                      as critical_recalls,
        coalesce(rv.affected_component_categories, 0)         as recall_component_categories,

        -- Segment couverture catalogue
        case
            when vt.coverage_score_pct >= 85  then 'couverture complète'
            when vt.coverage_score_pct >= 60  then 'bonne couverture'
            when vt.coverage_score_pct >= 40  then 'couverture partielle'
            else                                   'couverture faible'
        end                                                    as coverage_level,

        -- Opportunité : véhicule avec rappels mais faible couverture pièces
        case
            when vt.coverage_score_pct < 60 and coalesce(rv.total_recalls, 0) > 0
            then true
            else false
        end                                                    as is_coverage_opportunity,

        current_timestamp                                      as updated_at

    from vehicle_totals vt
    left join recalls_by_vehicle rv
        on vt.vehicle_make = rv.vehicle_make
        and vt.vehicle_model = rv.vehicle_model
)

select * from final
order by total_parts desc
