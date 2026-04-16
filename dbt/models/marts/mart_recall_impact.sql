-- mart_recall_impact
-- Croise les rappels véhicules avec la couverture catalogue.
-- Use case métier Kadensis : identifier quelles pièces de rappel
-- sont bien référencées dans le catalogue (opportunité business).

{{
  config(
    materialized='table',
    tags=['marts', 'recalls', 'gold']
  )
}}

with recalls as (
    select * from {{ ref('stg_vehicle_recalls') }}
),

listings as (
    select * from {{ ref('stg_market_listings') }}
),

-- Agrégation rappels par constructeur + composant
recall_summary as (
    select
        vehicle_make,
        component_category,
        severity,
        count(distinct recall_id)                              as nb_recalls,
        sum(potentially_affected)                              as total_vehicles_affected,
        count(distinct vehicle_model)                          as nb_models_affected,
        count(distinct model_year)                             as nb_years_affected
    from recalls
    group by 1, 2, 3
),

-- Disponibilité pièces sur le marché pour la même catégorie/marque
parts_availability as (
    select
        vehicle_make,
        category                                               as component_category,
        count(distinct part_number)                            as nb_parts_available,
        round(avg(asking_price_eur), 2)                        as avg_part_price,
        count(distinct brand)                                  as nb_brands_available
    from listings
    where condition in ('new', 'reconditioned')
    group by 1, 2
),

final as (
    select
        rs.vehicle_make,
        rs.component_category,
        rs.severity,
        rs.nb_recalls,
        rs.total_vehicles_affected,
        rs.nb_models_affected,
        rs.nb_years_affected,

        -- Couverture pièces disponibles pour ces rappels
        coalesce(pa.nb_parts_available, 0)                    as nb_parts_in_catalog,
        pa.avg_part_price,
        coalesce(pa.nb_brands_available, 0)                   as nb_brands_covering,

        -- Flag : rappel critique sans pièces disponibles → alerte catalogue
        case
            when rs.severity in ('critique', 'majeur')
             and coalesce(pa.nb_parts_available, 0) = 0
            then true
            else false
        end                                                    as is_critical_gap,

        -- Potentiel de marché estimé (volume véhicules × probabilité achat pièce)
        case
            when rs.severity = 'critique' then
                round(rs.total_vehicles_affected * 0.80 * coalesce(pa.avg_part_price, 100), 0)
            when rs.severity = 'majeur' then
                round(rs.total_vehicles_affected * 0.60 * coalesce(pa.avg_part_price, 80), 0)
            else
                round(rs.total_vehicles_affected * 0.30 * coalesce(pa.avg_part_price, 50), 0)
        end                                                    as estimated_market_potential_eur,

        current_timestamp                                      as updated_at

    from recall_summary rs
    left join parts_availability pa
        on rs.vehicle_make = pa.vehicle_make
        and rs.component_category = pa.component_category
)

select * from final
order by estimated_market_potential_eur desc nulls last
