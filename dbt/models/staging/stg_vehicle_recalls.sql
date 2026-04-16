-- stg_vehicle_recalls
-- Staging : rappels véhicules NHTSA

{{
  config(
    materialized='view',
    tags=['staging', 'recalls']
  )
}}

with source as (
    select * from read_parquet(
        's3://{{ var("silver_bucket") }}/vehicle_recalls/**/*.parquet'
    )
),

renamed as (
    select
        recall_id,
        upper(trim(make))                                 as vehicle_make,
        trim(model)                                       as vehicle_model,
        cast(model_year as integer)                       as model_year,
        trim(manufacturer)                                as manufacturer,
        trim(component)                                   as component,
        summary,
        remedy,
        cast(potentially_affected as integer)             as potentially_affected,
        source,
        cast(ingested_year as integer)                    as ingested_year,
        cast(ingested_month as integer)                   as ingested_month,

        -- Classification composant
        case
            when lower(component) like '%brake%'
              or lower(component) like '%frein%'       then 'Freinage'
            when lower(component) like '%engine%'
              or lower(component) like '%moteur%'      then 'Moteur'
            when lower(component) like '%steering%'
              or lower(component) like '%direction%'   then 'Direction'
            when lower(component) like '%airbag%'
              or lower(component) like '%restraint%'   then 'Sécurité passive'
            when lower(component) like '%fuel%'
              or lower(component) like '%carburant%'   then 'Alimentation'
            else                                            'Autre'
        end                                               as component_category,

        -- Sévérité estimée par volume affecté
        case
            when potentially_affected >= 100000 then 'critique'
            when potentially_affected >= 10000  then 'majeur'
            when potentially_affected >= 1000   then 'modéré'
            else                                     'mineur'
        end                                               as severity

    from source
    where recall_id is not null
      and recall_id != ''
)

select * from renamed
