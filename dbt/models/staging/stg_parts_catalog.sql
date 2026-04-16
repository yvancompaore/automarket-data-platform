-- stg_parts_catalog
-- Staging : lecture du Silver Parquet (parts_catalog)
-- Normalisation légère, renommage, cast des types

{{
  config(
    materialized='view',
    tags=['staging', 'catalog']
  )
}}

with source as (
    select * from read_parquet(
        's3://{{ var("silver_bucket") }}/parts_catalog/**/*.parquet'
    )
),

renamed as (
    select
        part_id,
        part_number,
        trim(brand)                                       as brand,
        trim(brand_type)                                  as brand_type,
        trim(category)                                    as category,
        trim(sub_category)                                as sub_category,
        description,
        cast(price_eur as double)                         as price_eur,
        cast(stock_quantity as integer)                   as stock_quantity,
        lower(trim(availability))                         as availability,
        supplier,
        lower(trim(source))                               as source,
        cast(quality_score as double)                     as quality_score,
        _fingerprint,
        cast(ingested_year as integer)                    as ingested_year,
        cast(ingested_month as integer)                   as ingested_month,

        -- Colonnes dérivées utiles
        case
            when availability = 'in_stock'    then true
            when availability = 'discontinued' then false
            else null
        end                                               as is_available,

        case
            when price_eur is null         then 'missing_price'
            when price_eur < 5             then 'very_low'
            when price_eur < 50            then 'low'
            when price_eur < 200           then 'medium'
            when price_eur < 500           then 'high'
            else                                'premium'
        end                                               as price_tier

    from source
    where part_id is not null
      and part_number is not null
      and part_number != ''
)

select * from renamed
