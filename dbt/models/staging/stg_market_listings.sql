-- stg_market_listings
-- Staging : annonces marketplace (simulation Partakus)

{{
  config(
    materialized='view',
    tags=['staging', 'marketplace']
  )
}}

with source as (
    select * from read_parquet(
        's3://{{ var("silver_bucket") }}/market_listings/**/*.parquet'
    )
),

renamed as (
    select
        listing_id,
        part_number,
        trim(brand)                                       as brand,
        trim(category)                                    as category,
        trim(vehicle_make)                                as vehicle_make,
        trim(vehicle_model)                               as vehicle_model,
        cast(vehicle_year as integer)                     as vehicle_year,
        cast(asking_price_eur as double)                  as asking_price_eur,
        cast(negotiated_price_eur as double)              as negotiated_price_eur,
        cast(discount_rate as double)                     as discount_rate,
        lower(trim(seller_type))                          as seller_type,
        trim(seller_region)                               as seller_region,
        lower(trim(condition))                            as condition,
        cast(days_to_sell as integer)                     as days_to_sell,
        source,
        cast(ingested_year as integer)                    as ingested_year,
        cast(ingested_month as integer)                   as ingested_month,

        -- Indicateur vente rapide (< 7 jours)
        case
            when days_to_sell is not null and days_to_sell <= 7 then true
            else false
        end                                               as is_fast_sale,

        -- Segment prix
        case
            when asking_price_eur < 30   then 'entrée de gamme'
            when asking_price_eur < 100  then 'milieu de gamme'
            when asking_price_eur < 300  then 'premium'
            else                              'haut de gamme'
        end                                               as price_segment

    from source
    where listing_id is not null
      and asking_price_eur > 0
      and asking_price_eur < 10000
)

select * from renamed
